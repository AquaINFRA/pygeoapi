import psycopg2
import sys
import logging
import sshtunnel
import geomet.wkt
LOGGER = logging.getLogger(__name__)


#######################
### Get SQL queries ###
#######################

def _get_query_basin_id_reg_id(subc_id):
    query = """
    SELECT basin_id, reg_id
    FROM sub_catchments
    WHERE subc_id = {given_subc_id}
    """.format(given_subc_id = subc_id)
    query = query.replace("\n", " ")
    return query


def _get_query_reg_id(lon, lat):
    """
    Example query:
    SELECT reg_id FROM regional_units
    WHERE st_intersects(ST_SetSRID(ST_MakePoint(9.931555, 54.695070),4326), geom);

    Result:
     reg_id 
    --------
         58
    (1 row)
    """
    query = """
    SELECT reg_id
    FROM regional_units
    WHERE st_intersects(ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326), geom)
    """.format(longitude = lon, latitude = lat)
    query = query.replace("\n", " ")
    return query 


def _get_query_subc_id_basin_id(lon, lat, reg_id):
    """
    Example query:
    SELECT sub.subc_id, sub.basin_id FROM sub_catchments sub
    WHERE st_intersects(ST_SetSRID(ST_MakePoint(9.931555, 54.695070),4326), sub.geom)
    AND sub.reg_id = 58;

    Result:
    subc_id  | basin_id 
    -----------+----------
     506251252 |  1292547
    (1 row)
    """

    query = """
    SELECT
    subc_id,
    basin_id
    FROM sub_catchments
    WHERE st_intersects(ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326), geom)
    AND reg_id = {reg_id}
    """.format(longitude = lon, latitude = lat, reg_id = reg_id)
    query = query.replace("\n", " ")
    return query 


def _get_query_snapped(lon, lat, subc_id, basin_id, reg_id):
    """
    SELECT seg.strahler,
    ST_AsText(ST_LineInterpolatePoint(seg.geom, ST_LineLocatePoint(seg.geom, ST_SetSRID(ST_MakePoint(9.931555, 54.695070),4326)))),
    ST_AsText(seg.geom)
    FROM hydro.stream_segments seg WHERE seg.subc_id = 506251252;

    Result:
     strahler |        st_astext         |                                                                                    st_astext                                                                                    
    ----------+--------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            2 | POINT(9.931555 54.69625) | LINESTRING(9.929583333333333 54.69708333333333,9.930416666666668 54.69625,9.932083333333335 54.69625,9.933750000000002 54.694583333333334,9.934583333333334 54.694583333333334)
    (1 row)
    """

    query = """
    SELECT 
    strahler,
    ST_AsText(ST_LineInterpolatePoint(geom, ST_LineLocatePoint(geom, ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326)))),
    ST_AsText(geom)
    FROM hydro.stream_segments
    WHERE subc_id = {subc_id}
    AND basin_id = {basin_id}
    AND reg_id = {reg_id}
    """.format(subc_id = subc_id, longitude = lon, latitude = lat, basin_id = basin_id, reg_id = reg_id)
    query = query.replace("\n", " ")
    return query


def _get_query_segment(subc_id, basin_id, reg_id):
    """
    Example query:
    SELECT seg.strahler, ST_AsText(seg.geom) FROM hydro.stream_segments seg WHERE seg.subc_id = 506251252;

    Result:
     strahler |                                                                                    st_astext                                                                                    
    ----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            2 | LINESTRING(9.929583333333333 54.69708333333333,9.930416666666668 54.69625,9.932083333333335 54.69625,9.933750000000002 54.694583333333334,9.934583333333334 54.694583333333334)
    (1 row)
    """

    query = """
    SELECT 
    strahler,
    ST_AsText(geom)
    FROM hydro.stream_segments
    WHERE subc_id = {subc_id}
    AND reg_id = {reg_id}
    AND basin_id = {basin_id}
    """.format(subc_id = subc_id, basin_id = basin_id, reg_id = reg_id)
    query = query.replace("\n", " ")
    return query


def _get_query_upstream(subc_id, reg_id, basin_id):
    """
    This one cuts the graph into connected components, by removing
    the segment-of-interest itself. As a result, its subcatchment
    is included in the result, and may have to be removed.

    Example query:
    SELECT 506251252, array_agg(node)::bigint[] AS nodes FROM pgr_connectedComponents('
        SELECT basin_id, subc_id AS id, subc_id AS source, target, length AS cost
        FROM hydro.stream_segments WHERE reg_id = 58 AND basin_id = 1292547 AND subc_id != 506251252
    ') WHERE component > 0 GROUP BY component;

    Result:
     ?column?  |                        nodes                        
    -----------+-----------------------------------------------------
     506251252 | {506250459,506251015,506251126,506251252,506251712}
    (1 row)
    """

    query = '''
    SELECT {subc_id}, array_agg(node)::bigint[] AS nodes 
    FROM pgr_connectedComponents('
        SELECT
        basin_id,
        subc_id AS id,
        subc_id AS source,
        target,
        length AS cost
        FROM hydro.stream_segments
        WHERE reg_id = {reg_id}
        AND basin_id = {basin_id}
        AND subc_id != {subc_id}
    ') WHERE component > 0 GROUP BY component;
    '''.format(subc_id = subc_id, reg_id = reg_id, basin_id = basin_id)

    query = query.replace("\n", " ")
    query = query.replace("    ", "")
    query = query.strip()
    return query


def _get_query_dijkstra(start_subc_id, end_subc_id, reg_id, basin_id):
    query = '''
    SELECT edge
    FROM pgr_dijkstra('
        SELECT
        subc_id AS id,
        subc_id AS source,
        target,
        length AS cost
        FROM hydro.stream_segments
        WHERE reg_id = {reg_id}
        AND basin_id = {basin_id}',
        {start_subc_id}, {end_subc_id},
        directed := false);
    '''.format(reg_id = reg_id, basin_id = basin_id, start_subc_id = start_subc_id, end_subc_id = end_subc_id)

    query = query.replace("\n", " ")
    query = query.replace("    ", "")
    query = query.strip()
    return query


def _get_query_upstream_dissolved(upstream_ids, basin_id, reg_id):
    """
    Example query:
    SELECT ST_AsText(ST_MemUnion(geom)) FROM sub_catchments WHERE subc_id IN (506250459, 506251015, 506251126, 506251712);

    Example result:
                                                         st_astext                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    POLYGON((9.916666666666668 54.7025,9.913333333333334 54.7025,9.913333333333334 54.705,9.915000000000001 54.705,9.915833333333333 54.705,9.915833333333333 54.70583333333333,9.916666666666668 54.70583333333333,9.916666666666668 54.705,9.918333333333335 54.705,9.918333333333335 54.704166666666666,9.919166666666667 54.704166666666666,9.919166666666667 54.70333333333333,9.920833333333334 54.70333333333333,9.920833333333334 54.704166666666666,9.924166666666668 54.704166666666666,9.925 54.704166666666666,9.925 54.705,9.926666666666668 54.705,9.9275 54.705,9.9275 54.70583333333333,9.928333333333335 54.70583333333333,9.928333333333335 54.70333333333333,9.929166666666667 54.70333333333333,9.929166666666667 54.7025,9.931666666666667 54.7025,9.931666666666667 54.7,9.930833333333334 54.7,9.930833333333334 54.69833333333333,9.930000000000001 54.69833333333333,9.929166666666667 54.69833333333333,9.929166666666667 54.6975,9.929166666666667 54.696666666666665,9.928333333333335 54.696666666666665,9.928333333333335 54.695,9.9275 54.695,9.9275 54.693333333333335,9.928333333333335 54.693333333333335,9.928333333333335 54.69166666666666,9.9275 54.69166666666666,9.9275 54.69083333333333,9.926666666666668 54.69083333333333,9.926666666666668 54.69,9.925833333333333 54.69,9.925 54.69,9.925 54.68833333333333,9.922500000000001 54.68833333333333,9.922500000000001 54.69083333333333,9.921666666666667 54.69083333333333,9.921666666666667 54.69166666666666,9.919166666666667 54.69166666666666,9.919166666666667 54.692499999999995,9.918333333333335 54.692499999999995,9.918333333333335 54.693333333333335,9.9175 54.693333333333335,9.9175 54.695,9.918333333333335 54.695,9.918333333333335 54.69833333333333,9.9175 54.69833333333333,9.9175 54.700833333333335,9.9175 54.70166666666667,9.916666666666668 54.70166666666667,9.916666666666668 54.7025))
    (1 row)
    """

    ids = ", ".join([str(elem) for elem in upstream_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712

    query = """
    SELECT ST_AsText(ST_MemUnion(geom))
    FROM sub_catchments
    WHERE subc_id IN ({ids})
    AND reg_id = {reg_id}
    AND basin_id = {basin_id}
    """.format(ids = ids, basin_id = basin_id, reg_id = reg_id)
    return query


def _get_query_linestrings_for_subc_ids(subc_ids, basin_id, reg_id):
    '''
    Example query:
    SELECT  subc_id, strahler, ST_AsText(geom)
    FROM hydro.stream_segments WHERE subc_id IN (506250459, 506251015, 506251126, 506251712);
    '''
    ids = ", ".join([str(elem) for elem in subc_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712

    query = '''
    SELECT 
    subc_id, strahler, ST_AsText(geom)
    FROM hydro.stream_segments
    WHERE subc_id IN ({ids})
    AND reg_id = {reg_id}
    AND basin_id = {basin_id}
    '''.format(ids = ids, basin_id = basin_id, reg_id = reg_id)
    query = query.replace("\n", " ")
    return query


def _get_query_upstream_polygons(upstream_ids, basin_id, reg_id):
    """
    Example query:
    SELECT ST_AsText(geom) FROM sub_catchments WHERE subc_id IN (506250459, 506251015, 506251126, 506251712);
    SELECT subc_id, ST_AsText(geom) FROM sub_catchments WHERE subc_id IN (506250459, 506251015, 506251126, 506251712);

    Result:
    st_astext
    --------------------------------------------------------------------------------------------------------------------------------------------------
     MULTIPOLYGON(((9.915833333333333 54.70583333333333,9.915833333333333 54.705,9.915000000000001 54.705,9.913333333333334 54.705,9.913333333333334 54.7025,9.916666666666668 54.7025,9.916666666666668 54.70166666666667,9.9175 54.70166666666667,9.9175 54.700833333333335,9.918333333333335 54.700833333333335,9.918333333333335 54.70166666666667,9.919166666666667 54.70166666666667,9.919166666666667 54.700833333333335,9.921666666666667 54.700833333333335,9.921666666666667 54.7,9.923333333333334 54.7,9.923333333333334 54.700833333333335,9.925 54.700833333333335,9.925 54.7,9.925833333333333 54.7,9.925833333333333 54.69916666666667,9.928333333333335 54.69916666666667,9.928333333333335 54.6975,9.929166666666667 54.6975,9.929166666666667 54.69833333333333,9.930000000000001 54.69833333333333,9.930833333333334 54.69833333333333,9.930833333333334 54.7,9.931666666666667 54.7,9.931666666666667 54.7025,9.929166666666667 54.7025,9.929166666666667 54.70333333333333,9.928333333333335 54.70333333333333,9.928333333333335 54.70583333333333,9.9275 54.70583333333333,9.9275 54.705,9.926666666666668 54.705,9.925 54.705,9.925 54.704166666666666,9.924166666666668 54.704166666666666,9.920833333333334 54.704166666666666,9.920833333333334 54.70333333333333,9.919166666666667 54.70333333333333,9.919166666666667 54.704166666666666,9.918333333333335 54.704166666666666,9.918333333333335 54.705,9.916666666666668 54.705,9.916666666666668 54.70583333333333,9.915833333333333 54.70583333333333)))
     MULTIPOLYGON(((9.918333333333335 54.70166666666667,9.918333333333335 54.700833333333335,9.9175 54.700833333333335,9.9175 54.69833333333333,9.918333333333335 54.69833333333333,9.918333333333335 54.695,9.919166666666667 54.695,9.919166666666667 54.69583333333333,9.920833333333334 54.69583333333333,9.920833333333334 54.695,9.922500000000001 54.695,9.922500000000001 54.69583333333333,9.923333333333334 54.69583333333333,9.923333333333334 54.696666666666665,9.924166666666668 54.696666666666665,9.924166666666668 54.6975,9.923333333333334 54.6975,9.923333333333334 54.69833333333333,9.922500000000001 54.69833333333333,9.922500000000001 54.7,9.921666666666667 54.7,9.921666666666667 54.700833333333335,9.919166666666667 54.700833333333335,9.919166666666667 54.70166666666667,9.918333333333335 54.70166666666667)))
     MULTIPOLYGON(((9.923333333333334 54.700833333333335,9.923333333333334 54.7,9.922500000000001 54.7,9.922500000000001 54.69833333333333,9.923333333333334 54.69833333333333,9.923333333333334 54.6975,9.924166666666668 54.6975,9.924166666666668 54.69583333333333,9.925833333333333 54.69583333333333,9.925833333333333 54.695,9.928333333333335 54.695,9.928333333333335 54.696666666666665,9.929166666666667 54.696666666666665,9.929166666666667 54.6975,9.928333333333335 54.6975,9.928333333333335 54.69916666666667,9.925833333333333 54.69916666666667,9.925833333333333 54.7,9.925 54.7,9.925 54.700833333333335,9.923333333333334 54.700833333333335)))
     MULTIPOLYGON(((9.923333333333334 54.696666666666665,9.923333333333334 54.69583333333333,9.922500000000001 54.69583333333333,9.922500000000001 54.695,9.920833333333334 54.695,9.920833333333334 54.69583333333333,9.919166666666667 54.69583333333333,9.919166666666667 54.695,9.918333333333335 54.695,9.9175 54.695,9.9175 54.693333333333335,9.918333333333335 54.693333333333335,9.918333333333335 54.692499999999995,9.919166666666667 54.692499999999995,9.919166666666667 54.69166666666666,9.921666666666667 54.69166666666666,9.921666666666667 54.69083333333333,9.922500000000001 54.69083333333333,9.922500000000001 54.68833333333333,9.925 54.68833333333333,9.925 54.69,9.925833333333333 54.69,9.926666666666668 54.69,9.926666666666668 54.69083333333333,9.9275 54.69083333333333,9.9275 54.69166666666666,9.928333333333335 54.69166666666666,9.928333333333335 54.693333333333335,9.9275 54.693333333333335,9.9275 54.695,9.925833333333333 54.695,9.925833333333333 54.69583333333333,9.924166666666668 54.69583333333333,9.924166666666668 54.696666666666665,9.923333333333334 54.696666666666665)))
    (4 rows)
    """

    ids = ", ".join([str(elem) for elem in upstream_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712

    query = '''
    SELECT subc_id, ST_AsText(geom)
    FROM sub_catchments
    WHERE subc_id IN ({ids})
    AND basin_id = {basin_id}
    AND reg_id = {reg_id}
    '''.format(ids = ids, basin_id = basin_id, reg_id = reg_id)
    return query


def _get_query_upstream_bbox(upstream_ids, basin_id, reg_id):
    """
    Example query:
    SELECT ST_AsText(ST_Extent(geom)) FROM sub_catchments WHERE subc_id IN (506250459, 506251015, 506251126, 506251712);

    These queries return the same result:
    geofresh_data=> SELECT ST_AsText(ST_Extent(geom)) as bbox FROM sub_catchments WHERE reg_id = 58 AND subc_id IN (506250459, 506251015, 506251126, 506251712) GROUP BY reg_id;
    geofresh_data=> SELECT ST_AsText(ST_Extent(geom)) as bbox FROM sub_catchments WHERE reg_id = 58 AND subc_id IN (506250459, 506251015, 506251126, 506251712);
    geofresh_data=> SELECT ST_AsText(ST_Extent(geom)) as bbox FROM sub_catchments WHERE subc_id IN (506250459, 506251015, 506251126, 506251712);
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    POLYGON((9.913333333333334 54.68833333333333,9.913333333333334 54.70583333333333,9.931666666666667 54.70583333333333,9.931666666666667 54.68833333333333,9.913333333333334 54.68833333333333))
    (1 row)
    """
    #LOGGER.debug('Inputs: %s' % upstream_ids)
    relevant_ids = ", ".join([str(elem) for elem in upstream_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712

    query = """
    SELECT ST_AsText(ST_Extent(geom))
    FROM sub_catchments
    WHERE subc_id IN ({relevant_ids})
    AND basin_id = {basin_id}
    AND reg_id = {reg_id}
    """.format(relevant_ids = relevant_ids, basin_id = basin_id, reg_id = reg_id)
    return query


def _get_query_test(point_table_name):
    # Then we can use "pgr_upstreamcomponent" and run it on that table "poi"
    # Then we get a table with, for each "subc_id", all the "subc_id" of the upstream subcatchments! (All of them? Or just the next? I guess all of them?)
    # Then, can"t we display them as raster?

    #query = "SELECT upstr.subc_id, upstr.nodes FROM "{point_table}" poi, hydro.pgr_upstreamcomponent(poi.subc_id, poi.reg_id, poi.basin_id) upstr WHERE poi.strahler_order != 1".format(point_table = point_table_name)
    query = """SELECT upstr.subc_id, upstr.nodes
        FROM "{point_table}" poi, hydro.pgr_upstreamcomponent(poi.subc_id, poi.reg_id, poi.basin_id) upstr
        WHERE poi.strahler_order != 1""".format(point_table = point_table_name)
    return query


###################################
### get results from SQL result ###
### Non-GeoJSON                 ###
###################################

def get_basin_id_reg_id(conn, subc_id):
    name = "get_basin_id_reg_id"
    LOGGER.debug("ENTERING: %s: subc_id=%s" % (name, subc_id))
    query = _get_query_basin_id_reg_id(subc_id)
    result_row = get_only_row(execute_query(conn, query), name)
    if result_row is None:
        LOGGER.warning('No basin id and region id found for subc_id %s!' % subc_id)
        error_message = 'No basin id and region id found for subc_id %s!' % subc_id
        LOGGER.error(error_message)
        raise ValueError(error_message)

    else:
        basin_id = result_row[0]
        reg_id = result_row[1]
    LOGGER.debug("LEAVING: %s: subc_id=%s" % (name, subc_id))

    return basin_id, reg_id


def check_outside_europe(lon, lat):
    # TODO: Move somewhere else!
    outside_europe = False
    err_msg = None
    LOGGER.debug("CHECKING for in or outside Europe?!")

    if lat > 82:
        err_msg = 'Too far north to be part of Europe: %s' % lat
        outside_europe = True
    elif lat < 34:
        err_msg = 'Too far south to be part of Europe: %s' % lat
        outside_europe = True
    if lon > 70:
        err_msg = 'Too far east to be part of Europe: %s' % lon
        outside_europe = True
    elif lon < -32:
        err_msg = 'Too far west to be part of Europe: %s' % lon
        outside_europe = True

    if outside_europe:
        LOGGER.error(err_msg)
        raise ValueError(err_msg)

def get_reg_id(conn, lon, lat):
    name = "get_reg_id"
    LOGGER.debug("ENTERING: %s: lon=%s, lat=%s" % (name, lon, lat))

    check_outside_europe(lon, lat) # may raise ValueError!

    query = _get_query_reg_id(lon, lat)
    result_row = get_only_row(execute_query(conn, query), name)
    
    if result_row is None:
        LOGGER.warning('No region id found for lon %s, lat %s! Is this in the ocean?' % (lon, lat)) # OCEAN CASE
        error_message = ('No result found for lon %s, lat %s! Is this in the ocean?' % (round(lon, 3), round(lat, 3))) # OCEAN CASE
        LOGGER.error(error_message)
        raise ValueError(error_message)

    else:
        reg_id = result_row[0]
    LOGGER.debug("LEAVING: %s: lon=%s, lat=%s: %s" % (name, lon, lat, reg_id))
    return reg_id


def get_subc_id_basin_id(conn, lon, lat, reg_id):
    name = "get_subc_id_basin_id"
    LOGGER.debug('ENTERING: %s for lon=%s, lat=%s' % (name, lon, lat))
    
    # Getting info from database:
    query = _get_query_subc_id_basin_id(lon, lat, reg_id)
    result_row = get_only_row(execute_query(conn, query), name)
    
    if result_row is None:
        subc_id = None
        basin_id = None
        LOGGER.warning('No subc_id and basin_id. This should have been caught before. Does this latlon fall into the ocean?') # OCEAN CASE!
        error_message = ('No result (basin, subcatchment) found for lon %s, lat %s! Is this in the ocean?' % (lon, lat)) # OCEAN CASE
        LOGGER.error(error_message)
        raise ValueError(error_message)

    else:
        subc_id = result_row[0]
        basin_id = result_row[1]

    # Returning it...
    LOGGER.debug('LEAVING: %s for lon=%s, lat=%s --> subc_id %s, basin_id %s' % (name, lon, lat, subc_id, basin_id))
    return subc_id, basin_id 


###################################
### get results from SQL result ###
### GeoJSON                     ###
###################################

def get_upstream_catchment_bbox_polygon(conn, subc_id, upstream_ids, basin_id, reg_id):
    """
    Returns GeoJSON Geometry! Can be None / null!
    Example result:
    {"type": "Polygon", "coordinates": [[[9.913333333333334, 54.68833333333333], [9.913333333333334, 54.70583333333333], [9.931666666666667, 54.70583333333333], [9.931666666666667, 54.68833333333333], [9.913333333333334, 54.68833333333333]]]}
    """
    name = "get_upstream_catchment_bbox_polygon"
    LOGGER.debug('ENTERING: %s for subc_id %s' % (name, subc_id))
    
    if len(upstream_ids) == 0:
        LOGGER.warning('No upstream ids. Cannot get upstream catchment bbox.')
        LOGGER.info('LEAVING %s for subc_id %s: Returning empty geometry...' % (name, subc_id))
        return None # returning null geometry
        # A geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
        # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

    # Getting info from database:
    query = _get_query_upstream_bbox(upstream_ids, basin_id, reg_id)
    result_row = get_only_row(execute_query(conn, query), name)
    bbox_wkt = result_row[0]

    # Assembling GeoJSON to return:
    bbox_geojson = geomet.wkt.loads(bbox_wkt)
    LOGGER.debug('LEAVING: %s for subc_id %s --> Geometry/Polygon (bbox)' % (name, subc_id))
    return bbox_geojson


def get_upstream_catchment_dissolved_feature_coll(conn, subc_id, upstream_ids, lonlat, basin_id, reg_id, **kwargs):
    name = "get_upstream_catchment_dissolved_feature_coll"
    LOGGER.debug('ENTERING: %s for subc_id %s' % (name, subc_id))
    feature_dissolved_upstream = get_upstream_catchment_dissolved_feature(conn, subc_id, upstream_ids, basin_id, reg_id, **kwargs)
    # This feature's geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
    # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

    # Assembling GeoJSON Feature for the Point:
    feature_point = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [lonlat[0], lonlat[1]]
        },
        "properties": kwargs
    }

    # Assembling GeoJSON Feature Collection (point and dissolved upstream catchment):
    feature_coll = {
        "type": "FeatureCollection",
        "features": [feature_dissolved_upstream, feature_point]
    }

    LOGGER.debug('LEAVING: %s for subc_id %s --> Feature collection' % (name, subc_id))
    return feature_coll


def get_upstream_catchment_dissolved_feature(conn, subc_id, upstream_ids, basin_id, reg_id, **kwargs):
    name = "get_upstream_catchment_dissolved_feature"
    LOGGER.debug('ENTERING: %s for subc_id %s' % (name, subc_id))
    geometry_polygon = get_upstream_catchment_dissolved_geometry(conn, subc_id, upstream_ids, basin_id, reg_id)
    # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
    # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

    feature_dissolved_upstream = {
        "type": "Feature",
        "geometry": geometry_polygon,
        "properties": {
            "description": "Polygon of the upstream catchment of subcatchment %s" % subc_id,
            "num_upstream_catchments": len(upstream_ids),
            "upstream_subc_ids": upstream_ids,
            "downstream_subc_id": subc_id,
        }
    }

    if len(kwargs) > 0:
        feature_dissolved_upstream["properties"].update(kwargs)

    LOGGER.debug('LEAVING: %s for subc_id %s --> Feature (dissolved)' % (name, subc_id))
    return feature_dissolved_upstream


def get_upstream_catchment_dissolved_geometry(conn, subc_id, upstream_ids, basin_id, reg_id):
    """
    Example result:
    {"type": "Polygon", "coordinates": [[[9.916666666666668, 54.7025], [9.913333333333334, 54.7025], [9.913333333333334, 54.705], [9.915000000000001, 54.705], [9.915833333333333, 54.705], [9.915833333333333, 54.70583333333333], [9.916666666666668, 54.70583333333333], [9.916666666666668, 54.705], [9.918333333333335, 54.705], [9.918333333333335, 54.704166666666666], [9.919166666666667, 54.704166666666666], [9.919166666666667, 54.70333333333333], [9.920833333333334, 54.70333333333333], [9.920833333333334, 54.704166666666666], [9.924166666666668, 54.704166666666666], [9.925, 54.704166666666666], [9.925, 54.705], [9.926666666666668, 54.705], [9.9275, 54.705], [9.9275, 54.70583333333333], [9.928333333333335, 54.70583333333333], [9.928333333333335, 54.70333333333333], [9.929166666666667, 54.70333333333333], [9.929166666666667, 54.7025], [9.931666666666667, 54.7025], [9.931666666666667, 54.7], [9.930833333333334, 54.7], [9.930833333333334, 54.69833333333333], [9.930000000000001, 54.69833333333333], [9.929166666666667, 54.69833333333333], [9.929166666666667, 54.6975], [9.929166666666667, 54.696666666666665], [9.928333333333335, 54.696666666666665], [9.928333333333335, 54.695], [9.9275, 54.695], [9.9275, 54.693333333333335], [9.928333333333335, 54.693333333333335], [9.928333333333335, 54.69166666666666], [9.9275, 54.69166666666666], [9.9275, 54.69083333333333], [9.926666666666668, 54.69083333333333], [9.926666666666668, 54.69], [9.925833333333333, 54.69], [9.925, 54.69], [9.925, 54.68833333333333], [9.922500000000001, 54.68833333333333], [9.922500000000001, 54.69083333333333], [9.921666666666667, 54.69083333333333], [9.921666666666667, 54.69166666666666], [9.919166666666667, 54.69166666666666], [9.919166666666667, 54.692499999999995], [9.918333333333335, 54.692499999999995], [9.918333333333335, 54.693333333333335], [9.9175, 54.693333333333335], [9.9175, 54.695], [9.918333333333335, 54.695], [9.918333333333335, 54.69833333333333], [9.9175, 54.69833333333333], [9.9175, 54.700833333333335], [9.9175, 54.70166666666667], [9.916666666666668, 54.70166666666667], [9.916666666666668, 54.7025]]]}
    """
    name = "get_upstream_catchment_dissolved_geometry"
    LOGGER.debug('ENTERING: %s for subcid %s' % (name, subc_id))

    if len(upstream_ids) == 0:
        LOGGER.info('No upstream ids, so cannot even query! Returning none.')
        LOGGER.warning('No upstream ids. Cannot get dissolved upstream catchment.')
        return None # Returning null geometry!
        # A geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
        # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2
    
    # Get info from the database:
    query = _get_query_upstream_dissolved(upstream_ids, basin_id, reg_id)
    result_row = get_only_row(execute_query(conn, query), name)
    if result_row is None:
        LOGGER.warning('Received result_row None! This is weird. Existing upstream ids should have geometries.')
        err_msg = "Weird: No area (polygon) found in database for upstream catchments of subcatchment %s" % subc_id
        LOGGER.error(err_msg)
        raise ValueError(err_msg)

    # Assemble GeoJSON:
    dissolved_wkt = result_row[0]
    dissolved_geojson = geomet.wkt.loads(dissolved_wkt)
    LOGGER.debug('LEAVING: %s for subcid %s' % (name, subc_id))
    return dissolved_geojson


def get_simple_linestrings_for_subc_ids(conn, subc_ids, basin_id, reg_id):
    name = "get_simple_linestrings_for_subc_ids"
    LOGGER.debug('ENTERING: %s for %s subc_ids...' % (name, len(subc_ids)))
    query = _get_query_linestrings_for_subc_ids(subc_ids, basin_id, reg_id)
    num_rows = len(subc_ids)
    result_rows = get_rows(execute_query(conn, query), num_rows, name)

    # Create GeoJSON geometry from each linestring:
    # In case we want a GeometryCollection, which is more lightweight to return:
    linestrings_geojson = []
    for row in result_rows:

        geometry = None
        if row[2] is not None:
            geometry = geomet.wkt.loads(row[2])
        else:
            # Geometry errors that happen when two segments flow into one outlet (Vanessa, 17 June 2024)
            # For example, subc_id 506469602, when routing from 507056424 to outlet -1294020
            LOGGER.error('Subcatchment %s has no geometry!' % row[0]) # for example: 506469602
            # Features with empty geometries:
            # A geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
            # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

        linestrings_geojson.append(geometry)

    LOGGER.debug('LEAVING: %s for %s subc_ids...' % (name, len(subc_ids)))
    return linestrings_geojson


def get_feature_linestrings_for_subc_ids(conn, subc_ids, basin_id, reg_id):
    name = "get_feature_linestrings_for_subc_ids"
    LOGGER.debug('ENTERING: %s for %s subc_ids...' % (name, len(subc_ids)))
    query = _get_query_linestrings_for_subc_ids(subc_ids, basin_id, reg_id)
    num_rows = len(subc_ids)
    result_rows = get_rows(execute_query(conn, query), num_rows, name)

    # Create GeoJSON feature from each linestring:
    features_geojson = []
    for row in result_rows:

        geometry = None
        if row[2] is not None:
            geometry = geomet.wkt.loads(row[2])
        else:
            # Geometry errors that happen when two segments flow into one outlet (Vanessa, 17 June 2024)
            # For example, subc_id 506469602, when routing from 507056424 to outlet -1294020
            LOGGER.error('Subcatchment %s has no geometry!' % row[0]) # for example: 506469602
            # Features with empty geometries:
            # A geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
            # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "subc_id": row[0],
                "strahler_order": row[1]
            }
        }
        features_geojson.append(feature)

    LOGGER.debug('LEAVING: %s for %s subc_ids...' % (name, len(subc_ids)))
    return features_geojson


def get_polygon_for_subcid_simple(conn, subc_id, basin_id, reg_id):
    name = "get_polygon_for_subcid_simple"
    LOGGER.debug('ENTERING: %s for subc_id %s' % (name, subc_id))
    
    # Get info from database:
    query = _get_query_upstream_polygons([subc_id], basin_id, reg_id)
    result_row = get_only_row(execute_query(conn, query), name)
    
    if result_row is None:
        LOGGER.error('Received result_row None! This is weird. An existing subcatchment id should have a geometry!')
        err_msg = "Weird: No area (polygon) found in database for subcatchment %s" % subc_id
        LOGGER.error(err_msg)
        raise ValueError(err_msg)
        # Or allow it:
        #polygon_subcatchment = None
        # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
        # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2
    else:
        polygon_subcatchment = geomet.wkt.loads(result_row[1])

    LOGGER.debug('LEAVING: %s: Returning a single polygon: %s' % (name, str(polygon_subcatchment)[0:50]))
    return polygon_subcatchment


def get_polygon_for_subcid_feature(conn, subc_id, basin_id, reg_id, **kwargs):
    name = "get_polygon_for_subcid_feature"
    LOGGER.debug('ENTERING: %s for subc_id %s' % (name, subc_id))

    # Get info from database:
    polygon_subcatchment = get_polygon_for_subcid_simple(conn, subc_id, basin_id, reg_id)

    # Construct GeoJSON feature:
    feature_subcatchment = {
        "type": "Feature",
        "geometry": polygon_subcatchment,
        "properties": {
            "subcatchment_id": subc_id
        }
    }
    if len(kwargs) > 0:
        feature_subcatchment["properties"].update(kwargs)

    LOGGER.debug('LEAVING: %s: Returning a single polygon feature: %s' % (name, str(feature_subcatchment)[0:50]))
    return feature_subcatchment


def get_upstream_catchment_polygons_feature_coll(conn, subc_id, upstream_ids, basin_id, reg_id):
    name = "get_upstream_catchment_polygons_feature_coll"
    LOGGER.info("ENTERING: %s for subc_id: %s" % (name, subc_id))
    
    # No upstream ids: (TODO: This should be caught earlier, probably):
    # Feature Collections can have empty array according to GeoJSON spec::
    # https://datatracker.ietf.org/doc/html/rfc7946#section-3.3
    if len(upstream_ids) == 0:
        LOGGER.warning('No upstream ids. Cannot get upstream catchments (individual polygons) .')
        feature_coll = {
            "type": "FeatureCollection",
            "features": []
        }
        LOGGER.debug('LEAVING: %s for subcid %s: No upstream catchment, empty FeatureCollection!' % (name, subc_id))
        return feature_coll

    # Get info from database:
    query = _get_query_upstream_polygons(upstream_ids, basin_id, reg_id)
    num_rows = len(upstream_ids)
    result_rows = get_rows(execute_query(conn, query), num_rows, name)
    if result_rows is None:
        err_msg = 'Received result_rows None! This is weird. Existing upstream ids should have geometries.'
        LOGGER.error(err_msg)
        raise ValueError(err_msg)

    # Construct GeoJSON feature:
    features_geojson = []
    for row in result_rows:
        feature = {
            "type": "Feature",
            "geometry": geomet.wkt.loads(row[1]),
            "properties": {
                "subcatchment_id": row[0]
            }

        }

        features_geojson.append(feature)

    feature_coll = {
        "type": "FeatureCollection",
        "features": features_geojson
    }

    LOGGER.debug('LEAVING: %s: Returning a FeatureCollection with Polygons...' % (name))
    return feature_coll
    

def get_upstream_catchment_polygons_geometry_coll(conn, subc_id, upstream_ids, basin_id, reg_id):
    name = "get_upstream_catchment_polygons_geometry_coll"
    LOGGER.info("ENTERING: %s for subc_id: %s" % (name, subc_id))

    # No upstream ids: (TODO: This should be caught earlier, probably):
    # Geometry Collections can have empty array according to GeoJSON spec: ??? WIP TODO CHECK
    # https://datatracker.ietf.org/doc/html/rfc7946#section-3.3
    if len(upstream_ids) == 0:
        LOGGER.warning('No upstream ids. Cannot get upstream catchments (individual polygons) .')
        geometry_coll = {
            "type": "GeometryCollection",
            "geometries": []
        }
        LOGGER.debug('LEAVING: %s for subcid %s: No upstream catchment, empty GeometryCollection!' % (name, subc_id))
        return geometry_coll

    # Get info from database:
    query = _get_query_upstream_polygons(upstream_ids, basin_id, reg_id)
    num_rows = len(upstream_ids)
    result_rows = get_rows(execute_query(conn, query), num_rows, name)
    if result_rows is None:
        err_msg = 'Received result_rows None! This is weird. Existing upstream ids should have geometries.'
        LOGGER.error(err_msg)
        raise ValueError(err_msg)

    # Construct GeoJSON feature:
    geometries_geojson = []
    for row in result_rows:
        geometries_geojson.append(geomet.wkt.loads(row[1]))

    geometry_coll = {
        "type": "GeometryCollection",
        "geometries": geometries_geojson
    }

    LOGGER.debug('LEAVING: %s: Returning a GeometryCollection with Polygons...' % (name))
    return geometry_coll


def get_dijkstra_ids(conn, subc_id_start, subc_id_end, reg_id, basin_id):
    '''
    INPUT: subc_ids (start and end)
    OUTPUT: subc_ids (the entire path, incl. start and end)
    '''
    name = "get_dijkstra_ids"
    LOGGER.info("ENTERING: %s for subc_ids: %s and %s" % (name, subc_id_start, subc_id_end))
    query = _get_query_dijkstra(subc_id_start, subc_id_end, reg_id, basin_id)
    num_rows = 10000 # TODO WIP we don't know how many!
    result_rows = get_rows(execute_query(conn, query), num_rows, name)

    all_ids = [subc_id_start] # Adding start segment, as it is not included in database return!
    i = 0
    for row in result_rows:
        i += 1
        if row[0] == -1: # pgr_dijkstra returns -1 as the last row...
            pass
        else:
            all_ids.append(row[0]) # these are already integer!

    LOGGER.debug('LEAVING: %s: Returning %s subc_ids...' % (name, len(all_ids)))
    return all_ids


def get_upstream_catchment_ids_incl_itself(conn, subc_id, basin_id, reg_id, include_itself = True):
    name = "get_upstream_catchment_ids_incl_itself"
    LOGGER.info("ENTERING: %s for subc_id: %s" % (name, subc_id))

    # Getting info from database:
    query = _get_query_upstream(subc_id, reg_id, basin_id)
    result_row = get_only_row(execute_query(conn, query), name)

    # If no upstream catchments are returned:
    if result_row is None:
        LOGGER.info('No upstream catchment returned. Assuming this is a headwater. Returning just the local catchment itself.')
        return [subc_id]

    # Getting the info from the database:
    upstream_catchment_subcids = result_row[1]

    # superfluous warning:
    subc_id_returned = result_row[0]
    if not int(subc_id) == int(subc_id_returned):
        msg = "WARNING: Wrong subc_id! Provided: %s, returned: %s." % (subc_id, subc_id_returned)
        LOGGER.error(msg)
        raise ValueError(msg)

    # Adding the subcatchment itself if it not returned:
    if not subc_id in upstream_catchment_subcids:
        upstream_catchment_subcids.append(subc_id)
        LOGGER.info('FYI: The database did not return the local subcatchment itself in the list of upstream subcatchments, so added it.')
    else:
        LOGGER.debug('FYI: The database returned the local subcatchment itself in the list of upstream subcatchments, which is fine.')

    # Stop any computations with more than x upstream catchments!
    # TODO: Allow returning them, but then nothing else!
    max_num = 200
    if len(upstream_catchment_subcids) > max_num:
        LOGGER.warning('Limiting queries to %s upstream subcatchments' % max_num)
        LOGGER.info("LEAVING EMPTY: %s for subc_id (found %s upstream ids): %s" % (name, len(upstream_catchment_subcids), subc_id))
        #return []
        raise ValueError('Found %s subcatchments, but temporarily, calculations over %s subcatchments are not done.' % 
            (len(upstream_catchment_subcids), max_num))

    LOGGER.info("LEAVING: %s for subc_id (found %s upstream ids): %s" % (name, len(upstream_catchment_subcids), subc_id))
    return upstream_catchment_subcids


# TODO MOVE TO OTHER SECTION
def get_upstream_catchment_ids_without_itself(conn, subc_id, basin_id, reg_id, include_itself = False):
    name = "get_upstream_catchment_ids_without_itself"
    LOGGER.info("ENTERING: %s for subc_id: %s" % (name, subc_id))

    # Getting info from database:
    query = _get_query_upstream(subc_id, reg_id, basin_id)
    result_row = get_only_row(execute_query(conn, query), name)
    
    # If no upstream catchments are returned:
    if result_row is None:
        LOGGER.info('No upstream catchment returned. Assuming this is a headwater. Returning an empty array.')
        return []

    upstream_catchment_subcids = result_row[1]
    
    # superfluous warning:
    subc_id_returned = result_row[0]
    if not subc_id == subc_id_returned:
        msg = "WARNING: Wrong subc_id!"
        LOGGER.error(msg)
        raise ValueError(msg)

    # remove itself
    if subc_id_returned in upstream_catchment_subcids:
        upstream_catchment_subcids.remove(subc_id_returned)
        LOGGER.info('FYI: The database returned the local subcatchment itself in the list of upstream subcatchments, which is not fine, so we removed it.')
    else:
        LOGGER.debug('FYI: The database did not return the local subcatchment itself in the list of upstream subcatchments, which is fine.')

    # Stop any computations with more than x upstream catchments!
    # TODO: Allow returning them, but then nothing else!
    max_num = 200
    if len(upstream_catchment_subcids) > max_num:
        LOGGER.warning('Limiting queries to %s upstream subcatchments' % max_num)
        LOGGER.info("LEAVING EMPTY: %s for subc_id (found %s upstream ids): %s" % (name, len(upstream_catchment_subcids), subc_id))
        #return []
        raise ValueError('Found %s subcatchments, but temporarily, calculations over %s subcatchments are not done.' % 
            (len(upstream_catchment_subcids), max_num))

    LOGGER.info("LEAVING: %s for subc_id (found %s upstream ids): %s" % (name, len(upstream_catchment_subcids), subc_id))
    return upstream_catchment_subcids


def get_snapped_point_simple(conn, lon, lat, subc_id, basin_id, reg_id):
    """
    Example result:
    2, {"type": "Point", "coordinates": [9.931555, 54.69625]}, {"type": "LineString", "coordinates": [[9.929583333333333, 54.69708333333333], [9.930416666666668, 54.69625], [9.932083333333335, 54.69625], [9.933750000000002, 54.694583333333334], [9.934583333333334, 54.694583333333334]]}

    """
    name = "get_snapped_point_simple"
    LOGGER.debug("ENTERING: %s for point: lon=%s, lat=%s (subc_id %s)" % (name, lon, lat, subc_id))
    
    # Getting info from database:
    query = _get_query_snapped(lon, lat, subc_id, basin_id, reg_id)
    result_row = get_only_row(execute_query(conn, query), name)
    if result_row is None:
        LOGGER.warning("%s: Received result_row None for point: lon=%s, lat=%s (subc_id %s). This is weird. Any point should be snappable, right?" % (name, lon, lat, subc_id))
        err_msg = "Weird: Could not snap point lon=%s, lat=%s" % (lon, lat) 
        LOGGER.error(err_msg)
        raise ValueError(err_msg)
        # Or return features with empty geometries:
        # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
        # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2
        #snappedpoint_geojson = None
        #streamsegment_geojson = None
        #strahler = None

    else:
        LOGGER.debug('Extracting from database...')
        strahler = result_row[0]
        snappedpoint_wkt = result_row[1]
        streamsegment_wkt = result_row[2]
        LOGGER.debug('Transforming to GeoJSON...')
        snappedpoint_point = geomet.wkt.loads(snappedpoint_wkt)
        streamsegment_linestring = geomet.wkt.loads(streamsegment_wkt)
        #LOGGER.debug("This is the snapped point for point: lon=%s, lat=%s (subc_id %s): %s" % (lon, lat, subc_id, snappedpoint_geojson))
        #LOGGER.debug("This is the stream segment for point: lon=%s, lat=%s (subc_id %s): %s" % (lon, lat, subc_id, streamsegment_geojson))
        #lon_snap = snappedpoint_geojson["coordinates"][0]
        #lat_snap = snappedpoint_geojson["coordinates"][1]
        LOGGER.debug("LEAVING: %s for point: lon=%s, lat=%s (subc_id %s)" % (name, lon, lat, subc_id))
        return strahler, snappedpoint_point, streamsegment_linestring


def get_snapped_point_feature(conn, lon, lat, subc_id, basin_id, reg_id, **kwargs):
    name = "get_snapped_point_feature"
    LOGGER.debug("ENTERING: %s for point: lon=%s, lat=%s (subc_id %s)" % (name, lon, lat, subc_id))

    strahler, point_snappedpoint, linestring_streamsegment = get_snapped_point_simple(conn, lon, lat, subc_id, basin_id, reg_id)
    feature_snappedpoint = {
        "type": "Feature",
        "geometry": point_snappedpoint,
        "properties": {
            "subcatchment_id": subc_id,
            "basin_id": basin_id,
            "reg_id": reg_id,
            "lon_original": lon,
            "lat_original": lat,
        }
    }

    feature_streamsegment = {
        "type": "Feature",
        "geometry": linestring_streamsegment,
        "properties": {
            "subcatchment_id": subc_id,
            "basin_id": basin_id,
            "reg_id": reg_id,
            "strahler_order": strahler
        }
    }

    if len(kwargs) > 0:
        feature_snappedpoint["properties"].update(kwargs)
        feature_streamsegment["properties"].update(kwargs)

    LOGGER.debug("LEAVING: %s for point: lon=%s, lat=%s (subc_id %s)" % (name, lon, lat, subc_id))
    return strahler, feature_snappedpoint, feature_streamsegment


def get_strahler_and_stream_segment_feature(conn, subc_id, basin_id, reg_id, **kwargs):
    name = "get_strahler_and_stream_segment_feature"
    LOGGER.debug('ENTERING: %s for subcid %s' % (name, subc_id))

    # Getting info from database:
    strahler, stream_segment_linestring = get_strahler_and_stream_segment_linestring(conn, subc_id, basin_id, reg_id)

    # Assembling GeoJSON feature to return:
    feature = {
        "type": "Feature",
        "geometry": stream_segment_linestring,
        "properties": {
            "subcatchment_id": subc_id,
            "strahler_order": strahler,
            "basin_id": basin_id,
            "reg_id": reg_id
        }
    }

    if len(kwargs) > 0:
        feature["properties"].update(kwargs)

    LOGGER.debug('LEAVING: %s for subcid %s' % (name, subc_id))
    return feature


def get_strahler_and_stream_segment_linestring(conn, subc_id, basin_id, reg_id):
    # TODO Make one query for various subc_ids! When would this be needed?
    """

    Stream segment is returned as a single LineString.
    Cannot return valid geoJSON, because this returns just the geometry, where we
    cannot add the strahler order as property.

    Example result:
    2, {"type": "LineString", "coordinates": [[9.929583333333333, 54.69708333333333], [9.930416666666668, 54.69625], [9.932083333333335, 54.69625], [9.933750000000002, 54.694583333333334], [9.934583333333334, 54.694583333333334]]}
    """
    name = "get_strahler_and_stream_segment_linestring"
    LOGGER.debug("ENTERING: %s for subc_id %s)" % (name, subc_id))

    # Getting info from the database:
    query = _get_query_segment(subc_id, basin_id, reg_id)
    result_row = get_only_row(execute_query(conn, query), name)
    
    # Database returns nothing:
    if result_row is None:
        LOGGER.error('Received result_row None! This is weird. An existing subcatchment id should have a linestring geometry!')
        err_msg = "Weird: No stream segment (linestring) found in database for subcatchment %s" % subc_id
        LOGGER.error(err_msg)
        raise ValueError(err_msg)
    
    # Getting geomtry from database result:
    strahler = result_row[0]
    streamsegment_wkt = result_row[1]
    streamsegment_linestring = geomet.wkt.loads(streamsegment_wkt)
    LOGGER.debug("LEAVING: %s for subc_id %s: %s, %s" % (name, subc_id, strahler, str(streamsegment_linestring)[0:50]))
    return strahler, streamsegment_linestring

    

###########################
### database connection ###
###########################


def open_ssh_tunnel(ssh_host, ssh_username, ssh_password, remote_host, remote_port, verbose=False):
    """Open an SSH tunnel and connect using a username and password.
    
    :param verbose: Set to True to show logging
    :return tunnel: Global SSH tunnel connection
    """
    LOGGER.info("Opening SSH tunnel...")
    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
    
    #global tunnel
    tunnel = sshtunnel.SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username = ssh_username,
        ssh_password = ssh_password,
        remote_bind_address=(remote_host, remote_port)
    )
    LOGGER.debug("Starting SSH tunnel...")
    tunnel.start()
    LOGGER.debug("Starting SSH tunnel... done.")
    return tunnel


def connect_to_db(geofresh_server, db_port, database_name, database_username, database_password):
    # This blocks! Cannot run KeyboardInterrupt
    LOGGER.debug("Connecting to db...")
    conn = psycopg2.connect(
       database=database_name,
       user=database_username,
       password=database_password,
       host=geofresh_server,
       port= str(db_port)
    )
    LOGGER.debug("Connecting to db... done.")
    return conn


def get_connection_object(geofresh_server, geofresh_port,
    database_name, database_username, database_password,
    verbose=False, use_tunnel=False, ssh_username=None, ssh_password=None):
    if use_tunnel:
        # See: https://practicaldatascience.co.uk/data-science/how-to-connect-to-mysql-via-an-ssh-tunnel-in-python
        ssh_host = geofresh_server
        remote_host = "127.0.0.1"
        remote_port = geofresh_port
        tunnel = open_ssh_tunnel(ssh_host, ssh_username, ssh_password, remote_host, remote_port, verbose)
        conn = connect_to_db(remote_host, tunnel.local_bind_port, database_name, database_username, database_password)
    else:
        conn = connect_to_db(geofresh_server, geofresh_port, database_name, database_username, database_password)
    return conn


def execute_query(conn, query):
    LOGGER.debug("Executing query...")
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor


def get_rows(cursor, num_rows, comment='unspecified function'):
    LOGGER.debug('get-rows (%s) for %s' % (num_rows, comment))
    i = 0
    return_rows = []
    while True:
        i += 1
        #LOGGER.debug("Fetching row %s..." % i)
        this_row = cursor.fetchone();
        if this_row is None and i == 1:
            LOGGER.error('Database returned no results at all (expected %s rows).' % num_rows)
            break
        elif this_row is None:
            break
        elif i <= num_rows:
            return_rows.append(this_row) # TODO: Do we need this? Just leave out the expected num_rows and let the "if this_row is None" do its job
        else:
            LOGGER.warning("Found more than %s rows in result! Row %s: %s" % (num_rows, i, this_row))
            LOGGER.info("WARNING: More than one row output! Will ignore row %s..." % i)

    return return_rows


def get_only_row(cursor, comment='unspecified function'):
    LOGGER.debug('get-only-row for function %s' % comment)
    i = 0
    return_row = None
    while True:
        i += 1
        #LOGGER.debug("Fetching row %s..." % i)
        this_row = cursor.fetchone()
        if this_row is None and i == 1:
            LOGGER.error('Database returned no results at all (expected one row).')
            break
        elif this_row is None:
            break
        elif i == 1:
            return_row = this_row
            LOGGER.debug("First and only row: %s" % str(this_row))
        else:
            # We are asking for one point, so the result should be just one row!
            # But if the point is exactly on a boundary, two can be returned! TODO how to deal with?
            # Example:
            # SELECT sub.subc_id, sub.basin_id FROM sub_catchments sub WHERE st_intersects(ST_SetSRID(ST_MakePoint(9.921666666666667, 54.69166666666666),4326), sub.geom) AND sub.reg_id = 58;
            LOGGER.warning("Found more than 1 row in result! Row %s: %s" % (i, this_row))
            print("WARNING: More than one row output! Will ignore row %s..." % i)

    if return_row is None:
        LOGGER.error('Returning none, because we expected one row but got none (for %s).' % comment)

    return return_row



if __name__ == "__main__":

    # This part is for testing the various functions, that"s why it is a bit makeshift.
    # In production, they would be called from the pygeoapi processes.
    #
    # source /home/mbuurman/work/pyg_geofresh/venv/bin/activate
    # python /home/mbuurman/work/pyg_geofresh/pygeoapi/pygeoapi/process/geofresh/py_query_db.py 9.931555 54.695070 dbpw pw
    #    where dbpw is the database passwort for postgresql, can be found in ~/.pgpass if you have access.
    #    where pw is your personal LDAP password for the ssh tunnel.

    if len(sys.argv) == 2:
        dbpw = sys.argv[1]
        mbpw = None
        use_tunnel = False

    elif len(sys.argv) == 3:
        dbpw = sys.argv[1]
        mbpw = sys.argv[2]
        use_tunnel = True
        print('Will try to make ssh tunnel with password "%s..."' % mbpw[0:1])

    else:
        print('Please provide a database password and (possibly an ssh tunnel password)...')
        sys.exit(1)

    verbose = True

    # Connection details:
    geofresh_server = "172.16.4.76"  # Hard-coded for testing
    geofresh_port = 5432             # Hard-coded for testing
    database_name = "geofresh_data"  # Hard-coded for testing
    database_username = "shiny_user" # Hard-coded for testing
    database_password = dbpw

    # Connection details for SSH tunneling:
    ssh_username = "mbuurman" # Hard-coded for testing
    ssh_password = mbpw
    localhost = "127.0.0.1"

    # Logging
    LOGGER = logging.getLogger()
    console = logging.StreamHandler()
    LOGGER.setLevel(logging.DEBUG)
    formatter = logging.Formatter("xxx %(name)-12s: %(levelname)-8s %(message)s")
    console.setFormatter(formatter)
    LOGGER.addHandler(console)

    conn = get_connection_object(geofresh_server, geofresh_port,
        database_name, database_username, database_password,
        verbose=verbose, use_tunnel=use_tunnel,
        ssh_username=ssh_username, ssh_password=ssh_password)

    # Data for testing:
    # These coordinates are in Vantaanjoki, reg_id = 65, basin_id = 1274183, subc_id = 553495421
    #lat = 60.7631596
    #lon = 24.8919571
    # These coordinates are in Schlei, reg_id = 58, basin_id = 1292547, subc_id = 506251252
    lat = 54.695070
    lon = 9.931555

    # Run all queries:
    print("\n(1) reg_id: ")
    reg_id = get_reg_id(conn, lon, lat)
    print("\nRESULT REG_ID: %s" % reg_id)

    print("\n(2) subc_id, basin_id: ")
    subc_id, basin_id = get_subc_id_basin_id(conn, lon, lat, reg_id)
    print("\nRESULT BASIN_ID, SUBC_ID: %s, %s" % (basin_id, subc_id))
    
    print("\n(3) upstream catchment ids: ")
    upstream_ids = get_upstream_catchment_ids_incl_itself(
        conn, subc_id, basin_id, reg_id)
    print("\nRESULT UPSTREAM IDS:\n%s" % upstream_ids)
    
    print("\n(4) strahler, snapped point, stream segment: ")
    strahler, feature_snappedpoint, feature_streamsegment = get_snapped_point_feature(
        conn, lon, lat, subc_id, basin_id, reg_id, bla='test')
    print("\nRESULT STRAHLER: %s" % strahler)
    print("RESULT SNAPPED (Feature/Point):\n%s" % feature_snappedpoint)
    print("\nRESULT SEGMENT (Feature/Linestring):\n%s" % feature_streamsegment)

    print("\n(5) strahler, stream segment: ")
    strahler, streamsegment_linestring = get_strahler_and_stream_segment_linestring(conn, subc_id, basin_id, reg_id)
    streamsegment_feature = get_strahler_and_stream_segment_feature(conn, subc_id, basin_id, reg_id, bla='test')
    print("\nRESULT STRAHLER: %s" % strahler)
    print("RESULT SEGMENT (Geometry/Linestring):\n%s" % streamsegment_linestring)
    print("\nRESULT SEGMENT FEATURE (Feature/Linestring):\n%s" % streamsegment_feature)

    print("\n(6a) upstream catchment bbox as geometry: ")
    bbox_geojson = get_upstream_catchment_bbox_polygon(
        conn, subc_id, upstream_ids, basin_id, reg_id)
    print("\nRESULT BBOX (Geometry/Polygon)\n%s" % bbox_geojson)

    print("\n(7) upstream catchment polygons: ")
    poly_collection = get_upstream_catchment_polygons_feature_coll(
        conn, subc_id, upstream_ids, basin_id, reg_id)
    print("\nRESULT UPSTREAM POLYGONS (FeatureCollection/MultiPolygons)\n%s" % poly_collection)

    print("\n(8a): dissolved polygon as geometry/polygon")
    dissolved_polygon = get_upstream_catchment_dissolved_geometry(
        conn, subc_id, upstream_ids, basin_id, reg_id)
    print("\nRESULT DISSOLVED (Geometry/Polygon): \n%s" % dissolved_polygon)

    print("\n(8b): dissolved polygon as feature")
    dissolved_feature = get_upstream_catchment_dissolved_feature(
        conn, subc_id, upstream_ids, basin_id, reg_id, bla='test')
    print("\nRESULT DISSOLVED (Feature/Polygon)): \n%s" % dissolved_feature)

    print("\n(8c): dissolved polygon as feature coll")
    dissolved_feature_coll = get_upstream_catchment_dissolved_feature_coll(
        conn, subc_id, upstream_ids, (lon, lat), basin_id, reg_id, bla='test')
    print("\nRESULT DISSOLVED (FeatureCollection/Polygon): \n%s" % dissolved_feature_coll)

    ###################################
    ### dijkstra between two points ###
    ###################################

    print("\n(9) DIJKSTRA ")
    # Falls into: 506 519 922, basin 1285755
    #lat2 = 53.695070
    #lon2 = 9.751555
    # Falls on boundary, error:
    #lon2 = 9.921666666666667 # falls on boundary!
    #lat2 = 54.69166666666666 # falls on boundary!
    # Falls into 506 251 713
    lon1 = 9.937520027160646
    lat1 = 54.69422745526058
    # Falls into: 506 251 712, basin 1292547
    lon2 = 9.9217
    lat2 = 54.6917
    subc_id_start, basin_id_dijkstra = get_subc_id_basin_id(conn, lon1, lat1, reg_id)
    subc_id_end, basin_id_end = get_subc_id_basin_id(conn, lon2, lat2, reg_id)
    print('Using start  subc_id: %s (%s)' % (subc_id_start, basin_id_dijkstra))
    print('Using target subc_id: %s (%s)' % (subc_id_end, basin_id_end))

    # Just the Ids:
    segment_ids = get_dijkstra_ids(conn, subc_id_start, subc_id_end, reg_id, basin_id_dijkstra)
    print('\nRESULT DIJKSTRA PATH segment_ids: %s\n' % segment_ids)
    
    # Feature Coll
    feature_list = get_feature_linestrings_for_subc_ids(conn, segment_ids, basin_id_dijkstra, reg_id1)
    feature_coll = {"type": "FeatureCollection", "features": feature_list}
    print('\nRESULT DIJKSTRA PATH TO SEA (FeatureCollection/LineStrings):\n%s' % feature_coll)
    
    # GeometryColl
    dijkstra_path_list = get_simple_linestrings_for_subc_ids(conn, segment_ids, basin_id_dijkstra, reg_id)
    coll = {"type": "GeometryCollection", "geometries": dijkstra_path_list}
    print('\nRESULT DIJKSTRA PATH TO SEA (GeometryCollection):\n%s' % coll)

    #######################
    ### dijkstra to sea ###
    #######################

    print("\n(9b) DIJKSTRA TO SEA")
    # Falls into: 506 251 712, basin 1292547
    #lon1 = 9.937520027160646
    #lat1 = 54.69422745526058
    # Far away from sea, but yields no result at all!
    #lon1 = 10.599210072990063
    #lat1 = 51.31162492387419
    # bei Bremervoerde, leads to one non-geometry subcatchment, subc_id : 506469602
    lat1 = 53.397626302268684
    lon1 = 9.155709977606723
    # Not sure where this is:
    lat1 = 52.76220968996532
    lon1 = 11.558802055604199
    subc_id_start, basin_id_dijkstra = get_subc_id_basin_id(conn, lon1, lat1, reg_id)
    subc_id_end = -basin_id_dijkstra
    print('Using start  subc_id: %s (%s)' % (subc_id_start, basin_id_dijkstra))
    print('Using target subc_id: %s (%s)' % (subc_id_end, basin_id_dijkstra))
    
    # Just the Ids:
    segment_ids = get_dijkstra_ids(conn, subc_id_start, subc_id_end, reg_id, basin_id_dijkstra)
    print('\nRESULT DIJKSTRA PATH TO SEA segment_ids: %s\n' % segment_ids)
    
    # Feature Coll
    #coll = get_dijkstra_linestrings_feature_coll(conn, subc_id_start, subc_id_end, reg_id, basin_id_dijkstra, destination="sea")
    feature_list = get_feature_linestrings_for_subc_ids(conn, segment_ids, basin_id_dijkstra, reg_id1)
    feature_coll = {"type": "FeatureCollection", "features": feature_list}
    print('\nRESULT DIJKSTRA PATH TO SEA (FeatureCollection/LineStrings):\n%s' % feature_coll)
    
    # GeometryColl
    dijkstra_path_list = get_simple_linestrings_for_subc_ids(conn, segment_ids, basin_id_dijkstra, reg_id)
    coll = {"type": "GeometryCollection", "geometries": dijkstra_path_list}
    print('\nRESULT DIJKSTRA PATH TO SEA (GeometryCollection):\n%s' % coll)

    ##################################
    ### local subcatchment polygon ###
    ##################################

    print("\n(10) Catchment polygon: ")
    polygon = get_polygon_for_subcid_simple(conn, subc_id, basin_id, reg_id)
    print("\nRESULT CATCHMENT (Geometry/Polygon)\n%s\n" % polygon)
    feature = get_polygon_for_subcid_feature(conn, subc_id, basin_id, reg_id, bla='test')
    print("\nRESULT CATCHMENT (Feature/Polygon)\n%s\n" % feature)


    # Finally:
    print("Closing connection...")
    conn.close()
    print("Done")
