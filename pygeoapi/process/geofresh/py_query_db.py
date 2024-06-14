import psycopg2
import sys
import logging
import sshtunnel
import geomet.wkt
LOGGER = logging.getLogger(__name__)


#######################
### Get SQL queries ###
#######################

def _get_query_reg_id(lon, lat):
    """
    Example query:
    SELECT reg.reg_id FROM regional_units reg
    WHERE st_intersects(ST_SetSRID(ST_MakePoint(9.931555, 54.695070),4326), reg.geom);

    Result:
     reg_id 
    --------
         58
    (1 row)
    """
    query = """
    SELECT reg.reg_id
    FROM regional_units reg
    WHERE st_intersects(ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326), reg.geom)
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
    sub.subc_id,
    sub.basin_id
    FROM sub_catchments sub
    WHERE st_intersects(ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326), sub.geom)
    AND sub.reg_id = {poi_reg_id}
    """.format(longitude = lon, latitude = lat, poi_reg_id = reg_id)
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
    seg.strahler,
    ST_AsText(ST_LineInterpolatePoint(seg.geom, ST_LineLocatePoint(seg.geom, ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326)))),
    ST_AsText(seg.geom)
    FROM hydro.stream_segments seg
    WHERE seg.subc_id = {subc_id}
    AND seg.basin_id = {basin_id}
    AND seg.reg_id = {reg_id}
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
    seg.strahler,
    ST_AsText(seg.geom)
    FROM hydro.stream_segments seg
    WHERE seg.subc_id = {subc_id}
    AND seg.reg_id = {reg_id}
    AND seg.basin_id = {basin_id}
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
    SELECT {poi_subc_id}, array_agg(node)::bigint[] AS nodes 
    FROM pgr_connectedComponents('
        SELECT
        basin_id,
        subc_id AS id,
        subc_id AS source,
        target,
        length AS cost
        FROM hydro.stream_segments
        WHERE reg_id = {poi_reg_id}
        AND basin_id = {poi_basin_id}
        AND subc_id != {poi_subc_id}
    ') WHERE component > 0 GROUP BY component;
    '''.format(poi_subc_id = subc_id, poi_reg_id = reg_id, poi_basin_id = basin_id)

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

def _get_query_upstream_linestrings(upstream_ids, basin_id, reg_id):
    '''
    Example query:
    SELECT  seg.subc_id, seg.strahler, ST_AsText(seg.geom)
    FROM hydro.stream_segments seg WHERE seg.subc_id IN (506250459, 506251015, 506251126, 506251712);
    '''
    ids = ", ".join([str(elem) for elem in upstream_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712

    query = '''
    SELECT 
    seg.subc_id, seg.strahler, ST_AsText(seg.geom)
    FROM hydro.stream_segments seg
    WHERE seg.subc_id IN ({ids})
    AND seg.reg_id = {reg_id}
    AND seg.basin_id = {basin_id}
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

def get_reg_id(conn, lon, lat):
    name = "get_reg_id"
    LOGGER.debug("ENTERING: %s: lon=%s, lat=%s" % (name, lon, lat))
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


def get_upstream_catchment_bbox_feature(conn, subc_id, upstream_ids, basin_id, reg_id, **kwargs):
    name = "get_upstream_catchment_bbox_feature"
    LOGGER.debug('ENTERING: %s for subc_id %s' % (name, subc_id))

    # Get information:
    bbox_geojson = get_upstream_catchment_bbox_polygon(conn, subc_id, upstream_ids, basin_id, reg_id)
    # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
    # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

    # Assembling GeoJSON to return:
    feature = {
        "type": "Feature",
        "geometry": bbox_geojson,
        "properties": {
            "description": "Bounding box of the upstream catchment of subcatchment %s" % subc_id,
            "upstream_subc_ids": upstream_ids,
            "downstream_subc_id": subc_id,
            "basin_id": basin_id,
            "reg_id": reg_id,
        }
    }

    if len(kwargs) > 0:
        feature["properties"].update(kwargs)

    LOGGER.debug('LEAVING: %s for subc_id %s --> Feature/Polygon (bbox)' % (name, subc_id))
    return feature

def get_upstream_catchment_dissolved_feature_coll(conn, subc_id, upstream_ids, basin_id, reg_id, lonlat=None, **kwargs):
    name = "get_upstream_catchment_dissolved_feature_coll"
    LOGGER.debug('ENTERING: %s for subc_id %s' % (name, subc_id))

    # Getting information from other function:
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
    return feature_dissolved_upsteram

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



def get_upstream_catchment_linestrings_feature_coll(conn, subc_id, upstream_ids, basin_id, reg_id):
    name = "get_upstream_catchment_linestrings_feature_coll"
    LOGGER.debug('ENTERING: %s for subcid %s' % (name, subc_id))
    
    # No upstream ids: (TODO: This should be caught earlier, probably):
    # Feature Collections can have empty array according to GeoJSON spec::
    # https://datatracker.ietf.org/doc/html/rfc7946#section-3.3
    if len(upstream_ids) == 0:
        LOGGER.warning('No upstream ids. Cannot get upstream linestrings .')
        feature_coll = {
            "type": "FeatureCollection",
            "features": []
        }
        LOGGER.debug('LEAVING: %s for subcid %s: No upstream catchment, empty feature collection!' % (name, subc_id))
        return feature_coll

    if len(upstream_ids) == 1 and subc_id == upstream_ids[0]:
        LOGGER.debug('Upstream catchments equals subcatchment!')
    
    # Getting info from database:
    query = _get_query_upstream_linestrings(upstream_ids, basin_id, reg_id)
    num_rows = len(upstream_ids)
    result_rows = get_rows(execute_query(conn, query), num_rows, name)

    # Assembling GeoJSON from that:
    features_geojson = []
    for row in result_rows:
        feature = {
            "type": "Feature",
            "geometry": geomet.wkt.loads(row[2]),
            "properties": {
                "subcatchment_id": row[0],
                "basin_id": basin_id,
                "reg_id": reg_id,
                "strahler_order": row[1],
                "part_of_upstream_catchment_of": subc_id,
            }

        }

        features_geojson.append(feature)

    feature_coll = {
        "type": "FeatureCollection",
        "features": features_geojson
    }

    LOGGER.debug('LEAVING: %s for subcid %s' % (name, subc_id))
    return feature_coll

def get_polygon_for_subcid_feature(conn, subc_id, basin_id, reg_id):
    name = "get_polygon_for_subcid_feature"
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

    # Construct GeoJSON feature:
    feature_subcatchment = {
        "type": "Feature",
        "geometry": polygon_subcatchment,
        "properties": {
            "subcatchment_id": subc_id
        }
    }
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
        LOGGER.debug('LEAVING: %s for subcid %s: No upstream catchment, empty feature collection!' % (name, subc_id))
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
                "subcatchment_id": row[0],
                "part_of_upstream_catchment_of": subc_id,
                "basin_id": basin_id,
                "reg_id": reg_id
            }

        }

        features_geojson.append(feature)

    feature_coll = {
        "type": "FeatureCollection",
        "features": features_geojson
    }

    LOGGER.debug('LEAVING: %s: Returning a polygon feature collection...' % (name))
    return feature_coll
    
    # In case we want a GeometryCollection, which is more lightweight to return:  
    #polygons_geojson = []
    #for row in result_rows:
    #    polygons_geojson.append(geomet.wkt.loads(row[1]))
    #geometry_coll = {
    #     "type": "GeometryCollection",
    #     "geometries": polygons_geojson
    #}
    #return geometry_coll


# TODO MOVE TO OTHER SECTION
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
    if not subc_id == subc_id_returned:
        msg = "WARNING: Wrong subc_id!"
        LOGGER.error(msg)
        raise ValueError(msg)

    # Adding the subcatchment itself if it not returned:
    if not subc_id in upstream_catchment_subcids:
        upstream_catchment_subcids.append(subc_id)
        LOGGER.info('FYI: The database did not return the local subcatchment itself in the list of upstream subcatchments, so added it.')
    else:
        LOGGER.debug('FYI: The database returned the local subcatchment itself in the list of upstream subcatchments, which is fine.')

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

def get_snapped_point_feature(conn, lon, lat, subc_id, basin_id, reg_id):
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

    LOGGER.debug("LEAVING: %s for point: lon=%s, lat=%s (subc_id %s)" % (name, lon, lat, subc_id))
    return strahler, feature_snappedpoint, feature_streamsegment


def get_strahler_and_stream_segment_feature(conn, subc_id, basin_id, reg_id):
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
            return_rows.append(this_row)
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
            err_msg = "Found more than 1 row in result! Row %s: %s" % (i, str(this_row))
            raise ValueError(err_msg)

    #if return_row is None:
    #    LOGGER.error('Returning none, because we expected one row but got none (for %s).' % comment)

    return return_row



if __name__ == "__main__":

    # This part is for testing the various functions, that"s why it is a bit makeshift.
    # In production, they would be called from the pygeoapi processes.
    #
    # source /home/mbuurman/work/pyg_geofresh/venv/bin/activate
    # python /home/mbuurman/work/pyg_geofresh/pygeoapi/pygeoapi/process/geofresh/py_query_db.py 9.931555 54.695070 dbpw pw
    #    where dbpw is the database passwort for postgresql, can be found in ~/.pgpass if you have access.
    #    where pw is your personal LDAP password for the ssh tunnel.


    # Data for testing:
    # These coordinates are in Vantaanjoki, reg_id = 65, basin_id = 1274183, subc_id = 553495421
    #lat = 60.7631596
    #lon = 24.8919571
    # These coordinates are in Schlei, reg_id = 58, basin_id = 1292547, subc_id = 506251252
    #lat = 54.695070
    #lon = 9.931555

    if len(sys.argv) == 2:
        dbpw = sys.argv[1]
        mbpw = None
        use_tunnel = False
        lat = 54.695070
        lon = 9.931555
    elif len(sys.argv) == 3:
        dbpw = sys.argv[1]
        mbpw = sys.argv[2]
        use_tunnel = True
        lat = 54.695070
        lon = 9.931555
    elif len(sys.argv) == 4:
        lon = float(sys.argv[1])
        lat = float(sys.argv[2])
        dbpw = sys.argv[3]
        mbpw = None
        use_tunnel = False
    elif len(sys.argv) == 5:
        lon = float(sys.argv[1])
        lat = float(sys.argv[2])
        dbpw = sys.argv[3]
        mbpw = sys.argv[4] # only when ssh tunnel is used!
        use_tunnel = True
        print('Will try to make ssh tunnel with password "%s..."' % mbpw[0:1])
    else:
        print('Please provide a point and a database password...')
        sys.exit(1)

    #print("POINT LON %s LAT %s" % (lon, lat))
    #print("DB PW %s" % dbpw)
    #print("MB PW %s" % mbpw)

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
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
    console.setFormatter(formatter)
    #logging.getLogger("").addHandler(console)
    LOGGER.addHandler(console)

    conn = get_connection_object(geofresh_server, geofresh_port,
        database_name, database_username, database_password,
        verbose=verbose, use_tunnel=use_tunnel,
        ssh_username=ssh_username, ssh_password=ssh_password)

    # Run all queries:
    print("\n(1) reg_id: ")
    reg_id = get_reg_id(conn, lon, lat)
    print("\nRESULT REG_ID: %s" % reg_id)

    print("\n(2) subc_id, basin_id: ")
    subc_id, basin_id = get_subc_id_basin_id(conn, lon, lat, reg_id)
    print("\nRESULT BASIN_ID, SUBC_ID: %s, %s" % (basin_id, subc_id))
    
    print("\n(3) upstream catchment ids: ")
    upstream_ids = get_upstream_catchment_ids_incl_itself(conn, subc_id, basin_id, reg_id)
    print("\nRESULT UPSTREAM IDS:\n%s" % upstream_ids)
    
    print("\n(4) strahler, snapped point, stream segment: ")
    strahler, snappedpoint_geojson, streamsegment_geojson = get_snapped_point_feature(
        conn, lon, lat, subc_id, basin_id=basin_id, reg_id=reg_id)
    print("\nRESULT STRAHLER: %s" % strahler)
    print("RESULT SNAPPED:\n%s" % snappedpoint_geojson)
    print("\nRESULT SEGMENT:\n%s" % streamsegment_geojson)
    
    print("\n(5) strahler, stream segment: ")
    strahler, streamsegment_linestring = get_strahler_and_stream_segment_linestring(conn, subc_id, basin_id, reg_id)
    streamsegment_feature = get_strahler_and_stream_segment_feature(conn, subc_id, basin_id, reg_id)
    print("\nRESULT STRAHLER: %s" % strahler)
    print("RESULT SEGMENT:\n%s" % streamsegment_linestring)
    print("\nRESULT SEGMENT FEATURE:\n%s" % streamsegment_feature)

    print("\n(6) upstream catchment bbox: ")
    bbox_geojson = get_upstream_catchment_bbox_polygon(conn, subc_id, upstream_ids, basin_id, reg_id)
    bbox_geojson = get_upstream_catchment_bbox_feature(
        conn, subc_id, upstream_ids, basin_id, reg_id, bla="blobb")
    print("\nRESULT BBOX\n%s" % bbox_geojson)

    print("\n(7) upstream catchment polygons: ")
    poly_collection = get_upstream_catchment_polygons_feature_coll(conn, subc_id, upstream_ids, basin_id, reg_id)
    print("\nRESULT POLYCOLL \n%s" % poly_collection)

    print("\n(8): dissolved polygon")
    dissolved_polygon = get_upstream_catchment_dissolved_feature(conn, subc_id, upstream_ids, basin_id, reg_id, bla="blub")
    print("\nRESULT DISSOLVED POLYGON: \n%s" % dissolved_polygon)

    print("\n\nClosing connection...")
    conn.close()
    print("Done")

