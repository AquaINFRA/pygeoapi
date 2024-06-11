import psycopg2
import sys
import logging
import sshtunnel
import geomet.wkt
LOGGER = logging.getLogger(__name__)

# TODO REPLACE PRINTS

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

def _get_query_snapped(lon, lat, subc_id):
    """
    SELECT seg.strahler,
    ST_AsText(ST_LineInterpolatePoint(seg.geom, ST_LineLocatePoint(seg.geom, ST_SetSRID(ST_MakePoint(9.931555, 54.695070),4326)))),
    ST_AsText(seg.geom)
    FROM stream_segments seg WHERE seg.subc_id = 506251252;

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
    FROM stream_segments seg
    WHERE seg.subc_id = {poi_subc_id}
    """.format(poi_subc_id = subc_id, longitude = lon, latitude = lat)
    query = query.replace("\n", " ")
    return query

def _get_query_segment(subc_id):
    """
    Example query:
    SELECT seg.strahler, ST_AsText(seg.geom) FROM stream_segments seg WHERE seg.subc_id = 506251252;

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
    FROM stream_segments seg
    WHERE seg.subc_id = {poi_subc_id}
    """.format(poi_subc_id = subc_id,)
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

def _get_query_upstream_dissolved(upstream_ids):
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
    """.format(ids = ids)
    return query

def _get_query_upstream_linestrings(upstream_ids):
    '''
    Example query:
    SELECT  seg.subc_id, seg.strahler, ST_AsText(seg.geom)
    FROM stream_segments seg WHERE seg.subc_id IN (506250459, 506251015, 506251126, 506251712);
    '''
    ids = ", ".join([str(elem) for elem in upstream_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712

    query = '''
    SELECT 
    seg.subc_id, seg.strahler, ST_AsText(seg.geom)
    FROM stream_segments seg
    WHERE seg.subc_id IN ({ids})
    '''.format(ids = ids)
    query = query.replace("\n", " ")
    return query

def _get_query_upstream_polygons(upstream_ids):
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

    # TODO ASK: Is this faster?
    # SELECT ST_AsText(geom) FROM sub_catchments WHERE reg_id = 58 AND subc_id IN (506250459, 506251015, 506251126, 506251712);

    """

    ids = ", ".join([str(elem) for elem in upstream_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712

    query = '''
    SELECT subc_id, ST_AsText(geom)
    FROM sub_catchments
    WHERE subc_id IN ({ids})
    '''.format(ids = ids)
    return query

def _get_query_upstream_bbox(upstream_ids):
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

    TODO ASK: Is this faster?
    SELECT ST_AsText(geom) FROM sub_catchments WHERE reg_id = 58 AND subc_id IN (506250459, 506251015, 506251126, 506251712);

    """

    relevant_ids = ", ".join([str(elem) for elem in upstream_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712

    query = """
    SELECT ST_AsText(ST_Extent(geom))
    FROM sub_catchments
    WHERE subc_id IN ({relevant_ids})
    """.format(relevant_ids = relevant_ids)
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
###################################


def get_reg_id(conn, lon, lat):
    print("Getting region id for points: lon=%s, lat=%s" % (lon, lat))
    query = _get_query_reg_id(lon, lat)
    result_row = get_only_row(execute_query(conn, query))
    reg_id = result_row[0]
    print("Getting region id for points: lon=%s, lat=%s: %s" % (lon, lat, reg_id))
    return reg_id


def get_subc_id_basin_id(conn, lon, lat, reg_id):
    print("Getting basin id and subcatchment id for points: lon=%s, lat=%s (reg_id %s)" % (lon, lat, reg_id))
    query = _get_query_subc_id_basin_id(lon, lat, reg_id)
    result_row = get_only_row(execute_query(conn, query))
    subc_id = result_row[0]
    basin_id = result_row[1]
    print("Getting basin id and subcatchment id for points: lon=%s, lat=%s (reg_id %s, basin_is %s): %s" % (lon, lat, reg_id, basin_id, subc_id))
    return subc_id, basin_id 


def get_upstream_catchment_bbox_polygon(conn, subc_id, upstream_ids):
    """
    Example result:
    {"type": "Polygon", "coordinates": [[[9.913333333333334, 54.68833333333333], [9.913333333333334, 54.70583333333333], [9.931666666666667, 54.70583333333333], [9.931666666666667, 54.68833333333333], [9.913333333333334, 54.68833333333333]]]}
    """
    print("Getting upstream catchment bbox for subc_id: %s" % subc_id)
    query = _get_query_upstream_bbox(upstream_ids)
    result_row = get_only_row(execute_query(conn, query))
    bbox_wkt = result_row[0]
    bbox_geojson = geomet.wkt.loads(bbox_wkt)
    return bbox_geojson


def get_upstream_catchment_bbox_feature(conn, subc_id, upstream_ids, **kwargs):
    bbox_geojson = get_upstream_catchment_bbox_polygon(conn, subc_id, upstream_ids)
    feature = {
        "type": "Feature",
        "geometry": bbox_geojson,
        "properties": {
            "description": "Bounding box of the upstream catchment of subcatchment %s" % subc_id,
            "upstream_subc_ids": upstream_ids,
            "downstream_subc_id": subc_id,
        }
    }

    if len(kwargs) > 0:
        feature["properties"].update(kwargs)

    return feature

def get_upstream_catchment_dissolved_feature_coll(conn, subc_id, upstream_ids, lonlat=None, **kwargs):
    feature = get_upstream_catchment_dissolved_feature(conn, subc_id, upstream_ids, **kwargs)
    point = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [lonlat[0], lonlat[1]]
        },
        "properties": kwargs
    }
    feature_coll = {
        "type": "FeatureCollection",
        "features": [feature, point]
    }
    return feature_coll

def get_upstream_catchment_dissolved_feature(conn, subc_id, upstream_ids, **kwargs):
    geometry_polygon = get_upstream_catchment_dissolved_geometry(conn, subc_id, upstream_ids)
    feature = {
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
        feature["properties"].update(kwargs)

    return feature

def get_upstream_catchment_dissolved_geometry(conn, subc_id, upstream_ids):
    """
    Example result:
    {"type": "Polygon", "coordinates": [[[9.916666666666668, 54.7025], [9.913333333333334, 54.7025], [9.913333333333334, 54.705], [9.915000000000001, 54.705], [9.915833333333333, 54.705], [9.915833333333333, 54.70583333333333], [9.916666666666668, 54.70583333333333], [9.916666666666668, 54.705], [9.918333333333335, 54.705], [9.918333333333335, 54.704166666666666], [9.919166666666667, 54.704166666666666], [9.919166666666667, 54.70333333333333], [9.920833333333334, 54.70333333333333], [9.920833333333334, 54.704166666666666], [9.924166666666668, 54.704166666666666], [9.925, 54.704166666666666], [9.925, 54.705], [9.926666666666668, 54.705], [9.9275, 54.705], [9.9275, 54.70583333333333], [9.928333333333335, 54.70583333333333], [9.928333333333335, 54.70333333333333], [9.929166666666667, 54.70333333333333], [9.929166666666667, 54.7025], [9.931666666666667, 54.7025], [9.931666666666667, 54.7], [9.930833333333334, 54.7], [9.930833333333334, 54.69833333333333], [9.930000000000001, 54.69833333333333], [9.929166666666667, 54.69833333333333], [9.929166666666667, 54.6975], [9.929166666666667, 54.696666666666665], [9.928333333333335, 54.696666666666665], [9.928333333333335, 54.695], [9.9275, 54.695], [9.9275, 54.693333333333335], [9.928333333333335, 54.693333333333335], [9.928333333333335, 54.69166666666666], [9.9275, 54.69166666666666], [9.9275, 54.69083333333333], [9.926666666666668, 54.69083333333333], [9.926666666666668, 54.69], [9.925833333333333, 54.69], [9.925, 54.69], [9.925, 54.68833333333333], [9.922500000000001, 54.68833333333333], [9.922500000000001, 54.69083333333333], [9.921666666666667, 54.69083333333333], [9.921666666666667, 54.69166666666666], [9.919166666666667, 54.69166666666666], [9.919166666666667, 54.692499999999995], [9.918333333333335, 54.692499999999995], [9.918333333333335, 54.693333333333335], [9.9175, 54.693333333333335], [9.9175, 54.695], [9.918333333333335, 54.695], [9.918333333333335, 54.69833333333333], [9.9175, 54.69833333333333], [9.9175, 54.700833333333335], [9.9175, 54.70166666666667], [9.916666666666668, 54.70166666666667], [9.916666666666668, 54.7025]]]}
    """
    print("Getting upstream catchment geometry (dissolved) for subc_id: %s" % subc_id)
    query = _get_query_upstream_dissolved(upstream_ids)
    result_row = get_only_row(execute_query(conn, query))
    dissolved_wkt = result_row[0]
    dissolved_geojson = geomet.wkt.loads(dissolved_wkt)
    return dissolved_geojson

def get_upstream_catchment_linestrings_feature_coll(conn, subc_id, upstream_ids, basin_id, reg_id):
    print("Getting upstream catchment linestrings for subc_id: %s" % subc_id)
    query = _get_query_upstream_linestrings(upstream_ids)
    num_rows = len(upstream_ids)
    result_rows = get_rows(execute_query(conn, query), num_rows)

    features_geojson = []
    for row in result_rows:
        feature = {
            "type": "Feature",
            "geometry": geomet.wkt.loads(row[2]),
            "properties": {
                "subcatchment_id": row[0],
                "strahler_order": row[1],
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

    return feature_coll


def get_upstream_catchment_polygons_feature_coll(conn, subc_id, upstream_ids, basin_id, reg_id):
    print("Getting upstream catchment geometries for subc_id: %s" % subc_id)
    query = _get_query_upstream_polygons(upstream_ids)
    num_rows = len(upstream_ids)
    result_rows = get_rows(execute_query(conn, query), num_rows)

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

    return feature_coll
    
    #polygons_geojson = []
    #for row in result_rows:
    #    polygons_geojson.append(geomet.wkt.loads(row[1]))
    #geometry_coll = {
    #     "type": "GeometryCollection",
    #     "geometries": polygons_geojson
    #}
    #return geometry_coll


def get_upstream_catchment_ids(conn, subc_id, reg_id, basin_id):
    print("Getting upstream catchment for subc_id: %s" % subc_id)
    query = _get_query_upstream(subc_id, reg_id, basin_id)
    result_row = get_only_row(execute_query(conn, query))
    subc_id_returned = result_row[0]
    upstream_catchment_subcids = result_row[1]
    #upstream_catchment_subcids = _get_result_upstream_catchment(cursor, subc_id)
    
    # superfluous warning:
    if not subc_id == subc_id_returned:
        msg = "WARNING: Wrong subc_id!"
        LOGGER.error(msg)
        raise ValueError(msg)

    # remove itself
    if subc_id_returned in upstream_catchment_subcids:
        upstream_catchment_subcids.remove(subc_id_returned)
    else:
        msg = "WARNING: Subcatchment id is not in the list of upstream catchment ids, so we cannot remove it."
        LOGGER.error(msg)
        raise ValueError(msg)

    print("Getting upstream catchment for subc_id: %s: %s" % (subc_id, upstream_catchment_subcids))
    return upstream_catchment_subcids


def get_snapped_point_feature(conn, lon, lat, subc_id, basin_id, reg_id):
    """
    Example result:
    2, {"type": "Point", "coordinates": [9.931555, 54.69625]}, {"type": "LineString", "coordinates": [[9.929583333333333, 54.69708333333333], [9.930416666666668, 54.69625], [9.932083333333335, 54.69625], [9.933750000000002, 54.694583333333334], [9.934583333333334, 54.694583333333334]]}

    """
    print("Getting snapped point for points: lon=%s, lat=%s (subc_id %s)" % (lon, lat, subc_id))
    query = _get_query_snapped(lon, lat, subc_id)
    result_row = get_only_row(execute_query(conn, query))
    
    strahler = result_row[0]
    snappedpoint_wkt = result_row[1]
    streamsegment_wkt = result_row[2]
    
    snappedpoint_geojson = geomet.wkt.loads(snappedpoint_wkt)
    streamsegment_geojson = geomet.wkt.loads(streamsegment_wkt)
    print("Getting snapped point for points: lon=%s, lat=%s (subc_id %s): %s" % (lon, lat, subc_id, snappedpoint_geojson))
    print("Getting stream segment for points: lon=%s, lat=%s (subc_id %s): %s" % (lon, lat, subc_id, streamsegment_geojson))

    #lon_snap = snappedpoint_geojson["coordinates"][0]
    #lat_snap = snappedpoint_geojson["coordinates"][1]

    snappedpoint_feature = {
        "type": "Feature",
        "geometry": snappedpoint_geojson,
        "properties": {
            "subcatchment_id": subc_id,
            "lon_original": lon,
            "lat_original": lat,
            "basin_id": basin_id,
            "reg_id": reg_id
        }
    }

    streamsegment_feature = {
        "type": "Feature",
        "geometry": streamsegment_geojson,
        "properties": {
            "subcatchment_id": subc_id,
            "strahler_order": strahler,
            "basin_id": basin_id,
            "reg_id": reg_id
        }
    }

    #return strahler, snappedpoint_geojson, streamsegment_geojson
    return strahler, snappedpoint_feature, streamsegment_feature


def get_strahler_and_stream_segment_feature(conn, subc_id, basin_id, reg_id):
    strahler, geojson_linestring = get_strahler_and_stream_segment(conn, subc_id)
    feature = {
        "type": "Feature",
        "geometry": geojson_linestring,
        "properties": {
            "subcatchment_id": subc_id,
            "strahler_order": strahler,
            "basin_id": basin_id,
            "reg_id": reg_id
        }
    }

    return strahler, feature


def get_strahler_and_stream_segment(conn, subc_id):
    # TODO Make one query for various subc_ids! When would this be needed?
    """

    Stream segment is returned as a single LineString.

    Example result:
    2, {"type": "LineString", "coordinates": [[9.929583333333333, 54.69708333333333], [9.930416666666668, 54.69625], [9.932083333333335, 54.69625], [9.933750000000002, 54.694583333333334], [9.934583333333334, 54.694583333333334]]}
    """
    print("Getting strahler and stream segment for subc_id %s)" %  subc_id)
    query = _get_query_segment(subc_id)
    result_row = get_only_row(execute_query(conn, query))
    strahler = result_row[0]
    streamsegment_wkt = result_row[1]
    streamsegment_geojson = geomet.wkt.loads(streamsegment_wkt)
    print("Getting strahler order and stream segment for subc_id %s: %s, %s" % (subc_id, strahler, streamsegment_geojson))
    return strahler, streamsegment_geojson

    

###########################
### database connection ###
###########################


def open_ssh_tunnel(ssh_host, ssh_username, ssh_password, remote_host, remote_port, verbose=False):
    """Open an SSH tunnel and connect using a username and password.
    
    :param verbose: Set to True to show logging
    :return tunnel: Global SSH tunnel connection
    """
    print("Opening SSH tunnel...")
    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
    
    #global tunnel
    tunnel = sshtunnel.SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username = ssh_username,
        ssh_password = ssh_password,
        remote_bind_address=(remote_host, remote_port)
    )
    print("Starting SSH tunnel...")
    tunnel.start()
    print("Starting SSH tunnel... done.")
    return tunnel


def connect_to_db(geofresh_server, db_port, database_name, database_username, database_password):
    # This blocks! Cannot run KeyboardInterrupt
    print("Connecting to db...")
    conn = psycopg2.connect(
       database=database_name,
       user=database_username,
       password=database_password,
       host=geofresh_server,
       port= str(db_port)
    )
    print("Connecting to db... done.")
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
    print("Executing query...")
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor


def get_rows(cursor, num_rows):
    i = 0
    return_rows = []
    while True:
        i += 1
        #print("Fetching row %s..." % i)
        this_row = cursor.fetchone();
        if this_row is None:
            break
        elif i <= num_rows:
            return_rows.append(this_row)
        else:
            LOGGER.warning("Found more than %s rows in result! Row %s: %s" % (num_rows, i, this_row))
            print("WARNING: More than one row output! Will ignore row %s..." % i)

    return return_rows


def get_only_row(cursor):
    i = 0
    return_row = None
    while True:
        i += 1
        print("Fetching row %s..." % i)
        this_row = cursor.fetchone()
        if this_row is None:
            break
        elif i == 1:
            return_row = this_row
            LOGGER.info("Row %s: %s" % (i, this_row))
        else:
            # We are asking for one point, so the result should be just one row!
            LOGGER.warning("Found more than 1 row in result! Row %s: %s" % (i, this_row))
            print("WARNING: More than one row output! Will ignore row %s..." % i)

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
    print(reg_id)

    print("\n(2) subc_id, basin_id: ")
    subc_id, basin_id = get_subc_id_basin_id(conn, lon, lat, reg_id)
    print(basin_id, subc_id)
    
    print("\n(3) upstream catchment ids: ")
    upstream_ids = get_upstream_catchment_ids(conn, subc_id, reg_id, basin_id)
    print(upstream_ids)
    
    print("\n(4) strahler, snapped point, stream segment: ")
    strahler, snappedpoint_geojson, streamsegment_geojson = get_snapped_point_feature(
        conn, lon, lat, subc_id, basin_id=basin_id, reg_id=reg_id)
    print(strahler)
    print(snappedpoint_geojson)
    print(streamsegment_geojson)
    
    print("\n(5) strahler, stream segment: ")
    strahler, streamsegment_geojson = get_strahler_and_stream_segment_feature(conn, subc_id, basin_id, reg_id)
    print(strahler)
    print(streamsegment_geojson)

    print("\n(6a) upstream catchment bbox as geometry: ")
    bbox_geojson = get_upstream_catchment_bbox_polygon(conn, subc_id, upstream_ids)
    print("BBOX\n%s" % bbox_geojson)

    print("\n(6b) upstream catchment bbox as feature: ")
    bbox_geojson = get_upstream_catchment_bbox_feature(
        conn, subc_id, upstream_ids, basin_id=basin_id, reg_id=reg_id)
    print("BBOX\n%s" % bbox_geojson)

    print("\n(7) upstream catchment polygons: ")
    poly_collection = get_upstream_catchment_polygons_feature_coll(conn, subc_id, upstream_ids)
    print("POLYCOLL \n%s" % poly_collection)

    print("\n(8a): dissolved polygon as geometry/polygon")
    dissolved_polygon = get_upstream_catchment_dissolved_geometry(conn, subc_id, upstream_ids)
    print("DISSOLVED POLYGON: \n%s" % dissolved_polygon)

    print("\n(8b): dissolved polygon as feature")
    dissolved_feature = get_upstream_catchment_dissolved_feature(conn, subc_id, upstream_ids)
    print("DISSOLVED FEATURE: \n%s" % dissolved_feature)

    print("\n(8c): dissolved polygon as feature coll")
    dissolved_feature_coll = get_upstream_catchment_dissolved_feature_coll(conn, subc_id, upstream_ids, lonlat=None, basin_id=basin_id, reg_id=reg_id)
    print("DISSOLVED FEATURE COLL: \n%s" % dissolved_feature_coll)

    print("Closing connection...")
    conn.close()
    print("Done")

