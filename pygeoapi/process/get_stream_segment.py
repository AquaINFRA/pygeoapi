
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import psycopg2
import pygeoapi.process.upstream_helpers as helpers
from pygeoapi.process.geofresh.py_query_db import get_connection_object
from pygeoapi.process.geofresh.py_query_db import get_strahler_and_stream_segment_feature
from pygeoapi.process.geofresh.py_query_db import get_strahler_and_stream_segment_linestring
from pygeoapi.process.geofresh.py_query_db import get_polygon_for_subcid_feature
from pygeoapi.process.geofresh.py_query_db import get_polygon_for_subcid_simple


'''

# Normal request, returning a simple geometry (linestring):
curl -X POST "https://aqua.igb-berlin.de/pygeoapi/processes/get-stream-segment/execution" -H "Content-Type: application/json" -d "{\"inputs\":{ \"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Schlei\"}}"

# Request returning a FeatureCollection and additionally the subcatchment polygon:
curl -X POST "https://aqua.igb-berlin.de/pygeoapi/processes/get-stream-segment/execution" -H "Content-Type: application/json" -d "{\"inputs\":{ \"lon\": 9.931555, \"lat\": 54.695070, \"add_subcatchment\": true, \"get_type\": \"FeatureCollection\"}}"

# Asking for a subcatchment polygon, but it cannot be fulfilled due to geometry compatibility:
curl -X POST "https://aqua.igb-berlin.de/pygeoapi/processes/get-stream-segment/execution" -H "Content-Type: application/json" -d "{\"inputs\":{ \"lon\": 9.931555, \"lat\": 54.695070, \"add_subcatchment\": true}}"
'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class StreamSegmentGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None


    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<StreamSegmentGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the stream segment..."')
        LOGGER.info('Inputs: %s' % data)
        LOGGER.info('Requested outputs: %s' % outputs)

        try:
            conn = self.get_db_connection()
            res = self._execute(data, outputs, conn)

            LOGGER.debug('Closing connection...')
            conn.close()
            LOGGER.debug('Closing connection... Done.')

            return res

        except psycopg2.Error as e3:
            conn.close()
            err = f"{type(e3).__module__.removesuffix('.errors')}:{type(e3).__name__}: {str(e3).rstrip()}"
            error_message = 'Database error: %s (%s)' % (err, str(e3))
            LOGGER.error(error_message)
            raise ProcessorExecuteError(user_msg = error_message)

        except Exception as e:
            conn.close()
            LOGGER.error('During process execution, this happened: %s' % e)
            print(traceback.format_exc())
            raise ProcessorExecuteError(e) # TODO: Can we feed e into ProcessExecuteError?


    def _execute(self, data, requested_outputs, conn):

        # TODO: Must change behaviour based on content of requested_outputs
        LOGGER.debug('Content of requested_outputs: %s' % requested_outputs)

        ## User inputs
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        get_type = data.get('get_type', 'LineString')
        add_subcatchment = data.get('add_subcatchment', False)
        if not isinstance(add_subcatchment, bool):
            LOGGER.error('Expected a boolean for "add_subcatchment"!')

        LOGGER.info('Retrieving stream segment for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))
        subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, subc_id)
        
        LOGGER.debug('Now, getting stream segment (incl. strahler order) for subc_id: %s' % subc_id)

        if get_type.lower() == 'feature':
            feature_streamsegment = get_strahler_and_stream_segment_feature(
                conn, subc_id, basin_id, reg_id)
            geojson_object = feature_streamsegment

            if add_subcatchment:
                LOGGER.info('User also requested subcatchment, but that is not compatible with returning a single feature.')
                geojson_object['note']: 'Cannot add subcatchment polygon to GeoJSON Feature.'


        elif get_type.lower() == 'linestring':
            strahler, streamsegment_simple_geometry = get_strahler_and_stream_segment_linestring(
                conn, subc_id, basin_id, reg_id)
            geojson_object = streamsegment_simple_geometry

            if add_subcatchment:
                LOGGER.info('User also requested subcatchment, but that is not compatible with returning a simple linestring.')
                geojson_object['note'] = 'Cannot add subcatchment polygon to GeoJSON linestring.'


        elif get_type.lower() == 'featurecollection':
            feature_streamsegment = get_strahler_and_stream_segment_feature(
                conn, subc_id, basin_id, reg_id)

            geojson_object = {
                "type": "FeatureCollection",
                "features": [feature_streamsegment]
            }

            # In some cases, we also want to add the subcatchment polygon!
            # (This is faster than querying the service twice).
            if add_subcatchment:
                feature_subcatchment = get_polygon_for_subcid_feature(conn, subc_id, basin_id, reg_id)
                geojson_object["features"].append(feature_subcatchment)


        elif get_type.lower() == 'geometrycollection':
            strahler, streamsegment_simple_geometry = get_strahler_and_stream_segment_linestring(
                conn, subc_id, basin_id, reg_id)

            geojson_object = {
                 "type": "GeometryCollection",
                 "geometries": [streamsegment_simple_geometry]
            }

            # In some cases, we also want to add the subcatchment polygon!
            # (This is faster than querying the service twice).
            if add_subcatchment:
                polygon_subcatchment = get_polygon_for_subcid_simple(conn, subc_id, basin_id, reg_id)
                geojson_object["geometries"].append(polygon_subcatchment)

        else:
            err_msg = "Input parameter 'get_type' can only be one of LineString, Feature, FeatureCollection and GeometryCollection!"
            # TODO: API definition: What is better: Feature vs SimpleGeometry, or Feature vs LineString / Point / Polygon / ... ?
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)


        ################
        ### Results: ###
        ################

        # TODO API definition: Comments as part of properties (for features), and then for geometries just adding it??
        if comment is not None:
            geojson_object['comment'] = comment

        return 'application/json', geojson_object

    def get_db_connection(self):

        with open('pygeoapi/config.json') as myfile:
            # TODO possibly read path to config from some env var, like for daugava?
            config = json.load(myfile)

        geofresh_server = config['geofresh_server']
        geofresh_port = config['geofresh_port']
        database_name = config['database_name']
        database_username = config['database_username']
        database_password = config['database_password']
        use_tunnel = config.get('use_tunnel')
        ssh_username = config.get('ssh_username')
        ssh_password = config.get('ssh_password')
        localhost = config.get('localhost')

        try:
            conn = get_connection_object(geofresh_server, geofresh_port,
                database_name, database_username, database_password,
                use_tunnel=use_tunnel, ssh_username=ssh_username, ssh_password=ssh_password)
        except sshtunnel.BaseSSHTunnelForwarderError as e1:
            LOGGER.error('SSH Tunnel Error: %s' % str(e1))
            raise e1

        return conn

