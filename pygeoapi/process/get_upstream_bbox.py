
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
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_bbox_feature
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_bbox_polygon
from pygeoapi.process.geofresh.py_query_db import get_strahler_and_stream_segment_feature





'''
curl -X POST "http://localhost:5000/processes/get-upstream-bbox/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Nordoestliche Schlei, bei Rabenholz\"}}"

Mitten in der ELbe:
53.537158298376575, 9.99475350366553
curl -o tmp.tmp -X POST "http://localhost:5000/processes/get-upstream-bbox/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lon\": 9.994753, \"lat\": 53.537158, \"comment\":\"Nordoestliche Schlei, bei Rabenholz\"}}"
curl --insecure 195.148.30.163/pygeoapi/ -u aqua:infra
curl -X POST --insecure 195.148.30.163/pygeoapi/processes/get-upstream-bbox/execution -u aqua:infra -H "Content-Type: application/json" -d "{\"inputs\":{\"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Nordoestliche Schlei, bei Rabenholz\"}}"


'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class UpstreamBboxGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.job_id = None


    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<UpstreamBboxGetter> {self.name}'


    def execute(self, data):
        LOGGER.info('Starting to get the upstream bounding box..."')
        LOGGER.info('Inputs: %s' % data)
        try:
            return self._execute(data)
        except Exception as e:
            LOGGER.error(e)
            print(traceback.format_exc())
            raise ProcessorExecuteError(e)

    def _execute(self, data):

        ## User inputs
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        get_type = data.get('get_type', 'Polygon')

        with open('pygeoapi/config.json') as myfile:
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

        error_message = None

        try:
            conn = get_connection_object(geofresh_server, geofresh_port,
                database_name, database_username, database_password,
                use_tunnel=use_tunnel, ssh_username=ssh_username, ssh_password=ssh_password)
        except sshtunnel.BaseSSHTunnelForwarderError as e1:
            error_message = str(e1)

        try:
            # Overall goal: Get the upstream stream segments!
            LOGGER.info('START: Getting upstream bbox for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))

            # Get reg_id, basin_id, subc_id, upstream_catchment_subcids
            subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, subc_id)
            upstream_catchment_subcids = helpers.get_upstream_catchment_ids(conn, subc_id, basin_id, reg_id, LOGGER)

            # Get geometry (two types)
            LOGGER.debug('...Getting upstream catchment bbox for subc_id: %s' % subc_id)
            if get_type.lower() == 'polygon':
                geojson_object = get_upstream_catchment_bbox_polygon(
                    conn, subc_id, upstream_catchment_subcids, basin_id, reg_id)
                LOGGER.debug('END: Received simple polygon: %s' % str(geojson_object)[0:50])

            elif get_type.lower() == 'feature':
                geojson_object = get_upstream_catchment_bbox_feature(
                    conn, subc_id, upstream_catchment_subcids,
                    basin_id=basin_id, reg_id=reg_id, comment=comment)
                LOGGER.debug('END: Received feature: %s' % str(geojson_object)[0:50])

            else:
                err_msg = "Input parameter 'get_type' can only be one of Polygon or Feature!"
                LOGGER.error(err_msg)
                raise ProcessorExecuteError(user_msg=err_msg)

        # TODO move this to execute! and the database stuff!
        except ValueError as e2:
            error_message = str(e2)
            conn.close()
            raise ValueError(e2)

        except psycopg2.Error as e3:
            err = f"{type(e3).__module__.removesuffix('.errors')}:{type(e3).__name__}: {str(e3).rstrip()}"
            LOGGER.error(err)
            error_message = str(e3)
            error_message = str(err)
            error_message = 'Database error. '
            #if conn: conn.rollback()


        LOGGER.debug('Closing connection...')
        conn.close()
        LOGGER.debug('Closing connection... Done.')


        ################
        ### Results: ###
        ################

        if error_message is None:

            if comment is not None:
                geojson_object['comment'] = comment

            return 'application/json', geojson_object

        else:
            output = {
                'error_message': 'getting upstream bbox failed.',
                'details': error_message}

            if comment is not None:
                output['comment'] = comment

            LOGGER.warning('Getting upstream bbox failed. Returning error message.')
            return 'application/json', output

