
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
from pygeoapi.process.geofresh.py_query_db import get_dijkstra_linestrings_geometry_coll
from pygeoapi.process.geofresh.py_query_db import get_dijkstra_linestrings_feature_coll


'''

curl -X POST "https://aqua.igb-berlin.de/pygeoapi-dev/processes/get-shortest-path/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lon_start\": 9.937520027160646, \"lat_start\": 54.69422745526058, \"lon_end\": 9.9217, \"lat_end\": 54.6917, \"comment\":\"Test\"}}"

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class DijkstraShortestPathGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None


    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<DijkstraShortestPathGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the dijkstra shortest path..."')
        try:
            return self._execute(data, outputs)
        except Exception as e:
            LOGGER.error(e)
            print(traceback.format_exc())
            raise ProcessorExecuteError(e, user_msg=e.message)

    def _execute(self, data, requested_outputs):

        # TODO: Must change behaviour based on content of requested_outputs
        LOGGER.debug('Content of requested_outputs: %s' % requested_outputs)

        ## User inputs
        lon_start = data.get('lon_start', None)
        lat_start = data.get('lat_start', None)
        subc_id_start = data.get('subc_id_start', None) # optional, need either lonlat OR subc_id
        lon_end = data.get('lon_end', None)
        lat_end = data.get('lat_end', None)
        subc_id_start = data.get('subc_id_end', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        get_type = data.get('get_type', 'GeometryCollection') # or FeatureCollection

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
            # Overall goal: Get the dijkstra shortest path (as linestrings)!
            LOGGER.info('START: Getting dijkstra shortest path for lon %s, lat %s (or subc_id %s) to lon %s, lat %s (or subc_id %s)' % (
                lon_start, lat_start, subc_id_start, lon_end, lat_end, subc_id_end))

            # Get reg_id, basin_id, subc_id
            subc_id1, basin_id1, reg_id1 = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon_start, lat_start, subc_id_start)
            subc_id2, basin_id2, reg_id2 = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon_end, lat_end, subc_id_end)

            # Check if same region and basin?
            # TODO: Can we route via the sea then??
            if not reg_id1 == reg_id2:
                err_msg = 'Start and end are in different regions (%s and %s) - this cannot work.' % (reg_id1, reg_id2)
                LOGGER.warning(err_msg)
                raise ProcessorExecuteError(user_msg=err_msg)

            if not basin_id1 == basin_id2:
                err_msg = 'Start and end are in different basins (%s and %s) - this cannot work.' % (basin_id1, basin_id2)
                LOGGER.warning(err_msg)
                raise ProcessorExecuteError(user_msg=err_msg)

            # Get subc_ids of the whole connection...
            LOGGER.debug('Getting network connection for subc_id: start = %s, end = %s' % (subc_id1, subc_id2))
            if get_type.lower() == 'geometrycollection':
                geojson_object = get_dijkstra_linestrings_geometry_coll(conn, subc_id1, subc_id2, reg_id1, basin_id1)
            elif get_type.lower() == 'featurecollection':
                geojson_object = get_dijkstra_linestrings_feature_coll(conn, subc_id1, subc_id2, reg_id1, basin_id1)
            else:
                err_msg = "Input parameter 'get_type' can only be one of GeometryCollection and FeatureCollection!"
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
            error_message = 'Database error.'
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
                'error_message': 'getting dijkstra stream segments failed.',
                'details': error_message}

            if comment is not None:
                output['comment'] = comment

            LOGGER.warning('Getting dijkstra stream segments failed. Returning error message.')
            return 'application/json', output

