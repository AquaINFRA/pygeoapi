
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
from pygeoapi.process.geofresh.py_query_db import get_polygon_for_subcid_feature 
from pygeoapi.process.geofresh.py_query_db import get_strahler_and_stream_segment_feature


'''
Note:
TODO FIXME:
This should be replaced by using the normal get_stream_segment.py with parameter add_subcatchment,
but then I need to change my test HTML client, which currently only can make different process calls
by using different process id, and not by adding parameters.

curl -X POST "http://localhost:5000/processes/get-stream-segment/execution" -H "Content-Type: application/json" -d "{\"inputs\":{ \"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Nordoestliche Schlei, bei Rabenholz\"}}"

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class StreamSegmentGetterPlus(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def __repr__(self):
        return f'<StreamSegmentGetter> {self.name}'


    def execute(self, data):
        LOGGER.info('Starting to get the stream segment..."')
        try:
            return self._execute(data)
        except Exception as e:
            LOGGER.error(e)
            print(traceback.format_exc())
            raise ProcessorExecuteError(e)

    def _execute(self, data):

        ### USER INPUTS
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional

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
            LOGGER.info('Getting stream segment and subcatchment for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))
            subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, subc_id)
            LOGGER.debug('... Now, getting strahler and stream segment for subc_id: %s' % subc_id)
            feature_streamsegment = get_strahler_and_stream_segment_feature(conn, subc_id, basin_id, reg_id)
            LOGGER.debug('... Now, getting subcatchment polygon for subc_id: %s' % subc_id)
            feature_subcatchment = get_polygon_for_subcid_feature(conn, subc_id, basin_id, reg_id)
            LOGGER.info('Received two features I think...') # TODO HOW TO CHECK VALIDITY OF RESULT?

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

        print('Closing connection...')
        conn.close()
        print('Done')


        ################
        ### Results: ###
        ################

        if error_message is None:
          
            if comment is not None:
                feature_streamsegment['properties']['comment'] = comment
                feature_subcatchment['properties']['comment'] = comment
            
            outputs = {
                    "type": "FeatureCollection",
                    "features": [feature_streamsegment, feature_subcatchment]
            }
            
            return 'application/json', outputs

        else:
            outputs = {
                'error_message': 'getting stream segment failed.',
                'details': error_message}
            if comment is not None:
                outputs['comment'] = comment
            LOGGER.warning('Getting stream segment failed. Returning error message.')
            return 'application/json', outputs

