
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
from pygeoapi.process.geofresh.py_query_db import get_polygon_for_subcid_simple
from pygeoapi.process.geofresh.py_query_db import get_strahler_and_stream_segment_linestring


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

        # User inputs
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional

        LOGGER.info('Getting stream segment and subcatchment for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))
        subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, subc_id)

        LOGGER.debug('... Now, getting strahler and stream segment for subc_id: %s' % subc_id)
        strahler, streamsegment_simple_geometry = get_strahler_and_stream_segment_linestring(
            conn, subc_id, basin_id, reg_id)
        feature_streamsegment = {
                "type": "Feature",
                "geometry": streamsegment_simple_geometry,
                "properties": {
                    "subcatchment_id": subc_id,
                    "strahler_order": strahler,
                    "basin_id": basin_id,
                    "reg_id": reg_id
                }
            }

        LOGGER.debug('... Now, getting subcatchment polygon for subc_id: %s' % subc_id)
        subcatchment_simple = get_polygon_for_subcid_simple(conn, subc_id, basin_id, reg_id)
        feature_subcatchment = {
            "type": "Feature",
            "geometry": subcatchment_simple,
            "properties": {
                "subcatchment_id": subc_id,
                "basin_id": basin_id,
                "reg_id": reg_id
            }
        }

        LOGGER.info('Received two features I think...') # TODO HOW TO CHECK VALIDITY OF RESULT?

        ################
        ### Results: ###
        ################
      
        if comment is not None:
            feature_streamsegment['properties']['comment'] = comment
            feature_subcatchment['properties']['comment'] = comment
        
        outputs = {
                "type": "FeatureCollection",
                "features": [feature_streamsegment, feature_subcatchment]
        }
        
        return 'application/json', outputs



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
