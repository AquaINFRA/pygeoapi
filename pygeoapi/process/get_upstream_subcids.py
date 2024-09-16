
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import pygeoapi.process.upstream_helpers as helpers
from pygeoapi.process.geofresh.py_query_db import get_connection_object
import psycopg2

'''

curl -X POST "http://localhost:5000/processes/get-upstream-catchment-ids/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Nordoestliche Schlei, bei Rabenholz\"}}"



# OLD:

curl -X POST "http://localhost:5000/processes/get-upstream-catchment-ids/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"subc_id\":553495421, \"basin_id\":1274183, \"reg_id\": 65, \"comment\":\"strahler 2, two headwaters directly upstream, nothing else.\"}}"

curl -X POST "http://localhost:5000/processes/get-upstream-catchment-ids/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"subc_id\":553489656, \"basin_id\":1274183, \"reg_id\": 65, \"comment\":\"headwater.\"}}"

curl -X POST "http://localhost:5000/processes/get-upstream-catchment-ids/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"subc_id\":553494913, \"basin_id\":1274183, \"reg_id\": 65, \"comment\":\"not a headwater, strahler 2, but has a headwater directly upstream.\"}}"

curl -X POST "http://localhost:5000/processes/get-upstream-catchment-ids/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"subc_id\":553493913, \"basin_id\":1274183, \"reg_id\": 65, \"comment\":\"not a headwater, strahler 3, no headwater directly upstream.\"}}"


'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class UpstreamCatchmentIdGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None


    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<UpstreamCatchmentIdGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the upstream subcatchment ids..."')
        try:
            return self._execute(data, outputs)
        except Exception as e:
            LOGGER.error(e)
            print(traceback.format_exc())
            raise ProcessorExecuteError(e)

    def _execute(self, data, requested_outputs):

        # TODO: Must change behaviour based on content of requested_outputs
        LOGGER.debug('Content of requested_outputs: %s' % requested_outputs)

        ## User inputs
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
            # Overall goal: Get the upstream subc_ids!
            LOGGER.info('START: Getting upstream subc_ids for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))

            # Get reg_id, basin_id, subc_id, upstream_catchment_subcids
            subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, subc_id)

            upstream_catchment_subcids = helpers.get_upstream_catchment_ids(conn, subc_id, basin_id, reg_id, LOGGER)
            LOGGER.debug('END: Received ids : %s' % upstream_catchment_subcids)

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

            # Note: This is not GeoJSON (on purpose), as we did not look for geometry yet.
            output = {
                'subcatchment': subc_id,
                'upstream_catchment_ids': upstream_catchment_subcids,
                'region_id': reg_id,
                'basin_id': basin_id
            }

            if comment is not None:
                output['comment'] = comment

            return 'application/json', output

        else:
            output = {
                'error_message': 'getting upstream catchment ids failed.',
                'details': error_message}

            if comment is not None:
                output['comment'] = comment

            LOGGER.warning('Getting upstream catchment ids failed. Returning error message.')
            return 'application/json', output

