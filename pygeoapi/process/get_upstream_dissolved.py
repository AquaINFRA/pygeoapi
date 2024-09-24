
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import psycopg2
import pygeoapi.process.utils.upstream_helpers as helpers
from pygeoapi.process.geofresh.py_query_db import get_connection_object
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_dissolved_feature
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_dissolved_geometry
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_dissolved_feature_coll




'''

# Small:
curl -X POST "https:/aqua.igb-berlin.de/pygeoapi/processes/get-upstream-dissolved/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lon\": 9.931555, \"lat\": 54.695070, \"comment\": \"Nordoestliche Schlei bei Rabenholz\", \"add_upstream_ids\": \"true\"}}"

# Large: Mitten in der Elbe: 53.537158298376575, 9.99475350366553
curl -X POST "https:/aqua.igb-berlin.de/pygeoapi/processes/get-upstream-dissolved/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lon\": 9.994753, \"lat\": 53.537158, \"comment\": \"Mitten inner Elbe bei Hamburg\", \"geometry_only\": \"true\"}}"
'''


# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))

class UpstreamDissolvedGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None
        self.config = None

        # Set config:
        config_file_path = os.environ.get('AQUA90M_CONFIG_FILE', "./config.json")
        with open(config_file_path, 'r') as config_file:
            self.config = json.load(config_file)


    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<UpstreamDissolvedGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the upstream polygon..."')
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
        geometry_only = data.get('geometry_only', 'false')
        add_upstream_ids = data.get('add_upstream_ids', 'true')

        # Parse booleans
        geometry_only = (geometry_only.lower() == 'true')
        add_upstream_ids = (add_upstream_ids.lower() == 'true')

        # Overall goal: Get the upstream polygon (as one dissolved)!
        LOGGER.info('START: Getting upstream dissolved polygon for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))

        # Get reg_id, basin_id, subc_id
        subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, subc_id)

        # Get upstream id
        upstream_ids = helpers.get_upstream_catchment_ids(conn, subc_id, basin_id, reg_id, LOGGER)

        # Get geometry
        LOGGER.debug('...Getting upstream catchment dissolved polygon for subc_id: %s' % subc_id)
        polygon_simple = get_upstream_catchment_dissolved_geometry(
            conn, subc_id, upstream_ids, basin_id, reg_id)

        # Return only geometry:
        if geometry_only:

            if comment is not None:
                polygon_simple['comment'] = comment

            if self.return_hyperlink('polygon', requested_outputs):
                return 'application/json', self.store_to_json_file('polygon', polygon_simple)
            else:
                return 'application/json', polygon_simple


        # Return Feature:
        if not geometry_only:

            # TODO: Should we include the requested lon and lat? Maybe as a point? Then FeatureCollection?
            feature = {
                "type": "Feature",
                "geometry": polygon_simple,
                "properties": {
                    "description": "Dissolved upstream catchment of subcatchment %s" % subc_id,
                    "subc_id": subc_id,
                    "basin_id": basin_id,
                    "reg_id": reg_id
                }
            }

            if comment is not None:
                feature['properties']['comment'] = comment

            if add_upstream_ids:
                feature["properties"]["upstream_ids"] = upstream_ids

            if self.return_hyperlink('polygon', requested_outputs):
                return 'application/json', self.store_to_json_file('polygon', feature)
            else:
                return 'application/json', feature


    def return_hyperlink(self, output_name, requested_outputs):

        if requested_outputs is None:
            return False

        if 'transmissionMode' in requested_outputs.keys():
            if requested_outputs['transmissionMode'] == 'reference':
                return True

        if output_name in requested_outputs.keys():
            if 'transmissionMode' in requested_outputs[output_name]:
                if requested_outputs[output_name]['transmissionMode'] == 'reference':
                    return True

        return False


    def store_to_json_file(self, output_name, json_object):

        # Store to file
        downloadfilename = 'outputs-%s-%s.json' % (self.metadata['id'], self.job_id)
        downloadfilepath = self.config['download_dir']+downloadfilename
        LOGGER.debug('Writing process result to file: %s' % downloadfilepath)
        with open(downloadfilepath, 'w', encoding='utf-8') as downloadfile:
            json.dump(json_object, downloadfile, ensure_ascii=False, indent=4)

        # Create download link:
        downloadlink = self.config['download_url'] + downloadfilename

        # Create output to pass back to user
        outputs_dict = {
            'title': self.metadata['outputs'][output_name]['title'],
            'description': self.metadata['outputs'][output_name]['description'],
            'href': downloadlink
        }

        return outputs_dict


    def get_db_connection(self):

        config = self.config

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

