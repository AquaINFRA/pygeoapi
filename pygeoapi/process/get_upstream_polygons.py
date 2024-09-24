
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
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_polygons_feature_coll
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_polygons_geometry_coll
import psycopg2



'''

curl -X POST "http://localhost:5000/processes/get-upstream-polygons/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Nordoestliche Schlei, bei Rabenholz\"}}"


'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class UpstreamPolygonGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None


    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<UpstreamPolygonGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the various upstream polygons..."')
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

        ## User inputs
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        geometry_only = data.get('geometry_only', 'false')
        add_upstream_ids = data.get('add_upstream_ids', 'false')

        # Parse add_upstream_ids
        geometry_only = (geometry_only.lower() == 'true')
        add_upstream_ids = (add_upstream_ids.lower() == 'true')

        # Overall goal: Get the upstream polygons (individual ones)
        LOGGER.info('START: Getting upstream polygons (individual ones) for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))

        # Get reg_id, basin_id, subc_id, upstream_ids
        subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, subc_id)
        upstream_ids = helpers.get_upstream_catchment_ids(conn, subc_id, basin_id, reg_id, LOGGER)

        # Get geometry only:
        if geometry_only:
            LOGGER.debug('...Getting upstream catchment polygons for subc_id: %s' % subc_id)
            geometry_coll = get_upstream_catchment_polygons_geometry_coll(
                conn, subc_id, upstream_ids, basin_id, reg_id)
            LOGGER.debug('END: Received GeometryCollection: %s' % str(geometry_coll)[0:50])

            if comment is not None:
                geometry_coll['comment'] = comment

            if self.return_hyperlink('polygons', requested_outputs):
                return 'application/json', self.store_to_json_file('polygons', geometry_coll)
            else:
                return 'application/json', geometry_coll

        # Get FeatureCollection
        if not geometry_only:
            LOGGER.debug('...Getting upstream catchment polygons for subc_id: %s' % subc_id)
            feature_coll = get_upstream_catchment_polygons_feature_coll(
                conn, subc_id, upstream_ids, basin_id, reg_id)
            LOGGER.debug('END: Received FeatureCollection: %s' % str(feature_coll)[0:50])

            feature_coll['reg_id'] = reg_id
            feature_coll['basin_id'] = basin_id
            feature_coll['upstream_catchment_of'] = subc_id

            if add_upstream_ids:
                feature_coll['upstream_ids'] = upstream_ids 

            if comment is not None:
                feature_coll['comment'] = comment

            if self.return_hyperlink('polygons', requested_outputs):
                return 'application/json', self.store_to_json_file('polygons', feature_coll)
            else:
                return 'application/json', feature_coll


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
        downloadfilename = 'outputs-%s-%s.json' % (self.metadata['id'], self.job_id)
        downloadfilepath = '/var/www/nginx/download'+os.sep+downloadfilename # TODO Not hardcode this directory.
        LOGGER.debug('Writing process result to file: %s' % downloadfilepath)
        with open(downloadfilepath, 'w', encoding='utf-8') as downloadfile:
            json.dump(json_object, downloadfile, ensure_ascii=False, indent=4)

        # Create download link:
        # TODO: Not hardcode that URL! Get from my config file, or can I even get it from pygeoapi config?
        downloadlink = 'https://aqua.igb-berlin.de/download/'+downloadfilename

        # Create output to pass back to user
        outputs_dict = {
            'title': self.metadata['outputs'][output_name]['title'],
            'description': self.metadata['outputs'][output_name]['description'],
            'href': downloadlink
        }

        return outputs_dict


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
