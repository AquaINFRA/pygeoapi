
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
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_dissolved_feature
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_dissolved_geometry
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_dissolved_feature_coll




'''

curl -X POST "http://localhost:5000/processes/get-upstream-dissolved/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Nordoestliche Schlei, bei Rabenholz\"}}"

Mitten in der ELbe:
53.537158298376575, 9.99475350366553
curl -X POST "http://localhost:5000/processes/get-upstream-dissolved/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lon\": 9.994753, \"lat\": 53.537158, \"comment\":\"Mitten inner Elbe bei Hamburg\"}}"
'''


# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))

class UpstreamDissolvedGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True # Maybe before super() ?
        self.job_id = None
        # To support requested outputs, such as transmissionMode
        # https://github.com/geopython/pygeoapi/blob/fef8df120ec52121236be0c07022490803a47b92/pygeoapi/process/manager/base.py#L253


    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<UpstreamDissolvedGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the upstream polygon..."')
        LOGGER.info('Inputs: %s' % data)
        LOGGER.info('Requested outputs: %s' % outputs)

        # Which transmission mode is set for all outputs?
        # And make outputs dict consistent...
        outputs, transmissionMode_all = self.get_overall_transmission_mode(outputs)

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

        # Overall goal: Get the upstream polygon (as one dissolved)!
        LOGGER.info('START: Getting upstream dissolved polygon for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))

        # Get reg_id, basin_id, subc_id, upstream_ids
        subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, subc_id)
        upstream_ids = helpers.get_upstream_catchment_ids(conn, subc_id, basin_id, reg_id, LOGGER)

        # Generate empty feature to be filled with requested outputs:
        # TODO: Should user have to specify that they want basin_id and reg_id? I guess not?
        # TODO: Should we include the requested lon and lat? Maybe as a point? Then FeatureCollection?
        feature = {
            "type": "Feature",
            "geometry": None,
            "properties": {
                "description": "Dissolved upstream catchment of subcatchment %s" % subc_id,
                "subc_id": subc_id, # TODO how to name it?
                "basin_id": basin_id,
                "reg_id": reg_id
            }
        }

        if comment is not None:
            feature['properties']['comment'] = comment


        ################
        ### Results: ###
        ################

        outputs_dict = {}

        if comment is not None: # TODO this is double!
            geojson_object['comment'] = comment

        if 'polygon' in requested_outputs or 'ALL' in requested_outputs:

            # Get geometry
            LOGGER.debug('...Getting upstream catchment dissolved polygon for subc_id: %s' % subc_id)
            polygon = get_upstream_catchment_dissolved_geometry(
                conn, subc_id, upstream_ids, basin_id, reg_id)

            # Adding the geometry to the feature:
            feature["geometry"] = polygon      

        if 'upstream_ids' in requested_outputs or 'ALL' in requested_outputs:
            LOGGER.info('User asks for upstream catchment ids')

            ## Adding the upstream ids to the feature:
            feature["properties"]["upstream_ids"] = upstream_ids


        ###########################
        ### Return JSON or link ###
        ###########################

        if transmissionMode_all == 'value':
            return 'application/json', feature

        else:
            # TODO: This may not be correct, as reference includes that the link is returned in
            # a location header rather than in the response body!
            # Store file # TODO: Not hardcode that directory!
            downloadfilename = 'outputs-get-upstream-dissolved-%s.json' % self.job_id
            downloadfilepath = '/var/www/nginx/download'+os.sep+downloadfilename
            LOGGER.debug('Writing process result to file: %s' % downloadfilepath)
            with open(downloadfilepath, 'w', encoding='utf-8') as downloadfile:
                json.dump(feature, downloadfile, ensure_ascii=False, indent=4)

            # Create download link:
            # TODO: Not hardcode that URL! Get from my config file, or can I even get it from pygeoapi config?
            downloadlink = 'https://aqua.igb-berlin.de/download/'+downloadfilename

            # Create output to pass back to user
            outputs_dict = {
                'title': 'Upstream Catchment, can I take this from process description TODO',
                'description': 'Can I take this from process description TODO',
                'href': downloadlink
            }

            return 'application/json', outputs_dict



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


    def get_overall_transmission_mode(self, outputs):

        if outputs is None:
            LOGGER.info('Client did not specify outputs, so all possible outputs are returned!')
            outputs = {'ALL': None }
            return outputs, 'value' # default transmissionMode

        # Otherwise, iterate over all outputs. If they all have the same
        # transmissionMode (or none), great. If not, complain.
        transmissionMode_all = None
        for key in outputs.keys():

            if not 'transmissionMode' in outputs[key]:
                pass # leave empty and see what is set for the others

            elif outputs[key]['transmissionMode'] == 'value':
                if transmissionMode_all == 'reference':
                    raise ProcessorExecuteError(user_msg='Cannot mix transmissionMode "value" (%s) with "reference' % key)

            elif outputs[key]['transmissionMode'] == 'reference':
                if transmissionMode_all == 'value':
                    raise ProcessorExecuteError(user_msg='Cannot mix transmissionMode "reference" (%s) with "value' % key)
            else:
                error_message = 'Did not understand "transmissionMode" of requested output "%s": "%s". Has to be either "value" or "reference"' % (
                    key, outputs[key]['transmissionMode'])
                raise ProcessorExecuteError(user_msg=error_message)

        # If no output had a tranmissionMode set, set it to 'value':
        if transmissionMode_all is None:
            transmissionMode_all = "value" # default

        # Fill up the ones left empty:
        for key in outputs.keys():
            if not 'transmissionMode' in outputs[key]:
                outputs[key]['transmissionMode'] = transmissionMode_all

        return outputs, transmissionMode_all