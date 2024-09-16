
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


    def __repr__(self):
        return f'<UpstreamDissolvedGetter> {self.name}'


    def set_job_id(self, job_id: str):
        self.job_id = job_id



    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the upstream bounding box..."')
        LOGGER.info('Inputs: %s' % data)
        LOGGER.info('Outputs: %s' % outputs)

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
        get_type = data.get('get_type', 'polygon')

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
            # Overall goal: Get the upstream polygon (as one dissolved)!
            LOGGER.info('START: Getting upstream dissolved polygon for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))

            # Get reg_id, basin_id, subc_id, upstream_catchment_ids
            subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, subc_id)
            upstream_catchment_ids = helpers.get_upstream_catchment_ids(conn, subc_id, basin_id, reg_id, LOGGER)

            # Get geometry (three types)
            LOGGER.debug('...Getting upstream catchment dissolved polygon for subc_id: %s' % subc_id)
            geojson_object = {}
            if get_type.lower() == 'polygon':
                geojson_object = get_upstream_catchment_dissolved_geometry(
                    conn, subc_id, upstream_catchment_ids, basin_id, reg_id)
                LOGGER.debug('END: Received simple polygon : %s' % str(geojson_object)[0:50])

            elif get_type.lower() == 'feature':
                geojson_object = get_upstream_catchment_dissolved_feature(
                    conn, subc_id, upstream_catchment_ids,
                    basin_id, reg_id, comment=comment)
                LOGGER.debug('END: Received feature : %s' % str(geojson_object)[0:50])
           
            elif get_type.lower() == 'featurecollection':
                geojson_object = get_upstream_catchment_dissolved_feature_coll(
                    conn, subc_id, upstream_catchment_ids, (lon, lat),
                    basin_id, reg_id, comment=comment)
                LOGGER.debug('END: Received feature collection: %s' % str(geojson_object)[0:50])

            else:
                err_msg = "Input parameter 'get_type' can only be one of Polygon or Feature or FeatureCollection!"
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
            outputs_dict = {}

            if comment is not None: # TODO this is double!
                geojson_object['comment'] = comment

            # Check for which outputs it is asking:
            if requested_outputs is None:
                LOGGER.info('USER DID NOT SPECIFY OUTPUT SO WE PASS ALL OF THEM!')
                requested_outputs = {'ALL': None}

            if 'subcatchment' in requested_outputs or 'ALL' in requested_outputs:
                LOGGER.info('USER ASKS FOR SUBCATCHMENT')

                try:
                    transmission_mode = requested_outputs['subcatchment']['transmissionMode']
                except (KeyError, TypeError) as e:
                    # KeyError if requested_outputs is a dict, but without subcatchment
                    # TypeError if requested_outputs is list! ("list indices must be integers or slices, not str")
                    LOGGER.debug('transmissionMode not passed for output "subcatchment": %s' % e)
                    transmission_mode = 'value' # default

                if transmission_mode == 'value':
                    LOGGER.info('USER ASKS FOR SUBCATCHMENT VALUE')
                    outputs_list.append({'subcatchment': geojson_object})

                elif transmission_mode == 'reference':
                    # TODO: This may not be correct, as reference includes that the link is returned in
                    # a location header rather than in the response body!
                    # Store file # TODO: Not hardcode that directory!
                    downloadfilename = 'subcatchment-%s.json' % self.job_id
                    downloadfilepath = '/var/www/nginx/download'+os.sep+downloadfilename
                    LOGGER.debug('Writing process result to file: %s' % downloadfilepath)
                    with open(downloadfilepath, 'w', encoding='utf-8') as downloadfile:
                        json.dump(geojson_object, downloadfile, ensure_ascii=False, indent=4)

                    # Create download link:
                    # TODO: Not hardcode that URL! Get from my config file, or can I even get it from pygeoapi config?
                    downloadlink = 'https://aqua.igb-berlin.de/download/'+downloadfilename

                    # Create output to pass back to user
                    json_response = {
                        'title': 'Subcatchment, can I take this from process description TODO',
                        'description': 'Can I take this from process description TODO',
                        'href': downloadlink
                    }
                    outputs_dict['subcatchment'] = json_response

                else:
                    LOGGER.error('Cannot understand transmissionMode: %s' % transmission_mode)

            if 'upstream_catchment_ids' in requested_outputs or 'ALL' in requested_outputs:
                LOGGER.info('USER ASKS FOR UPSTREAM CATCHMENT IDS')

                try:
                    transmission_mode = requested_outputs['upstream_catchment_ids']['transmissionMode']
                except KeyError as e:
                    LOGGER.debug('transmissionMode not passed for upstream_catchment_ids: %s' % e)
                    transmission_mode = 'value' # default

                if transmission_mode == 'value':
                    LOGGER.info('USER ASKS FOR UPSTREAM CATCHMENT IDS VALUE')
                    outputs_dict['upstream_catchment_ids'] = geojson_object
                
                elif transmission_mode == 'reference':
                    LOGGER.info('USER ASKS FOR UPSTREAM CATCHMENT IDS REFERENCE')

                    # Store file # TODO: Not hardcode that directory!
                    downloadfilename = 'upstream_catchment_ids-%s.json' % self.job_id
                    downloadfilepath = '/var/www/nginx/download'+os.sep+downloadfilename
                    LOGGER.debug('Writing process result to file: %s' % downloadfilepath)
                    with open(downloadfilepath, 'w', encoding='utf-8') as downloadfile:
                        json.dump(upstream_catchment_ids, downloadfile, ensure_ascii=False, indent=4)

                    # Create download link:
                    # TODO: Not hardcode that URL! Get from my config file, or can I even get it from pygeoapi config?
                    downloadlink = 'https://aqua.igb-berlin.de/download/'+downloadfilename

                    # Create output to pass back to user
                    json_response = {
                        'title': 'Upstream catchment ids, can I take this from process description TODO',
                        'description': 'Can I take this from process description TODO',
                        'href': downloadlink
                    }
                    outputs_dict['upstream_catchment_ids'] = json_response


                else:
                    LOGGER.error('Cannot understand transmissionMode: %s' % transmission_mode)

            return 'application/json', outputs_dict


        else:
            output = { # TODO check syntax here!
                'error_message': 'getting upstream polygon (dissolved) failed.',
                'details': error_message}

            if comment is not None:
                output['comment'] = comment

            LOGGER.warning('Getting upstream polygon (dissolved) failed. Returning error message.')
            return 'application/json', output

