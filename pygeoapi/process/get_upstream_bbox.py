
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
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_bbox_polygon





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
        self.supports_outputs = True
        self.job_id = None


    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<UpstreamBboxGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the upstream bounding box..."')
        LOGGER.info('Inputs: %s' % data)
        LOGGER.info('Requested outputs: %s' % outputs)

        # Check for which outputs it is asking:
        if outputs is None:
            LOGGER.info('Client did not specify outputs, so all possible outputs are returned!')
            outputs = {'ALL': None}

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
        comment = data.get('comment', None) # optional
        add_upstream_ids = data.get('add_upstream_ids', 'false')
        geometry_only = data.get('geometry_only', 'false')

        # Parse booleans
        add_upstream_ids = (add_upstream_ids.lower() == 'true')
        geometry_only = (geometry_only.lower() == 'true')

        # Overall goal: Get the upstream stream segments!
        LOGGER.info('START: Getting upstream bbox for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))

        # Get reg_id, basin_id, subc_id, upstream_ids
        subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, subc_id)
        upstream_ids = helpers.get_upstream_catchment_ids(conn, subc_id, basin_id, reg_id, LOGGER)

        # Get bounding box:
        bbox_geojson = get_upstream_catchment_bbox_polygon(conn, subc_id, upstream_ids, basin_id, reg_id)
        # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
        # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

        if geometry_only:

            if comment is not None:
                bbox_geojson['comment'] = comment

            return 'application/json', bbox_geojson

        # Generate feature:
        # TODO: Should we include the requested lon and lat? Maybe as a point? Then FeatureCollection?
        feature = {
            "type": "Feature",
            "geometry": bbox_geojson,
            "properties": {
                "description": "Bounding box of the upstream catchment of subcatchment %s" % subc_id,
                "subc_id": subc_id, # TODO how to name it?
                "basin_id": basin_id,
                "reg_id": reg_id
            }
        }

        if comment is not None:
            feature['properties']['comment'] = comment

        if add_upstream_ids:
            feature['properties']['upstream_ids'] = upstream_ids

        return 'application/json', feature



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
