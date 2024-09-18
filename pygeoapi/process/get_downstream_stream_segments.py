
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
from pygeoapi.process.geofresh.py_query_db import get_dijkstra_ids
from pygeoapi.process.geofresh.py_query_db import get_simple_linestrings_for_subc_ids
from pygeoapi.process.geofresh.py_query_db import get_feature_linestrings_for_subc_ids

'''
curl -X POST "https://aqua.igb-berlin.de/pygeoapi-dev/processes/get-shortest-path-to-sea/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lon\": 9.937520027160646, \"lat\": 54.69422745526058, \"comment\":\"Test\"}}"

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class DijkstraShortestPathSeaGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None


    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<DijkstraShortestPathSeaGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the dijkstra shortest path to sea..."')
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

        # TODO: Must change behaviour based on content of requested_outputs
        LOGGER.debug('Content of requested_outputs: %s' % requested_outputs)

        ## User inputs
        lon_start = data.get('lon', None)
        lat_start = data.get('lat', None)
        subc_id1 = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional

        # Overall goal: Get the dijkstra shortest path (as linestrings)!
        LOGGER.info('START: Getting dijkstra shortest path for lon %s, lat %s (or subc_id %s) to sea' % (
            lon_start, lat_start, subc_id1))
        subc_id1, basin_id1, reg_id1 = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon_start, lat_start, subc_id1)

        # Outlet has minus basin_id as subc_id!
        subc_id2 = -basin_id1

        # Get subc_ids of the whole connection...
        LOGGER.debug('Getting network connection for subc_id: start = %s, end = %s' % (subc_id1, subc_id2))
        downstream_ids = get_dijkstra_ids(conn, subc_id1, subc_id2, reg_id1, basin_id1)

        # To be returned
        outputs = {}

        # If user ONLY wants geometry collection, make Geometry collection.
        # In all other cases, a FeatureCollection is returned. This slightly
        # violates the principles of returning ALL if none are requested, and
        # we'll ignore the user's wish for GeometryCollection if they also ask
        # for other things, but overall, this seems more useful than anything else...
        if set('path_geometry_collection') == set(requested_outputs.keys())
            dijkstra_path_list = get_simple_linestrings_for_subc_ids(
                conn, downstream_ids, basin_id1, reg_id1)

            outputs = {
                "type": "GeometryCollection",
                "geometries": dijkstra_path_list
            }
        
        else:
            # Generate empty feature collection to be filled with requested outputs:
            # TODO: Should user have to specify that they want basin_id and reg_id? I guess not?
            # TODO: Should we include the requested lon and lat? Maybe as a point?
            outputs = {
                "type": "FeatureCollection",
                "features": [],
                "description": "Downstream BLABLA TODO of %s" % subc_id1,
                "start_subc_id": subc_id1, # TODO how to name it?
                "basin_id": basin_id1,
                "region_id": reg_id1,
                "outlet_id": subc_id2
            }
            if comment is not None:
                outputs['comment'] = comment

        if 'downstream_ids' in requested_outputs or 'ALL' in requested_outputs:
            outputs['downstream_ids'] = downstream_ids

        if 'path_feature_collection' in requested_outputs or 'ALL' in requested_outputs:
            
            dijkstra_path_list = get_feature_linestrings_for_subc_ids(
                conn, downstream_ids, basin_id1, reg_id1):

            outputs["features"] = dijkstra_path_list


        ################
        ### Results: ###
        ################

        return 'application/json', outputs
        # TODO: So far, we are packaging all requested outputs into one GeoJSON
        # object.


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
