
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
curl -X POST "https://aqua.igb-berlin.de/pygeoapi-dev/processes/get-shortest-path-to-sea/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lon\": 9.937520027160646, \"lat\": 54.69422745526058, \"comment\":\"Test\"}}"

'''

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'get-shortest-path-to-sea',
    'title': {'en': 'Dijkstra shortest path to sea'},
    'description': {
        'en': 'Return the shortest path stream from the stream segment'
              ' to which the given start point belongs to sea (outlet).'
              ' The path is returned as'
              ' GeoJSON FeatureCollection or GeometryCollection, depending '
              ' on user input. The geometries are LineStrings in both cases.'
              ' The start and end segments are included.'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['dijkstra', 'shortest-path', 'stream', 'stream-segment', 'geojson', 'GeoFRESH', 'hydrography90m', 'routing'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'GeoFRESH website',
        'href': 'https://geofresh.org/',
        'hreflang': 'en-US'
    },
    {
        'type': 'text/html',
        'rel': 'about',
        'title': 'On Stream segments (Hydrography90m)',
        'href': 'https://hydrography.org/hydrography90m/hydrography90m_layers',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'lon': {
            'title': 'Longitude (WGS84)',
            'description': 'Longitude of the starting point.',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use the Metadata item?
            'keywords': ['longitude', 'wgs84']
        },
        'lat': {
            'title': 'Latitude (WGS84)',
            'description': 'Latitude of the starting point.',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['latitude', 'wgs84']
        },
        'get_type': {
            'title': 'Get GeoJSON Feature',
            'description': 'Can be "GeometryCollection" or "FeatureCollection".',
            'schema': {'type': 'string'},
            'minOccurs': 0,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['comment']
        },
        'comment': {
            'title': 'Comment',
            'description': 'Arbitrary string that will not be processed but returned, for user\'s convenience.',
            'schema': {'type': 'string'},
            'minOccurs': 0,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['comment']
        },
    },
    'outputs': {
        'path': { # TODO: We return a GeoJSON object without a name, how to put that here?
            'title': 'Dijkstra shortest path to sea outlet',
            'description': 'GeometryCollection or FeatureCollection',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        },
    },
    'example': {
        'inputs': {
            'lon_start': '9.937520027160646',
            'lat_start': '54.69422745526058',
            'comment': 'test query'
        }
    }
}

class DijkstraShortestPathSeaGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def __repr__(self):
        return f'<DijkstraShortestPathSeaGetter> {self.name}'


    def execute(self, data):
        LOGGER.info('Starting to get the dijkstra shortest path to sea..."')
        try:
            return self._execute(data)
        except Exception as e:
            LOGGER.error(e)
            print(traceback.format_exc())
            raise ProcessorExecuteError(e, user_msg=str(e))

    def _execute(self, data):

        ## User inputs
        lon_start = float(data.get('lon'))
        lat_start = float(data.get('lat'))
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
            LOGGER.info('START: Getting dijkstra shortest path for lon %s, lat %s to sea' % (
                lon_start, lat_start))

            # Get reg_id, basin_id, subc_id
            subc_id1, basin_id1, reg_id1 = helpers.get_subc_id_basin_id_reg_id(conn, lon_start, lat_start, LOGGER)

            # Outlet has minus basin_id as subc_id!
            subc_id2 = -basin_id1

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

