
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import psycopg2
from pygeoapi.process.geofresh.py_query_db import get_connection_object
from pygeoapi.process.geofresh.py_query_db import get_reg_id
from pygeoapi.process.geofresh.py_query_db import get_subc_id_basin_id
from pygeoapi.process.geofresh.py_query_db import get_polygon_for_subcid_feature 
from pygeoapi.process.geofresh.py_query_db import get_strahler_and_stream_segment_feature


'''
curl -X POST "http://localhost:5000/processes/get-stream-segment/execution" -H "Content-Type: application/json" -d "{\"inputs\":{ \"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Nordoestliche Schlei, bei Rabenholz\"}}"

'''

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'get-stream-segment-plus',
    'title': {'en': 'Return a Stream Segment as GeoJSON'},
    'description': {
        'en': 'Return the Stream Segment of the single subcatchment'
              ' (into which the given point falls)'
              ' as a GeoJSON Feature (where the geometry is a LineString).'
              ' Upstream not included.'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['subcatchment', 'stream', 'stream-segment', 'geojson', 'GeoFRESH', 'hydrography90m'],
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
            'description': 'Longitude....',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use the Metadata item?
            'keywords': ['longitude', 'wgs84']
        },
        'lat': {
            'title': 'Latitude (WGS84)',
            'description': 'Latitude....',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use the Metadata item?
            'keywords': ['latitude', 'wgs84']
        },
        'comment': {
            'title': 'Comment',
            'description': 'Arbitrary string that will not be processed but returned, for user\'s convenience.',
            'schema': {'type': 'string'},
            'minOccurs': 0,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use the Metadata item?
            'keywords': ['comment']
        }
    },
    'outputs': {
        'stream_segment': {
            'title': 'Stream Segment',
            'description': 'WGS84 coordinates of the stream segment.',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        },
        'strahler_order': {
            'title': 'Strahler Order',
            'description': 'Strahler order of the stream segment.',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        },
        'comment': {
            'title': 'Comment',
            'description': 'Arbitrary string provided by the user.',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'lon': '9.931555',
            'lat': '54.695070',
            'comment': 'located in schlei area'
        }
    }
}

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

        lon = float(data.get('lon'))
        lat = float(data.get('lat'))
        comment = data.get('comment') # optional

        with open('/opt/pyg_upstream/config.json') as myfile:
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
            LOGGER.info('Getting stream segment and subcatchment for lon, lat: %s, %s' % (lon, lat))
            LOGGER.debug('... First, getting subcatchment for lon, lat: %s, %s' % (lon, lat))
            reg_id = get_reg_id(conn, lon, lat)
            subc_id, basin_id = get_subc_id_basin_id(conn, lon, lat, reg_id)
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

