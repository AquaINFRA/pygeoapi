
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
from pygeoapi.process.geofresh.py_query_db import get_connection_object
from pygeoapi.process.geofresh.py_query_db import get_reg_id
from pygeoapi.process.geofresh.py_query_db import get_subc_id_basin_id
from pygeoapi.process.geofresh.py_query_db import get_snapped_point_feature
from pygeoapi.process.geofresh.py_query_db import get_snapped_point_simple
import psycopg2

'''
curl -X POST "http://localhost:5000/processes/get-snapped-point/execution" -H "Content-Type: application/json" -d "{\"inputs\":{ \"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Nordoestliche Schlei, bei Rabenholz\"}}"


OLD:
curl -X POST "http://localhost:5000/processes/get-snapped-point/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"subc_id\": 506251252, \"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Nordoestliche Schlei, bei Rabenholz\"}}"

'''

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'get-snapped-points',
    'title': {'en': 'Get corrected coordinates: Snapped to nearest stream segment.'},
    'description': {
        'en': 'Return a pair of coordinates that were snapped to the nearest stream'
              ' segment as a GeoJSON Point. Also return the stream segment as a GeoJSON'
              ' LineString, and basin id, region id, subcatchment id.'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['subcatchment', 'GeoFRESH', 'stream', 'stream-segment', 'geojson', 'hydrography90m'],
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
        #'subc_id': {
        #    'title': 'Subcatchment Id',
        #    'description': 'Id of the subcatchment where the point is located. Example: 553495421.',
        #    'schema': {'type': 'string'},
        #    'minOccurs': 1,
        #    'maxOccurs': 1,
        #    'metadata': None,  # TODO how to use the Metadata item?
        #    'keywords': ['subcatchment', "stream_segment"]
        #},
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
        'snapped_point': {
            'title': 'Snapped Point',
            'description': 'WGS84 coordinates of the snapped point.',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        },
        'stream_segment': {
            'title': 'Stream Segment',
            'description': 'WGS84 coordinates of the stream segment.',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            #'subc_id': '506251252',
            'lon': '9.931555',
            'lat': '54.695070',
            'comment': 'located in schlei area'
        }
    }
}

class SnappedPointsGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def __repr__(self):
        return f'<SnappedPointsGetter> {self.name}'


    def execute(self, data):
        LOGGER.info('Starting to get the snapped point coordinates..."')
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
        get_type = data.get('get_type', 'not-feature') # 'feature' or something else...
        comment = data.get('comment') # optional

        with open('config.json') as myfile:
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
            LOGGER.info('Getting snapped point...')
            LOGGER.debug('... First, getting subcatchment for lon, lat: %s, %s' % (lon, lat))
            reg_id = get_reg_id(conn, lon, lat)
            subc_id, basin_id = get_subc_id_basin_id(conn, lon, lat, reg_id)

            if get_type.lower() == 'feature':
                # Returned as feature "Point", and feature "LineString"
                LOGGER.debug('... Now, getting snapped point for subc_id (as feature): %s' % subc_id)
                strahler, feature_snappedpoint, feature_streamsegment = get_snapped_point_feature(
                conn, lon, lat, subc_id, basin_id, reg_id)
            else:
                # Returned as simple GeoJSON geometries "Point" and "LineString"
                LOGGER.debug('... Now, getting snapped point for subc_id (as simple geometries): %s' % subc_id)
                strahler, point_snappedpoint, linestring_streamsegment = get_snapped_point_simple(
                conn, lon, lat, subc_id, basin_id, reg_id)


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

        if error_message is None and not get_type.lower() == 'feature':
            outputs = point_snappedpoint
            LOGGER.info('Returning a single simple point geometry.')
            return 'application/json', outputs

        elif error_message is None and get_type.lower() == 'feature':
            # Construct connecting line:
            snap_lon = feature_snappedpoint["geometry"]["coordinates"][0]
            snap_lat = feature_snappedpoint["geometry"]["coordinates"][1]
            feature_connecting_line = {
                    "type": "Feature",
                    "properties": {"description": "connecting line"},
                    "geometry": {
                        "type": "LineString",
                        "coordinates":[[lon,lat],[snap_lon,snap_lat]]
                    }
            }
            outputs = {
                "type": "FeatureCollection",
                "features": [feature_snappedpoint, feature_streamsegment, feature_connecting_line]
            }
            LOGGER.info('Returning snapped point and its stream segment as FeatureCollection')
            return 'application/json', outputs

        else:
            outputs = {
                'error_message': 'getting snapped point failed.',
                'details': error_message}
            if comment is not None:
                outputs['comment'] = comment
            LOGGER.warning('Getting snapped points failed. Returning error message.')
            return 'application/json', outputs

