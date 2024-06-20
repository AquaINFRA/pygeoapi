
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
from pygeoapi.process.geofresh.py_query_db import get_strahler_and_stream_segment_feature
from pygeoapi.process.geofresh.py_query_db import get_strahler_and_stream_segment_linestring
from pygeoapi.process.geofresh.py_query_db import get_polygon_for_subcid_feature
from pygeoapi.process.geofresh.py_query_db import get_polygon_for_subcid_simple


'''

# Normal request, returning a simple geometry (linestring):
curl -X POST "https://aqua.igb-berlin.de/pygeoapi/processes/get-stream-segment/execution" -H "Content-Type: application/json" -d "{\"inputs\":{ \"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Schlei\"}}"

# Request returning a FeatureCollection and additionally the subcatchment polygon:
curl -X POST "https://aqua.igb-berlin.de/pygeoapi/processes/get-stream-segment/execution" -H "Content-Type: application/json" -d "{\"inputs\":{ \"lon\": 9.931555, \"lat\": 54.695070, \"add_subcatchment\": true, \"get_type\": \"FeatureCollection\"}}"

# Asking for a subcatchment polygon, but it cannot be fulfilled due to geometry compatibility:
curl -X POST "https://aqua.igb-berlin.de/pygeoapi/processes/get-stream-segment/execution" -H "Content-Type: application/json" -d "{\"inputs\":{ \"lon\": 9.931555, \"lat\": 54.695070, \"add_subcatchment\": true}}"
'''

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'get-stream-segment',
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
            'metadata': None,
            'keywords': ['latitude', 'wgs84']
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
        'get_type': {
            'title': 'Get GeoJSON Feature',
            'description': 'Can be "LineString", "Feature", "FeatureCollection" or "GeometryCollection".',
            'schema': {'type': 'string'},
            'minOccurs': 0,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['comment']
        },
        'add_subcatchment': {
            'title': 'Add subcatchment polygon',
            'description': 'Additionally request the subcatchment polygon (only for FeatureCollection or GeometryCollection, will be ignored for Point)',
            'schema': {'type': 'boolean'},
            'minOccurs': 0,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': ['comment']
        },
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
        },
    },
    'example': {
        'inputs': {
            'lon': '9.931555',
            'lat': '54.695070',
            'comment': 'located in schlei area'
        }
    }
}

class StreamSegmentGetter(BaseProcessor):

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

        ## User inputs
        lon = float(data.get('lon'))
        lat = float(data.get('lat'))
        comment = data.get('comment') # optional
        get_type = data.get('get_type', 'LineString')
        add_subcatchment = data.get('add_subcatchment', False)
        if not isinstance(add_subcatchment, bool):
            LOGGER.error('Expected a boolean for "add_subcatchment"!')

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
            LOGGER.info('Retrieving stream segment for lon, lat: %s, %s' % (lon, lat))
            LOGGER.debug('First, getting subcatchment for lon, lat: %s, %s' % (lon, lat))
            reg_id = get_reg_id(conn, lon, lat)
            subc_id, basin_id = get_subc_id_basin_id(conn, lon, lat, reg_id)
            LOGGER.debug('Now, getting stream segment (incl. strahler order) for subc_id: %s' % subc_id)


            if get_type.lower() == 'feature':
                feature_streamsegment = get_strahler_and_stream_segment_feature(
                    conn, subc_id, basin_id, reg_id)
                geojson_object = feature_streamsegment

                if add_subcatchment:
                    LOGGER.info('User also requested subcatchment, but that is not compatible with returning a single feature.')
                    geojson_object['note']: 'Cannot add subcatchment polygon to GeoJSON Feature.'


            elif get_type.lower() == 'linestring':
                strahler, streamsegment_simple_geometry = get_strahler_and_stream_segment_linestring(
                    conn, subc_id, basin_id, reg_id)
                geojson_object = streamsegment_simple_geometry

                if add_subcatchment:
                    LOGGER.info('User also requested subcatchment, but that is not compatible with returning a simple linestring.')
                    geojson_object['note'] = 'Cannot add subcatchment polygon to GeoJSON linestring.'


            elif get_type.lower() == 'featurecollection':
                feature_streamsegment = get_strahler_and_stream_segment_feature(
                    conn, subc_id, basin_id, reg_id)

                geojson_object = {
                    "type": "FeatureCollection",
                    "features": [feature_streamsegment]
                }

                # In some cases, we also want to add the subcatchment polygon!
                # (This is faster than querying the service twice).
                if add_subcatchment:
                    feature_subcatchment = get_polygon_for_subcid_feature(conn, subc_id, basin_id, reg_id)
                    geojson_object["features"].append(feature_subcatchment)


            elif get_type.lower() == 'geometrycollection':
                strahler, streamsegment_simple_geometry = get_strahler_and_stream_segment_linestring(
                    conn, subc_id, basin_id, reg_id)

                geojson_object = {
                     "type": "GeometryCollection",
                     "geometries": [streamsegment_simple_geometry]
                }

                # In some cases, we also want to add the subcatchment polygon!
                # (This is faster than querying the service twice).
                if add_subcatchment:
                    polygon_subcatchment = get_polygon_for_subcid_simple(conn, subc_id, basin_id, reg_id)
                    geojson_object["geometries"].append(polygon_subcatchment)

            else:
                err_msg = "Input parameter 'get_type' can only be one of LineString, Feature, FeatureCollection and GeometryCollection!"
                # TODO: API definition: What is better: Feature vs SimpleGeometry, or Feature vs LineString / Point / Polygon / ... ?
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

            # TODO API definition: Comments as part of properties (for features), and then for geometries just adding it??
            if comment is not None:
                geojson_object['comment'] = comment

            return 'application/json', geojson_object

        else:
            output = {
                'error_message': 'getting stream segment failed.',
                'details': error_message}

            if comment is not None:
                output['comment'] = comment

            LOGGER.warning('Getting stream segment failed. Returning error message.')
            return 'application/json', output

