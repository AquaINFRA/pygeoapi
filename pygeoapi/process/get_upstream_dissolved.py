
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import psycopg2
import pygeoapi.process.geofresh.upstream_helpers as helpers
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

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'get-upstream-dissolved',
    'title': {'en': 'Get Upstream Catchment as GeoJSON Polygon Feature'},
    'description': {
        'en': 'Return the geometry of the upstream catchment'
              ' of the subcatchment into which the given point falls'
              ' as a GeoJSON Feature (where the geometry is a Polygon).'
              ' The subcatchment itself not included.'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['subcatchment', 'upstream', 'geojson', 'GeoFRESH', 'hydrography90m', 'bbox'],
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
        'title': 'On subcatchments (Hydrography90m)',
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
        },
        'get_type': {
            'title': 'Get GeoJSON Feature',
            'description': 'Can be "feature", "polygon" or "feature_collection".',
            'schema': {'type': 'string'},
            'minOccurs': 0,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use the Metadata item?
            'keywords': ['comment']
        }
    },
    'outputs': {
        'subcatchment': {
            'title': 'Subcatchment Id',
            'description': 'Id of the subcatchment whose upstream catchment was computed. Example: 553495421.',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        },
        'upstream_catchment_ids': {
            'title': 'Upstream Catchment Ids',
            'description': 'List of subcatchment ids of the subcatchments included in the upstream catchment.',
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

class UpstreamDissolvedGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def __repr__(self):
        return f'<UpstreamDissolvedGetter> {self.name}'


    def execute(self, data):
        LOGGER.info('Starting to get the upstream bounding box..."')
        LOGGER.info('Inputs: %s' % data)
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
        get_type = data.get('get_type', 'polygon')

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
            # Overall goal: Get the upstream polygon (as one dissolved)!
            LOGGER.info('START: Getting upstream dissolved polygon for lon, lat: %s, %s' % (lon, lat))

            # Get reg_id, basin_id, subc_id, upstream_catchment_subcids
            subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, lon, lat, LOGGER)
            upstream_catchment_subcids = helpers.get_upstream_catchment_ids(conn, subc_id, basin_id, reg_id, LOGGER)

            # Get geometry (three types)
            LOGGER.debug('...Getting upstream catchment dissolved polygon for subc_id: %s' % subc_id)
            geojson_object = {}
            if get_type == 'polygon':
                geojson_object = get_upstream_catchment_dissolved_geometry(
                    conn, subc_id, upstream_catchment_subcids, basin_id, reg_id)
                LOGGER.debug('END: Received simple polygon : %s' % str(geojson_object)[0:50])

            elif get_type == 'feature':
                geojson_object = get_upstream_catchment_dissolved_feature(
                    conn, subc_id, upstream_catchment_subcids,
                    basin_id=basin_id, reg_id=reg_id, comment=comment)
                LOGGER.debug('END: Received feature : %s' % str(geojson_object)[0:50])
           
            elif get_type == 'feature_collection':
                geojson_object = get_upstream_catchment_dissolved_feature_coll(
                    conn, subc_id, upstream_catchment_subcids, lonlat=(lon, lat),
                    basin_id=basin_id, reg_id=reg_id, comment=comment)
                LOGGER.debug('END: Received feature collection: %s' % str(geojson_object)[0:50])

                

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
            output = geojson_object
            return 'application/json', output

        else:
            outputs = {
                'error_message': 'getting upstream polygon (dissolved) failed.',
                'details': error_message}
            if comment is not None:
                outputs['comment'] = comment
            LOGGER.warning('Getting upstream polygon (dissolved) failed. Returning error message.')
            return 'application/json', outputs

