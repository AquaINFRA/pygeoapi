
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
from pygeoapi.process.geofresh.py_query_db import get_snapped_point_feature
from pygeoapi.process.geofresh.py_query_db import get_polygon_for_subcid_feature
import psycopg2

'''
Note:
TODO FIXME:
This should be replaced by using the normal get_snapped_point.py with parameter add_subcatchment,
but then I need to change my test HTML client, which currently only can make different process calls
by using different process id, and not by adding parameters.

curl -X POST "http://localhost:5000/processes/get-snapped-point/execution" -H "Content-Type: application/json" -d "{\"inputs\":{ \"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Nordoestliche Schlei, bei Rabenholz\"}}"


OLD:
curl -X POST "http://localhost:5000/processes/get-snapped-point/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"subc_id\": 506251252, \"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Nordoestliche Schlei, bei Rabenholz\"}}"

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class SnappedPointsGetterPlus(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.job_id = None


    def set_job_id(self, job_id: str):
        self.job_id = job_id


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

        ### User inputs
        lon = float(data.get('lon'))
        lat = float(data.get('lat'))
        comment = data.get('comment') # optional

        with open('/pygeoapi/config.json') as myfile:
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
            LOGGER.debug('Getting subcatchment for lon, lat: %s, %s' % (lon, lat))
            subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, None)

            LOGGER.debug('Getting snapped point for subc_id: %s' % subc_id)
            # Returned as feature "Point", and feature "LineString"
            strahler, snappedpoint_geojson, streamsegment_geojson = get_snapped_point_feature(
                conn, lon, lat, subc_id, basin_id, reg_id)
            # Get local subcatchment too
            subcatchment_geojson = get_polygon_for_subcid_feature(conn, subc_id, basin_id, reg_id)

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
            #outputs = {
            #    'snapped_point': snappedpoint_geojson,
            #    'stream_segment': streamsegment_geojson,
            #}
            # TODO! ADD THIS!
            #if comment is not None:
            #    outputs['comment'] = comment
            #outputs = snappedpoint_geojson # TODO: Also original point! MultipointI!
            snap_lon = snappedpoint_geojson["geometry"]["coordinates"][0]
            snap_lat = snappedpoint_geojson["geometry"]["coordinates"][1]
            connecting_line = {
                    "type": "Feature",
                    "properties": {"description": "connecting line"},
                    "geometry": {
                        "type": "LineString",
                        "coordinates":[[lon,lat],[snap_lon,snap_lat]]
                    }
            }
            outputs = {
                "type": "FeatureCollection",
                "features": [snappedpoint_geojson, streamsegment_geojson, connecting_line, subcatchment_geojson]
            }
            return 'application/json', outputs

        else:
            outputs = {
                'error_message': 'getting snapped point failed.',
                'details': error_message}
            if comment is not None:
                outputs['comment'] = comment
            LOGGER.warning('Getting snapped points failed. Returning error message.')
            return 'application/json', outputs

