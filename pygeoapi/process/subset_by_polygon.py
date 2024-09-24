# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2022 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import logging
import traceback
import os
import rasterio
import rasterio.mask
from osgeo import gdal
import json
import requests
import pygeoapi.process.raster_helpers as helpers
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError



'''
curl -X POST "https://aqua.igb-berlin.de/pygeoapi/processes/get-subset-by-polygon/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"polygon\": {\"type\": \"Polygon\", \"coordinates\": [ [ [ 15.081460166988848, 66.296144397828058 ], [ 13.809362140071178, 66.465757468083737 ], [ 13.809362140071178, 66.465757468083737 ], [ 13.809362140071178, 66.465757468083737 ], [ 14.948192754645092, 67.683337008133506 ], [ 15.711451570795695, 66.859502095463029 ], [ 14.493872030745925, 66.84738687615905 ], [ 15.081460166988848, 66.296144397828058 ] ] ] }}}" -o /tmp/rasteroutput.tiff

# Curl without polygon (fill in):
curl -X POST "https://aqua.igb-berlin.de/pygeoapi/processes/get-subset-by-polygon/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"polygon\": FILL_IN}}" -o /tmp/rasteroutput.tiff

# Curl with input reference url:
curl -X POST "https://aqua.igb-berlin.de/pygeoapi/processes/get-subset-by-polygon/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"href\": \"https://aqua.igb-berlin.de/download/example_input_polygon_for_subset_by_polygon.json\"}}" -o /tmp/rasteroutput.tiff

# Example polygon:
{\"type\": \"Polygon\", \"coordinates\": [ [ [ 15.081460166988848, 66.296144397828058 ], [ 13.809362140071178, 66.465757468083737 ], [ 13.809362140071178, 66.465757468083737 ], [ 13.809362140071178, 66.465757468083737 ], [ 14.948192754645092, 67.683337008133506 ], [ 15.711451570795695, 66.859502095463029 ], [ 14.493872030745925, 66.84738687615905 ], [ 15.081460166988848, 66.296144397828058 ] ] ] }

'''

LOGGER = logging.getLogger(__name__)

#: Process metadata and description
# Has to be in a JSON file of the same name, in the same dir!
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class SubsetterPolygon(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None

    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def __repr__(self):
        return f'<SubsetPolygonProcessor> {self.name}'

    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the subset from a GeoTIFF..."')
        LOGGER.info('Inputs: %s' % data)
        LOGGER.info('Requested outputs: %s' % outputs)

        # Check for which outputs it is asking:
        if outputs is None:
            LOGGER.info('Client did not specify outputs, so all possible outputs are returned!')
            outputs = {'ALL': None}

        try:
            res = self._execute(data, outputs)
            return res

        except Exception as e:
            LOGGER.error('During process execution, this happened: %s' % e)
            print(traceback.format_exc())
            raise ProcessorExecuteError(e) # TODO: Can we feed e into ProcessExecuteError?

    def _execute(self, data, requested_outputs):
        LOGGER.debug('Content of requested_outputs: %s' % requested_outputs)

        # User inputs:
        '''
        ## User inputs: Either "polygon" (GeoJSON), or "href" (link to a GeoJSON file)
        polygon = data.get('polygon', None)
        href = data.get('href', None)

        if polygon is None and href is None:
            err_msg = 'Please pass at least one of "polygon" or "href"!'
            LOGGER.error(err_msg)
            raise ValueError(err_msg)

        elif polygon is not None and href is not None:
            err_msg = 'Please pass only one of "polygon" or "href"!'
            LOGGER.error(err_msg)
            raise ValueError(err_msg)

        elif href is not None:
            # Read geojson from URL!
            LOGGER.debug('Reading GeoJSON from URL: %s' % href)
            resp = requests.get(href)
            polygon = resp.json()
            LOGGER.debug('We got this content (http %s): %s' % (resp, polygon))
        '''
        polygon = data.get('polygon', None)
        if polygon is None:
            LOGGER.error('*** ALL THE DATA: %s' % data)
        else:
            # TODO: How do I know whether I get a link or a geojson stuff? now it is all called polygon
            # Try reading geoJSON:
            #try:
            #    json.loads(json.dumps(polygon)) # TODO maybe better way to verify if this is json?
            #    # Damn this does not fail for a string!!!
            #except:
            try:
                if polygon['type'] == 'Feature' or polygon['type'] == 'Polygon' or polygon['type'] == 'FeatureCollection' or polygon['type'] == 'GeometryCollection':
                    LOGGER.error('LOOKS LIKE GEOJSON: %s' % polygon)
            except TypeError:
                if polygon.startswith('http'):
                    LOGGER.error('THIS IS A URLLL: %s' % polygon)
                    # Then it must be an URL!
                    # Read geojson from URL!
                    LOGGER.debug('Reading GeoJSON from URL: %s' % polygon)
                    resp = requests.get(polygon)
                    polygon = resp.json()
                    LOGGER.debug('We got this content (http %s): %s' % (resp, polygon))
                else:
                    LOGGER.error('No clue what this is: %s : %s' % (type(polygon), polygon))


        with open('pygeoapi/config.json') as myfile:
            config = json.load(myfile)

        # Where to find input data
        input_raster_basedir = config['base_dir_subsetting_tiffs']
        input_raster_filepath = input_raster_basedir.rstrip('/')+'/sub_catchment_h18v00.cog.tiff' # TODO this is just one small file!

        # Where to store output data
        result_filepath_uncompressed = r'/tmp/subset_%s_%s_uncompressed.tiff' % (self.metadata['id'], self.job_id)
        downloadfilename = 'outputs-%s-%s.tiff' % (self.metadata['id'], self.job_id)
        result_filepath_compressed = r'/var/www/nginx/download'+os.sep+downloadfilename # TODO Not hardcode this directory.
        # TODO: Must delete result files!

        # Run it:
        _subset_by_polygon(polygon, input_raster_filepath, result_filepath_uncompressed)
        helpers.compress_tiff(result_filepath_uncompressed, result_filepath_compressed, LOGGER)

        # Read bytestream from disk and return to user as application/octet-stream:
        with open(result_filepath_compressed, 'r+b') as myraster:
            resultfile = myraster.read()

        mimetype = 'application/octet-stream' # TODO: Probably a more specific type for GeoTIFF?

        if self.return_hyperlink('subset', requested_outputs):
            return 'application/json', self.get_download_link('subset', downloadfilename, mimetype)
        else:
            return mimetype, resultfile


    def return_hyperlink(self, output_name, requested_outputs):

        if requested_outputs is None:
            return False

        if 'transmissionMode' in requested_outputs.keys():
            if requested_outputs['transmissionMode'] == 'reference':
                return True

        if output_name in requested_outputs.keys():
            if 'transmissionMode' in requested_outputs[output_name]:
                if requested_outputs[output_name]['transmissionMode'] == 'reference':
                    return True

        return False


    def get_download_link(self, output_name, downloadfilename, mimetype):

        # Create download link:
        # TODO: Not hardcode that URL! Get from my config file, or can I even get it from pygeoapi config?
        downloadlink = 'https://aqua.igb-berlin.de/download/'+downloadfilename

        # Create output to pass back to user
        outputs_dict = {
            'title': self.metadata['outputs'][output_name]['title'],
            'description': self.metadata['outputs'][output_name]['description'],
            'mediatype': mimetype,
            'href': downloadlink
        }

        return outputs_dict

def _subset_by_polygon(shape, input_raster_filepath, result_filepath_uncompressed):

    # Subset raster
    # The values must be a GeoJSON-like dict or an object that implements the Python geo interface protocol (such as a Shapely Polygon).
    # https://gis.stackexchange.com/questions/459126/clipping-a-raster-with-a-multipolygon-using-rasterio-in-python
    #shape = { "type": "Polygon", "coordinates": [ [ [ 15.081460166988848, 66.296144397828058 ], [ 13.809362140071178, 66.465757468083737 ], [ 13.809362140071178, 66.465757468083737 ], [ 13.809362140071178, 66.465757468083737 ], [ 14.948192754645092, 67.683337008133506 ], [ 15.711451570795695, 66.859502095463029 ], [ 14.493872030745925, 66.84738687615905 ], [ 15.081460166988848, 66.296144397828058 ] ] ] }

    with rasterio.open(input_raster_filepath) as src:
        subset, subset_transform = rasterio.mask.mask(src, [shape], crop=True)
        result_metadata = src.meta.copy()

    result_metadata.update({
        "driver": "GTiff",
        "height": subset.shape[1],
        "width": subset.shape[2],
        "transform": subset_transform})

    # Write raster to disk as GeoTIFF:
    with rasterio.open(fp=result_filepath_uncompressed, mode='w',**result_metadata) as dst:
        dst.write(subset)





if __name__ == "__main__":

    gdal.UseExceptions()

    with open('pygeoapi/config.json') as myfile:
        config = json.load(myfile)

    input_raster_basedir = config['base_dir_subsetting_tiffs']
    input_raster_filepath = input_raster_basedir.rstrip('/')+'/sub_catchment_h18v00.cog.tiff'
    result_filepath_uncompressed = r'/tmp/processresult_uncompressed.tif'
    result_filepath = r'/tmp/processresult.tif'
    polygon = { "type": "Polygon", "coordinates": [ [ [ 15.081460166988848, 66.296144397828058 ], [ 13.809362140071178, 66.465757468083737 ], [ 13.809362140071178, 66.465757468083737 ], [ 13.809362140071178, 66.465757468083737 ], [ 14.948192754645092, 67.683337008133506 ], [ 15.711451570795695, 66.859502095463029 ], [ 14.493872030745925, 66.84738687615905 ], [ 15.081460166988848, 66.296144397828058 ] ] ] }


    print('RUN IT:')
    _execute(polygon, input_raster_filepath, result_filepath_uncompressed, result_filepath)
    print('FINISHED RUNNING IT!')
    print('Written to: %s' % result_filepath)

