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
from rasterio import warp
#import rasterio
#import rasterio.mask
from osgeo import gdal
import json
import pygeoapi.process.utils.raster_helpers as helpers
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError


'''
curl -X POST "https://aqua.igb-berlin.de/pygeoapi/processes/get-subset-by-bbox/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"north\": 72.1, \"south\": 66.1, \"west\": 13.3, \"east\": 16.3}}" -o /tmp/rasteroutput.tiff
'''


LOGGER = logging.getLogger(__name__)

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class SubsetterBbox(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None

    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def __repr__(self):
        return f'<SubsetBboxProcessor> {self.name}'

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
        north_lat = float(data.get('north'))
        south_lat = float(data.get('south'))
        east_lon = float(data.get('east'))
        west_lon = float(data.get('west'))

        # Check if inside our boundaries:
        _check_boundaries(north_lat, south_lat, east_lon, west_lon)

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

        LOGGER.info('Subsetting by window (bbox)')
        _subset_by_window(input_raster_filepath, result_filepath_uncompressed, north_lat, south_lat, east_lon, west_lon)

        #LOGGER.info('Subsetting by polygon (bbox)') # Note: This is slower!
        #polygon = _make_bbox_geojson(north_lat, south_lat, east_lon, west_lon)
        #_subset_by_polygon(polygon, input_raster_filepath, result_filepath_uncompressed) # same function as subset_by_polygon

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


def _check_boundaries(north_lat, south_lat, east_lon, west_lon):

    if north_lat > 85 or south_lat > 85:
        raise ProcessorExecuteError('Cannot process latitudes above 85 degrees north! You specified: {north}, {south}'.format(
            north = north_lat, south = south_lat))
    if south_lat < 65 or north_lat < 65:
        raise ProcessorExecuteError('Cannot (currently) process latitudes under 65 degrees north! You specified: {north}, {south}'.format(
            north = north_lat, south = south_lat))
    if east_lon > 20 or west_lon > 20:
        raise ProcessorExecuteError('Cannot (currently) process longitudes above 20 degrees east! You specified: {west}, {east}.'.format(
            west = west_lon, east = east_lon))
    if west_lon < 0 or east_lon < 0:
        raise ProcessorExecuteError('Cannot (currently) process longitudes under 0 degrees east! You specified: {west}, {east}.'.format(
            west = west_lon, east = east_lon))
    if north_lat <= south_lat:
        raise ProcessorExecuteError('North latitude must be greater than south latitude! You specified: {north}, {south}'.format(
            north = north_lat, south = south_lat))
    if east_lon <= west_lon:
        raise ProcessorExecuteError('East longitude must be greater than west longitude! You specified: {west}, {east}.'.format(
            west = west_lon, east = east_lon))


def _make_bbox_geojson(north_lat, south_lat, east_lon, west_lon):

    NE_corner = [east_lon, north_lat]
    SE_corner = [east_lon, south_lat]
    SW_corner = [west_lon, south_lat]
    NW_corner = [west_lon, north_lat]

    polygon = {
        "type": "Polygon",
        "coordinates": [[NE_corner, SE_corner, SW_corner, NW_corner, NE_corner]]
    }
    #print('Bbox polygon:\n%s\n' % polygon)
    return polygon


def _subset_by_window(input_raster_filepath, result_filepath_uncompressed, north_lat, south_lat, east_lon, west_lon):

    # Make windows in col/row/pixels instead of WGS84
    win = _win_rows_cols(input_raster_filepath, west_lon, east_lon, south_lat, north_lat)

    # Subset raster
    window = rasterio.windows.Window(win["col_off"], win["row_off"], win["width"], win["height"])
    with rasterio.open(input_raster_filepath) as src:
        subset = src.read(1, window=window)
        result_metadata = src.meta.copy()

    # Prepare writing raster to GeoTIFF:
    subset_transform = rasterio.transform.from_bounds(
        west_lon,
        south_lat,
        east_lon,
        north_lat,
        width=subset.shape[0],
        height=subset.shape[1]
    )

    result_metadata.update({'driver':'GTiff',
        'width':subset.shape[0],
        'height':subset.shape[1],
        'transform':subset_transform,
        'nodata':0})

    # Write raster to disk as GeoTIFF:
    with rasterio.open(fp=result_filepath_uncompressed, mode='w',**result_metadata) as dst:
        dst.write(subset, 1)


# Function to return a window in rows and cols
def _win_rows_cols(file_path, west_lon, east_lon, south_lat, north_lat):

    # Get crs and transform from COG file
    with rasterio.open(file_path) as src:
        src_crs = src.crs
        src_transform = src.transform
        print('CRS: "%s" -> "%s"' % (src_crs, src_transform))

    # Projected coordinates
    X, Y = rasterio.warp.transform({'init': 'EPSG:4326'}, src_crs, [west_lon, east_lon], [south_lat, north_lat])
    print('X "%s" Y "%s"' % (X, Y))

    # Transform in row, col
    (rows, cols) = rasterio.transform.rowcol(src_transform, X, Y)
    print('Rows "%s" cols "%s"' % (rows, cols))
    ncols = cols[1]-cols[0]

    nrows = rows[0]-rows[1] # ! rows[0] > rows[1]
    return {"col_off": cols[0], "row_off": rows[1], "width": ncols, "height": nrows}



if __name__ == "__main__":

    print('Testing subsetting by bbox...')

    gdal.UseExceptions()

    with open('pygeoapi/config.json') as myfile:
        config = json.load(myfile)

    input_raster_basedir = config['base_dir_subsetting_tiffs']
    input_raster_filepath = input_raster_basedir.rstrip('/')+'/sub_catchment_h18v00.cog.tiff'
    result_filepath_uncompressed = r'/tmp/processresult_uncompressed.tif'
    result_filepath_compressed   = r'/tmp/processresult.tif'

    # Test the checks
    north_lat = 50
    south_lat = 95
    west_lon = -5
    east_lon = 99
    try:
        _check_boundaries(north_lat, south_lat, east_lon, west_lon)
        print('Wait, we expected a ProcessorExecuteError')
    except ProcessorExecuteError as e:
        print('Expected: ProcessorExecuteError')

    # Test the real thing:
    north_lat = 72
    south_lat = 70
    west_lon = 3
    east_lon = 4

    print('Run the subsetting...')
    _subset_by_window(input_raster_filepath, result_filepath_uncompressed, north_lat, south_lat, east_lon, west_lon)
    print('Run the compression...')
    _compress_tiff(result_filepath_uncompressed, result_filepath_compressed)
    print('FINISHED RUNNING!')
    print('Written to: %s' % result_filepath_compressed)
