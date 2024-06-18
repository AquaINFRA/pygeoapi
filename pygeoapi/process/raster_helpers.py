from osgeo import gdal


def compress_tiff(result_filepath_uncompressed, result_filepath_compressed, LOGGER):

    # Compress
    # https://gis.stackexchange.com/questions/368874/read-and-then-write-rasterio-geotiff-file-without-loading-all-data-into-memory
    # https://gis.stackexchange.com/questions/42584/how-to-call-gdal-translate-from-python-code/237411#237411
    ds = gdal.Open(result_filepath_uncompressed)
    gdal.Translate(result_filepath_compressed, ds, creationOptions = ['COMPRESS=LZW'])
    try:
        ds.Close() # Some versions do not have this, apparently.
    except AttributeError as e:
        # https://gis.stackexchange.com/questions/80366/why-close-a-dataset-in-gdal-python
        LOGGER.debug('Cannot close gdal dataset: %s' % e)
        ds = None

    LOGGER.debug('Written to: %s' % result_filepath_compressed)
