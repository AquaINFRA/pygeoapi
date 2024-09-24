from pygeoapi.process.geofresh.py_query_db import get_reg_id
from pygeoapi.process.geofresh.py_query_db import get_basin_id_reg_id
from pygeoapi.process.geofresh.py_query_db import get_subc_id_basin_id
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_ids_incl_itself


def get_subc_id_basin_id_reg_id(conn, LOGGER, lon = None, lat = None, subc_id = None):

    # If user provided subc_id, then use it!
    if subc_id is not None:
        LOGGER.debug('... Getting subcatchment, region and basin id for subc_id: %s' % subc_id)
        subc_id, basin_id, reg_id = get_subc_id_basin_id_reg_id_from_subc_id(conn, subc_id, LOGGER)

    # Standard case: User provided lon and lat!
    elif lon is not None and lat is not None:
            LOGGER.debug('... Getting subcatchment, region and basin id for lon, lat: %s, %s' % (lon, lat))
            lon = float(lon)
            lat = float(lat)
            subc_id, basin_id, reg_id = get_subc_id_basin_id_reg_id_from_lon_lat(conn, lon, lat, LOGGER)
    else:
        error_message = 'Lon and lat (or subc_id) have to be provided! Lon: %s, lat: %s, subc_id %s' % (lon, lat, subc_id)
        raise ValueError(error_message)

    return subc_id, basin_id, reg_id



#def get_subc_id_basin_id_reg_id(conn, lon, lat, LOGGER):# RENAMED!
def get_subc_id_basin_id_reg_id_from_lon_lat(conn, lon, lat, LOGGER):


    # Get reg_id
    reg_id = get_reg_id(conn, lon, lat)
    
    if reg_id is None: # Might be in the ocean!
        error_message = "Caught an error that should have been caught before! (reg_id = None)!"
        LOGGER.error(error_message)
        raise ValueError(error_message)

    # Get basin_id, subc_id
    subc_id, basin_id = get_subc_id_basin_id(conn, lon, lat, reg_id)
    
    if basin_id is None:
        LOGGER.error('No basin_id id found for lon %s, lat %s !' % (lon, lat))
    
    LOGGER.debug('... Subcatchment has subc_id %s, basin_id %s, reg_id %s.' % (subc_id, basin_id, reg_id))

    return subc_id, basin_id, reg_id

def get_subc_id_basin_id_reg_id_from_subc_id(conn, subc_id, LOGGER):

    # Get basin_id, reg_id
    basin_id, reg_id = get_basin_id_reg_id(conn, subc_id)
    
    if reg_id is None:
        error_message = 'No reg_id id found for subc_id %s' % subc_id
        LOGGER.error(error_message)
        raise ValueError(error_message)
    
    if basin_id is None:
        error_message = 'No basin_id id found for subc_id %s' % subc_id
        LOGGER.error(error_message)
        raise ValueError(error_message)
    
    LOGGER.debug('... Subcatchment has subc_id %s, basin_id %s, reg_id %s.' % (subc_id, basin_id, reg_id))

    return subc_id, basin_id, reg_id


def get_upstream_catchment_ids(conn, subc_id, basin_id, reg_id, LOGGER):

    # Get upstream catchment subc_ids
    LOGGER.debug('... Getting upstream catchment for subc_id: %s' % subc_id)
    upstream_catchment_subcids = get_upstream_catchment_ids_incl_itself(conn, subc_id, basin_id, reg_id)

    return upstream_catchment_subcids

