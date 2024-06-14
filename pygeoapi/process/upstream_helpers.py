from pygeoapi.process.geofresh.py_query_db import get_reg_id
from pygeoapi.process.geofresh.py_query_db import get_subc_id_basin_id
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_ids_incl_itself

def get_subc_id_basin_id_reg_id(conn, lon, lat, LOGGER):

    LOGGER.debug('... Getting subcatchment for lon, lat: %s, %s' % (lon, lat))

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


def get_upstream_catchment_ids(conn, subc_id, basin_id, reg_id, LOGGER):

    # Get upstream catchment subc_ids
    LOGGER.debug('... Getting upstream catchment for subc_id: %s' % subc_id)
    upstream_catchment_subcids = get_upstream_catchment_ids_incl_itself(conn, subc_id, basin_id, reg_id)

    return upstream_catchment_subcids