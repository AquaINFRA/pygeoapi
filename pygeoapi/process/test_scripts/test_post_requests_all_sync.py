import requests
import sys
import datetime

'''
This is just a little script to test whether the OGC processing
services of GeoFREHS / hydrographr were properly
installed using pygeoapi and run as expected.
This does not test any edge cases, just a very basic setup. The input
data may already be on the server, so proper downloading is not 
guaranteed.

Check the repository here:
https://github.com/glowabio/geofresh
https://glowabio.github.io/hydrographr/

Merret Buurman (IGB Berlin), 2024-08-20
'''


base_url = 'https://xxx.xxx/pygeoapi'
headers  = {'Content-Type': 'application/json'}
#headers = {'Content-Type': 'application/json', 'Prefer': 'respond-async'}


# Get started...
session = requests.Session()


'''
Local catchment:
1  get_subc_from_coords.py
2  get_snapped_points.py
2b get_snapped_points_plus.py
3  get_stream_segment.py
3b get_stream_segment_plus.py

Routing:
4  get_downstream_stream_segments.py
5  get_dijkstra_stream_segments.py

Upstream catchment:
6  get_upstream_subcids.py
7  get_upstream_stream_segments.py
8  get_upstream_polygons.py
9  get_upstream_bbox.py
10 get_upstream_dissolved.py

'''


#####################
#####################
### Local queries ###
#####################
#####################

#############################
### 1 get-subcatchment-id ###  get_subc_from_coords
#############################
name = "get-subcatchment-id"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)



###########################
### 2 get-snapped-point ###
###########################
name = "get-snapped-point"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


####################################
### 2 get-snapped-point          ###
### Special case: Outside Europe ###
####################################
## Outside europe, so we expect an error!
name = "get-snapped-point"
print('\n##### Calling %s... #####' % name)
print('Calling the same process, but coordinates outside Europe! We expect it to fail.')
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "72.5",
        "lat": "83.5",
        "comment": "outside europe"
    }
}
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 400: # expecting error
    print('Error, as expected:')
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


#################################
### 2b get-snapped-point-plus ###
#################################
name = "get-snapped-point-plus"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


############################
### 3 get-stream-segment ###
############################
name = "get-stream-segment"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


##################################
### 3b get-stream-segment-plus ###
##################################
name = "get-stream-segment-plus"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)




#######################
#######################
### Routing queries ###
#######################
#######################


##################################
### 4 get-shortest-path-to-sea ### TODO rename file get_downstream_stream_segments.py
##################################
name = 'get-shortest-path-to-sea'
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


###########################
### 5 get-shortest-path ### TODO rename file get_dijkstra_stream_segments.py
###########################
name = 'get-shortest-path'
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon_start": "9.937520027160646",
        "lat_start": "54.69422745526058",
        "lon_end": "9.9217",
        "lat_end": "54.6917"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)

########################
########################
### Upstream queries ###
########################
########################



##############################
### 6 get-upstream-subcids ###
##############################
name = "get-upstream-catchment-ids"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


##########################################
### 6 get-upstream-subcids             ###
### Special case: Test passing subc_id ###
##########################################
name = "get-upstream-catchment-ids"
print('\n##### Calling %s... #####' % name)
print('Input: subc_id this time, not lon lat!')
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "subc_id": "506245899",
        "comment": "located in nordfriesland"
        #"subc_id": "553495421",
        #"comment": "located in vantaanjoki area, finland"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


##################################
### 6 get-upstream-subcids     ###
### Special case: Exceed max   ###
### num of upstream catchments ###
##################################
name = "get-upstream-catchment-ids"
print('\n##### Calling %s... #####' % name)
print('This one should not pass, because we restrict to 200 upstream catchments...')
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.109039306640627",
        "lat": "52.7810591224723",
        "comment": "this has 403 upstream catchments"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 400: # expecting error
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


######################################
### 7 get-upstream-stream-segments ###
######################################
name = "get-upstream-stream-segments"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


###############################
### 8 get-upstream-polygons ###
###############################
name = "get-upstream-polygons"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


###########################
### 9 get-upstream-bbox ###
###########################
name = "get-upstream-bbox"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


######################################
### 10 get-upstream-dissolved-cont ###
######################################
name = "get-upstream-dissolved-cont"
print('\n##### Calling %s... #####' % name)
print('Asking for default, which is "value"')
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area",
        "get_type": "Feature"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)



###################
### Finally ... ###
###################
print('\nDone!')

