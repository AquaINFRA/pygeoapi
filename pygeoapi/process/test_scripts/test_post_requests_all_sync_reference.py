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
1  get_local_subcatchment_ids.py
2  get_snapped_points.py
2b get_snapped_points_plus.py
3  get_local_stream_segments.py
3b get_local_streamsegments_subcatchments.py

Routing:
4  get_shortest_path_to_outlet.py
5  get_shortest_path_two_points.py

Upstream catchment:
6  get_upstream_subcids.py
7  get_upstream_stream_segments.py
8  get_upstream_subcatchments.py
9  get_upstream_bbox.py
10 get_upstream_dissolved.py

'''


#####################
#####################
### Local queries ###
#####################
#####################

####################################
### 1 get-local-subcatchment-ids ###
####################################
name = "get-local-subcatchment-ids"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)



############################
### 2 get-snapped-points ###
############################
name = "get-snapped-points"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area",
        "geometry_only": "false"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
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
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 400: # expecting error
    print('Error, as expected:')
    print('Response content: %s' % resp.json())
    #print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
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
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


###################################
### 3 get-local-stream-segments ###
###################################
name = "get-local-stream-segments"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area",
        "geometry_only": "false"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


#################################################
### 3b get-local-streamsegments-subcatchments ###
#################################################
name = "get-local-streamsegments-subcatchments"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area",
        "geometry_only": "false"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
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


#####################################
### 4 get-shortest-path-to-outlet ###
#####################################
name = 'get-shortest-path-to-outlet'
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area",
        "geometry_only": "false",
        "add_segment_ids": "true"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


######################################
### 5 get-shortest-path-two-points ###
######################################
name = 'get-shortest-path-two-points'
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon_start": "9.937520027160646",
        "lat_start": "54.69422745526058",
        "lon_end": "9.9217",
        "lat_end": "54.6917",
        "comment": "test bla",
        "geometry_only": "false",
        "add_segment_ids": "true"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
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
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
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
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
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
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 400: # expecting error
    print('Response content: %s' % resp.json())
    #print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
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
        "add_upstream_ids": "true",
        "comment": "located in schlei area",
        "geometry_only": "false"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


####################################
### 8 get-upstream-subcatchments ###
####################################
name = "get-upstream-subcatchments"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "add_upstream_ids": "true",
        "comment": "located in schlei area",
        "geometry_only": "false"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
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
        "add_upstream_ids": "true",
        "comment": "located in schlei area",
        "geometry_only": "false"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
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
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area",
        "geometry_only": "false",
        "add_upstream_ids": "true"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)

######################################
### 10 get-upstream-dissolved-cont ###
### Requesting reference, OGC compliant... ###
######################################
name = "get-upstream-dissolved-cont"
print('\n##### Calling %s... #####' % name)
print('Asking for reference..."')
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area",
        "get_type": "Feature"
    },
    "outputs": {
        "polygon": {
            "transmissionMode": "reference" # This is OGC compliant, but you need to know the output's name...
        }
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('Link content    : %s...' % session.get(resp.json()['href']).content[:200])
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)



###################
### Finally ... ###
###################
print('\nDone!')

