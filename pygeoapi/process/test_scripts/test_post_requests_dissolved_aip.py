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

##################################
### get-upstream-dissolved-aip ###
##################################
# Requesting URL
name = "get-upstream-dissolved"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area",
        "get_type": "Feature"
    }
}

print('\nSynchronous, reference (default) %s...' % name)
print('URL %s' % url)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('---> HTTP %s <--------------------------------' % resp.status_code)
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


# Requesting GeoJSON Feature:
print('\nSynchronous, value (via input "get_json_directly") %s...' % name)
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area",
        "get_type": "Feature",
        "get_json_directly": "true"
    }
}
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    if not resp.json()['type'] == "Feature":
        print('Response is not a Feature!')
        sys.exit(1)
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


# Requesting GeoJSON Polygon:
print('\nSynchronous, value (via input "get_json_directly") %s...' % name)
inputs = { 
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "located in schlei area",
        "get_type": "Polygon",
        "get_json_directly": "true"
    }
}
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    if not resp.json()['type'] == "Polygon":
        print('Response is not a Polygon!')
        sys.exit(1)
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


###################
### Finally ... ###
###################
print('\nDone!')

