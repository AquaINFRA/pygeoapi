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
# NOT COMMIT:
base_url = 'https://aqua.igb-berlin.de/pygeoapi'
base_url = 'https://aqua.igb-berlin.de/pygeoapi-dev'
headers  = {'Content-Type': 'application/json'}
#headers = {'Content-Type': 'application/json', 'Prefer': 'respond-async'}


# Get started...
session = requests.Session()



########################
### 1 subset_by_bbox ###
### Value            ###
########################
name = "get-subset-by-bbox"
print('\n##### Calling %s... (value) #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "north": 72.1,
        "south": 66.1,
        "west": 13.3,
        "east": 16.3
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s... etc.' % resp.content[:200])
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)



########################
### 1 subset_by_bbox ###
### Reference        ###
########################

name = "get-subset-by-bbox"
print('\n##### Calling %s... (reference) #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "north": 72.1,
        "south": 66.1,
        "west": 13.3,
        "east": 16.3
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


#print('Stopping...')
#import sys
#sys.exit()

###########################
### 2 subset_by_polygon ###
### Value               ###
###########################

name = "get-subset-by-polygon"
print('\n##### Calling %s... (value) #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "polygon": {"type": "Polygon", "coordinates": [ [ [ 15.081460166988848, 66.296144397828058 ], [ 13.809362140071178, 66.465757468083737 ], [ 13.809362140071178, 66.465757468083737 ], [ 13.809362140071178, 66.465757468083737 ], [ 14.948192754645092, 67.683337008133506 ], [ 15.711451570795695, 66.859502095463029 ], [ 14.493872030745925, 66.84738687615905 ], [ 15.081460166988848, 66.296144397828058 ] ] ] }
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s... etc.' % resp.content[:200])
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)



###########################
### 2 subset_by_polygon ###
### Reference           ###
###########################

name = "get-subset-by-polygon"
print('\n##### Calling %s... (reference) #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "polygon": {"type": "Polygon", "coordinates": [ [ [ 15.081460166988848, 66.296144397828058 ], [ 13.809362140071178, 66.465757468083737 ], [ 13.809362140071178, 66.465757468083737 ], [ 13.809362140071178, 66.465757468083737 ], [ 14.948192754645092, 67.683337008133506 ], [ 15.711451570795695, 66.859502095463029 ], [ 14.493872030745925, 66.84738687615905 ], [ 15.081460166988848, 66.296144397828058 ] ] ] }
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



###################
### Finally ... ###
###################
print('\nDone!')

