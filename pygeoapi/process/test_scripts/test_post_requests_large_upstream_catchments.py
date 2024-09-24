import requests
import sys
import datetime

'''
This is just a little script to test the time our pygeoapi server takes
to compute upstream catchments of large sizes.

Check the repository here:
https://github.com/glowabio/geofresh
https://glowabio.github.io/hydrographr/

Merret Buurman (IGB Berlin), 2024-09-24
'''


base_url = 'https://xxx.xxx/pygeoapi'
headers  = {'Content-Type': 'application/json'}
#headers = {'Content-Type': 'application/json', 'Prefer': 'respond-async'}


# Get started...
session = requests.Session()
intermediate_result = None





################################
### get-upstream-subcids     ###
### 5000 upstream catchments ###
################################
name = "get-upstream-catchment-ids"
print('\n##### Calling %s... #####' % name)
print('This one should not pass, because we restrict to 200 upstream catchments...')
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "subc_id": "553704475",
        "comment": "this has 5000 upstream catchments"
    }
}
print('\nSynchronous %s...' % name)
print('START      %s' % datetime.datetime.now())
resp = session.post(url, headers=headers, json=inputs)
print('GOT RESULT %s' % datetime.datetime.now())
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


###################################
### get-upstream-dissolved-cont ###
### 5000 upstream catchments    ###
###################################
name = "get-upstream-dissolved-cont"
print('\n##### Calling %s... #####' % name)
print('Asking for "reference"')
url = base_url+'/processes/%s/execution' % name
inputs = { 
    "inputs": {
        "subc_id": "553704475",
        "comment": "this has 5000 upstream catchments"
    },
    "outputs": {
        "polygon": {"transmissionMode": "reference"}
    }
}
print('\nSynchronous %s...' % name)
print('START      %s' % datetime.datetime.now())
resp = session.post(url, headers=headers, json=inputs)
print('GOT RESULT %s' % datetime.datetime.now())
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    try:
        print('Response content: %s' % resp.json())
    except requests.exceptions.JSONDecodeError as e:
        print('Response content (not JSON) %s' % resp.content)
    print('Failed. Stopping...')
    sys.exit(1)


###################
### Finally ... ###
###################
print('\nDone!')

