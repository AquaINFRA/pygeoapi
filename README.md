# pygeoapi

[![DOI](https://zenodo.org/badge/121585259.svg)](https://zenodo.org/badge/latestdoi/121585259)
[![Build](https://github.com/geopython/pygeoapi/actions/workflows/main.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/main.yml)
[![Docker](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml)
[![Vulnerabilities](https://github.com/geopython/pygeoapi/actions/workflows/vulnerabilities.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/vulnerabilities.yml)

[pygeoapi](https://pygeoapi.io) is a Python server implementation of the [OGC API](https://ogcapi.ogc.org) suite of standards. The project emerged as part of the next generation OGC API efforts in 2018 and provides the capability for organizations to deploy a RESTful OGC API endpoint using OpenAPI, GeoJSON, and HTML. pygeoapi is [open source](https://opensource.org/) and released under an [MIT license](https://github.com/geopython/pygeoapi/blob/master/LICENSE.md).

Please read the docs at [https://docs.pygeoapi.io](https://docs.pygeoapi.io) for more information.


## AquaINFRA instance

This explains the specifics of installing the AquaINFRA pygeoapi instance.

* Clone the pygeoapi git repo into some base directory:

```
cd /opt
sudo mkdir pyg_aquainfra
cd pyg_aquainfra
git clone https://github.com/geopython/pygeoapi.git

```

* Now install and run pygeoapi following their official documentation at https://pygeoapi.io/
* Make sure to install all python dependencies into a virtualenv located in `/opt/pyg_aquainfra/venv`
* Install nginx as a reverse proxy that also does the TLS / SSL termination
* To add the AquaINFRA-related stuff from this repo, add this repo as a remote:

```
cd /opt/pyg_aquainfra/pygeoapi
git remote add aquainfra git@github.com:AquaINFRA/pygeoapi.git
git checkout aquainfra_ci
# At the time of you cloning this, this branch may be outdated in comparison to pygeoapi's official master, as pygeoapi develops quite quickly!
# In this case, you are welcome to merge their newest developments into aquainfra_ci (or even rebase aquainfra_ci onto their master, if you know what you're doing).
# However, obviously, if they have change diverged too much, both merging or rebasing might break the functionality of the AquaINFRA stuff.
```

* Enable nginx to serve the result files as static content
* How to make pygeoapi asynchronous and which server to run this in (gunicorn, uvicorn, flask, starlette ---)
* How to set required environment variables
* Which uid/gid to run pygeoapi and nginx in:
** Run pygeoapi as the user `pyguser`, group `www-data` - the latter allows to share files with nginx who runs as `www-data`!
* Templates for unix service files:
** A template for the unit file `pygeoapi.service` can be found in the folder `deployment` (based on running pygeoapi via `gunicorn`)
* How to setup logging
* Styling, logos, favicon and contact info
* Testing and monitoring
* Sandbox and productive instance
* Test frontend with javascript client
* And last but not least, add the proper processes and process descriptions from their own repositories, together with process-specific config etc.
* Note: Overall log level for pygeoapi is in `pygeoapi-config.yml`!

### Asynchronous

How to make pygeoapi asynchronous and which server to run this in (gunicorn, uvicorn, flask, starlette ---)

This section is work in progress!!

Don't forget:

* dev and prod need different TinyDB files!




### How to add a process or a set of processes

* Processes that sit in a git repo: Go to `/.../pygeoapi/pygeoapi/process/` and clone the git repo there
* Individual processes: Put the python and json files into `/.../pygeoapi/pygeoapi/process/`
* If you need specific environment variables, add them to the Flask and/or Starlette apps, close to this line `os.environ['PYGEOAPI_CONFIG'] = '/xyz/pygeoapi/pygeoapi-config.yml'`
* For each process, add a line to `/.../pygeoapi/pygeoapi/plugin.py`
* For each process, add a line line to `/.../pygeoapi/pygeoapi-config.yml`
* To reflect those additions in the API file, run:

```
source /.../venv/bin/activate
export PYGEOAPI_CONFIG=pygeoapi-config.yml
export PYGEOAPI_OPENAPI=pygeoapi-openapi.yml
pygeoapi openapi generate $PYGEOAPI_CONFIG --output-file $PYGEOAPI_OPENAPI
```

If there are new dependencies:

* Add them to `/.../pygeoapi/requirements.txt`
* Then run:

```
source /.../venv/bin/activate
which pip3
pip3 install -r /.../pygeoapi/requirements.txt
```


* Finally, restart the service: `sudo systemctl restart pygeoapi`





## AquaINFRA instance: Freshwater Metadatabase Catalogue


```
 resources:
    fmdb_catalogue:
        type: collection
        title: Freshwater Metadatabase catalogue
        description: The Freshwater Metadatabase was built as part of the EU BioFresh project to centralise information on freshwater related datasets. It collects various characteristics describing a dataset (e.g. data provider, access and intellectual property rights, regional coverage and spatial extent, environmental and climate related parameter) and allows querying this information.
        keywords:
            - freshwater
            - catalogue
        links:
            - type: text/html
              rel: canonical
              title: information
              href: http://www.freshwatermetadata.eu/metadb/
              hreflang: en-US
        extents:
            spatial:
                bbox: [-180,-90,180,90]
                crs: http://www.opengis.net/def/crs/OGC/1.3/CRS84
        providers:
            - type: record
              name: TinyDBCatalogue
              data: /.../metadata/fmdb_20240708.tinydb
              id_field: id
              time_field: created
              title_field: title
```

