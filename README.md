# pygeoapi

[![DOI](https://zenodo.org/badge/121585259.svg)](https://zenodo.org/badge/latestdoi/121585259)
[![Build](https://github.com/geopython/pygeoapi/actions/workflows/main.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/main.yml)
[![Docker](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml)
[![Vulnerabilities](https://github.com/geopython/pygeoapi/actions/workflows/vulnerabilities.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/vulnerabilities.yml)

[pygeoapi](https://pygeoapi.io) is a Python server implementation of the [OGC API](https://ogcapi.ogc.org) suite of standards. The project emerged as part of the next generation OGC API efforts in 2018 and provides the capability for organizations to deploy a RESTful OGC API endpoint using OpenAPI, GeoJSON, and HTML. pygeoapi is [open source](https://opensource.org/) and released under an [MIT license](https://github.com/geopython/pygeoapi/blob/master/LICENSE.md).

Please read the docs at [https://docs.pygeoapi.io](https://docs.pygeoapi.io) for more information.


## AquaINFRA instance

This explains the specifics of installing the AquaINFRA pygeoapi instance, in quite some details. The general steps are:

* Install pygeoapi according to pygeoapi's official docs.
* Make it able to operate asynchronous (we use the TinyDB manager)
* Make sure you follow the advice about running in production, which includes:
 * Running pygeoapi using a proper webserver (we use gunicorn/starlette/uvicorn)
 * Running pygeoapi behind a reverse proxy (we use nginx)
 * Adding TLS/SSL support (we use nginx, it does SSL termination for us)


### Install pygeoapi from GitHub

Basically, you install and run pygeoapi following their official documentation at https://pygeoapi.io/, but instead of user their master branch, use this repo's `aquainfra_ci` branch. Here are the modified steps, with some aquainfra specifics.

* Clone the pygeoapi git repo into some base directory:

```
cd /opt
sudo mkdir pyg_aquainfra
sudo chown ubuntu:ubuntu /opt/pyg_aquainfra # TODO: eventually don't run as ubuntu:ubuntu
cd pyg_aquainfra
git clone https://github.com/geopython/pygeoapi.git

```

* To add the AquaINFRA-related stuff from this repo, add this repo as a remote:
 * At the time of you cloning this, this branch may be outdated in comparison to pygeoapi's official master, as pygeoapi develops quite quickly!
 * In this case, you are welcome to merge their newest developments into aquainfra_ci (or even rebase aquainfra_ci onto their master, if you know what you're doing).
 * However, obviously, if they have change diverged too much, both merging or rebasing might break the functionality of the AquaINFRA stuff.

```
cd /opt/pyg_aquainfra/pygeoapi
git remote add aquainfra https://github.com/AquaINFRA/pygeoapi.git
git fetch aquainfra
git checkout -b aquainfra_ci aquainfra/aquainfra_ci
```

* Install all python dependencies into a virtualenv located in `/opt/pyg_aquainfra/venv3`:

```
# make virtualenv:
cd /opt/pyg_aquainfra/
python3 -m venv venv3
source venv3/bin/activate

# install dependencies:
cd /opt/pyg_aquainfra/pygeoapi
pip3 install -r requirements.txt
```

* Modify `pygeoapi-config.yml`:
 * Change the line `url: http://localhost:5000` to whatever your IP or URL is. Port 5000 is fine.
 * Change log level to DEBUG, if you like.
 * Note that asynchronous operation is enabled by default in the AquaINFRA branch, by having uncommenting. If you set up from scratch, make sure to uncomment this section to enable asynchronous operations, and put a path to a directory/database file (pygeoapi / the user that runs pygeoapi needs write permissions for that directory:

```
    manager:
        name: TinyDB
        connection: /opt/processdb/pygeoapi-process-manager.db
        output_dir: /opt/processdb
```

** Note that pygeoapi requires environment variables WIPPPPP


* Modify `starlette_app.py` and `flask_app.py` (in `/opt/pyg_aquainfra/pygeoapi$/pygeoapi/`):
 * Correct the paths with `/xyz/` in the logging config
 * Correct the paths with `/xyz/` in the environmental variable definition, i.e. `os.environ['PYGEOAPI_CONFIG']` and `os.environ['PYGEOAPI_OPENAPI']` (the latter has to be before setup.py install)  (TODO or commit with functioning path?)(but without all the other configs...)


* Now install the actual pygeoapi module (note: if you want to actively develop on this server, consider enabling hot-reloading, see https://docs.pygeoapi.io/en/stable/running.html#hot-reloading)

```
cd /opt/pyg_aquainfra/pygeoapi
python3 setup.py install # or, for hot-reloading: pip3 install -e .
# this may throw some errors, solve them all...
```

* Create the `pygeoapi-openapi.yml`:

```
cd /opt/pyg_aquainfra/pygeoapi
export PYGEOAPI_CONFIG=pygeoapi-config.yml
export PYGEOAPI_OPENAPI=pygeoapi-openapi.yml
pygeoapi openapi generate $PYGEOAPI_CONFIG --output-file $PYGEOAPI_OPENAPI
```

* And create the database where the jobs database will be created:

```
sudo mkdir /opt/processdb
sudo chown ubuntu:ubuntu /opt/processdb/
```


* Now try running in debug mode for the first time:

```
pygeoapi serve
# this is the same as: pygeoapi serve --flask
```

* It should be available on port 5000 (at least on localhost), so in another session, try: `curl localhost:5000`. Also try `curl localhost:5000/jobs`
* Try a first Hello-World process with: `curl -X POST localhost:5000/processes/hello-world/execution --header "Content-Type: application/json" --data '{"inputs": {"name": "Miss Piggy", "message": "Oink!"}}'`
* Try asynchronous: `curl -i -X POST localhost:5000/processes/hello-world/execution --header "Content-Type: application/json" -H "Prefer: respond-async" --data '{"inputs": {"name": "Miss Piggy", "message": "Oink!"}}'`

* See logs here: `tail -f /opt/pyg_aquainfra/flask-error-pygeoapi.log` and `tail -f /opt/pyg_aquainfra/flask-debug-pygeoapi.log`

 TODO are thos logs only flask?
TODO:
Also entweder alles rein (requirements, os.environ, plugin.py, pygeoapi-config.yml, oder nix davon! So ist das ein Mix der ist Mist)

### Run pygeoapi via Starlette

Pygeoapi as a python application needs a layer that translates HTTP requests to something Python can handle. The interface between HTTP requests and Python applications is called WSGI, or ASGI for the asynchronous version. So the python application needs to implement the WSGI/ASGI interface. It is not necessary to reinvent the wheel: There are libraries who do this already, which pygeoapi can use (e.g. inherit from their base classes, add custom code). Some of these libraries/frameworks are Flask, Django, Bottle, Starlette. Both of these are supported in pygeoapi, there is a `flask_app.py` and a `starlette_app.py`.

When you run pygeoapi in the simplest setup (above), it uses Flask by default. For asynchronous usage, Starlette is recommended: "Starlette has built-in support for asynchronous operations using Python's async/await syntax, making it more suitable for high-performance asynchronous applications compared to Flask." (https://stackshare.io/stackups/flask-vs-starlette).


* Install the Starlette requirements

```
pip3 install -r requirements-starlette.txt
python setup.py install # maybe not needed if you enabled hot-loading? unsure!
```

* Try running with starlette

```
pygeoapi serve --starlette
```

* Try the same requests as above
* Add log config json: `vi /opt/pyg_aquainfra/pygeoapi/logconfig.json` with this content:

```
{
    "version": 1,
    "formatters": {
        "simple": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "loggers": {
        "filelock": { "level": "ERROR" },
        "pygeoapi.l10n": { "level": "WARNING" },
        "pygeoapi.util": { "level": "INFO" },
        "asyncio": { "level": "INFO" }
    },
    "handlers": {
        "debughandler": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "filename": "/opt/pyg_aquainfra/pygeoapi-debug.log",
            "maxBytes": 10485760,
            "backupCount": 40,
            "encoding": "utf8",
            "formatter": "simple"
        },
        "errhandler": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "WARNING",
            "filename": "/opt/pyg_aquainfra/pygeoapi-warn.log",
            "maxBytes": 10485760,
            "backupCount": 40,
            "encoding": "utf8",
            "formatter": "simple"
        }
    },
    "root": {
        "level": "DEBUG",
        "handlers": [ "debughandler", "errhandler" ]
    }
}
```

* Adapt the starlette app to add the log config: `vi /opt/pyg_aquainfra/pygeoapi/pygeoapi/starlette_app.py`, you should have a line `log_config='/opt/pyg_aquainfra/pygeoapi/logconfig.json',`...
* Re-install: `python setup.py install`
* Restart: `pygeoapi serve --starlette`
* Try the same requests as above
* Check the logs: `tail -f /opt/pyg_aquainfra/starlette.log`


### Gunicorn

The pygeoapi docs state that "Running pygeoapi serve in production is not recommended or advisable" (https://docs.pygeoapi.io/en/latest/running.html#running-in-production), so we pick one of their recommended setups: Using the gunicorn webserver together with Starlette.


* Install dependencies:

```
pip3 install gunicorn
pip3 install uvicorn
```

* Try running on gunicorn:

```
gunicorn pygeoapi.starlette_app:APP -w 4 -k uvicorn.workers.UvicornH11Worker
```

* Try the same requests as above, but gunicorn now listens on port 8000, so try `curl localhost:8000/jobs` (the port can be specified using `-b`, see gunicorn docs at https://docs.gunicorn.org/en/stable/settings.html)


### Adding a systemd service file

Here, we create a service file for running pygeoapi via systemd. This way, we don't need to start 


* Create a file `/etc/systemd/system/pygeoapi.service` with this content:

```
[Unit]
Description=Gunicorn instance to serve pygeoapi
After=network.target

[Service]
User=ubuntu
Group=ubuntu
WorkingDirectory=/opt/pyg_aquainfra
Environment="PATH=/opt/pyg_aquainfra/venv3/bin"

# Link to existing log-config JSON file:
ExecStart=/opt/pyg_aquainfra/venv3/bin/gunicorn pygeoapi.starlette_app:APP --log-config-json /opt/pyg_aquainfra/pygeoapi/logconfig.json -w 4 -k uvicorn.workers.UvicornH11Worker

# (Or specify log files directly, if desired:)
#ExecStart=/opt/pyg_aquainfra/venv3/bin/gunicorn pygeoapi.starlette_app:APP --log-level DEBUG --capture-output --access-logfile /opt/pyg_aquainfra/gunicorn-access.log --error-logfile /opt/pyg_aquainfra/gunicorn-error.log -w 4 -k uvicorn.workers.UvicornH11Worker

[Install]
WantedBy=multi-user.target
```

* Load and start:

```
sudo systemctl daemon-reload
sudo systemctl start pygeoapi
```

* Check if it worked:

```
sudo systemctl status pygeoapi
```

* Check the logs:

```
sudo tail -f /opt/pyg_aquainfra/pygeoapi/starlette.log
```

* Also enable it, so it will be restarted automatically after a reboot:

```
sudo systemctl is-enabled pygeoapi
sudo systemctl enable pygeoapi
sudo systemctl is-enabled pygeoapi
```

### Asynchronous

* Note that asynchronous operation is enabled by default in the AquaINFRA branch, in `pygeoapi-config.yml`.
* If you set up from scratch, make sure to uncomment this section to enable asynchronous operations, and put a path to a directory/database file (pygeoapi / the user that runs pygeoapi needs write permissions for that directory:

```
    manager:
        name: TinyDB
        connection: /opt/processdb/pygeoapi-process-manager.db
        output_dir: /opt/processdb
```

### web server, TLS / SSL

* Install nginx as a reverse proxy that also does the TLS / SSL termination
* Enable nginx to serve the result files as static content


### Install and configure webserver (nginx)

* Install nginx according to nginx documentation (e.g. `apt install nginx`)
* Verify it is running: `systemctl status nginx`
* Verify you can see the welcome page by `curl http://<your-ip>`
* Verify whether the logs are located at `/var/log/nginx/error.log` and `/var/log/nginx/access.log`.


### Configure nginx to serve static files

* Create dir

```
cd /var/www/html/
sudo mkdir -p nginx/download
sudo vi /var/www/nginx/download/hello.txt # write some text into it, or some html
cat /var/www/nginx/download/hello.txt  # Hello nginx!
```

* Change root dir in config: `sudo vi /etc/nginx/sites-enabled/default`

```
# backup:
sudo cp /etc/nginx/sites-enabled/default /etc/nginx/backup-default-config
# now modify:
sudo vi /etc/nginx/sites-enabled/default
# ...
# restart:
sudo systemctl restart nginx
```

* Try it: `curl http://<your-ip>/download/hello.txt`

### Configure nginx as a reverse proxy for pygeoapi (proxy_pass)

Now we add nginx as a reverse proxy to our pygeoapi instance, which is running behind gunicorn locally on `127.0.0.1:8000`. So far, it is accessible only via this URL, i.e. only from the VM itself. With the nginx reverse proxy, we can access nginx from outside (via `http://<your-ip>/...`), and it passes the requests on to gunicorn/pygeoapi.

* On which port is pygeoapi running? Likely 8000 (gunicorn default).
* Open nginx config:

```
sudo vi /etc/nginx/sites-enabled/default
```

* Add this part inside the `server {` bracket. For example, above the line `location / {` - this makes it faster, as all `location` config snippets are evaluated one after another, so putting this one first ensures all requests to pygeoapi do not have to go through the other `location` config snippets.

```
         location ^~ /pygeoapi/ {
             include proxy_params;
             proxy_pass http://127.0.0.1:8000/;
             proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
             proxy_set_header X-Forwarded-Proto $scheme;
             proxy_set_header X-Forwarded-Host $host;
             proxy_set_header X-Forwarded-Prefix /;
         }

```

* Note: It is important to have the slash after the local address in the `proxy_pass` line (`http://127.0.0.1:8000/`)! Not adding the slash changes the behaviour.
* Now try whether the proxying work! You should now be able to access pygeoapi through nginx, by adding `/pygeoapi`:

```
curl http://86.50.231.227/pygeoapi/processes?f=json
```

* Now try whether you can also send `http POST` requests via the reverse proxy:

```
curl -X POST http://<your-ip>/pygeoapi/processes/hello-world/execution --header "Content-Type: application/json" --data '{"inputs": {"name": "Miss Piggy", "message": "Oink!"}}'
```


### Configure TLS on nginx

So far, we can only access nginx and pygeoapi via `http`, but we obviously want `https`.

As nginx is our reverse proxy in front of gunicorn/pygeoapi, we can let it do the SSL termination, so that we don't need to configure gunicorn/pygeoapi to handle SSL. Nginx will receive the `https` requests, handle the SSL stuff, and pass simple `http` requests on to gunicorn/pygeoapi.


```
... TODO TLS/SSL...
```

### Where to put this stuff... (TODO)

* Which uid/gid to run pygeoapi and nginx in:
 * Run pygeoapi as the user `pyguser`, group `www-data` - the latter allows to share files with nginx who runs as `www-data`!
* Templates for unix service files:
 * A template for the unit file `pygeoapi.service` can be found in the folder `deployment` (based on running pygeoapi via `gunicorn`)
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

* Processes that sit in a git repo: Go to `/opt/pyg_aquainfra/pygeoapi/pygeoapi/process/` and clone the git repo there.
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

