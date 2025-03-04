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
git clone https://github.com/geopython/pygeoapi.git # TODO: Maybe better clone via ssh not https?

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
 * Change the `url` setting to whatever your IP or URL is. (This has to be the URL, port and possibly path where the service will be accessible from outside, so this must match your reverse proxy settings!) For testing, you can start with localhost and port 5000.
 * Change log level, if you like.
 * Note that asynchronous operation is enabled by default in the AquaINFRA branch, by having uncommenting. If you set up from scratch, make sure to uncomment this section to enable asynchronous operations, and put a path to a directory/database file (pygeoapi / the user that runs pygeoapi needs write permissions for that directory:

```
    manager:
        name: TinyDB
        connection: /opt/processdb/pygeoapi-process-manager.db
        output_dir: /opt/processdb
```

* Two notes:
 * Pygeoapi needs the environmental variables `PYGEOAPI_CONFIG` and `PYGEOAPI_OPENAPI` to function. They must indicate the location of the `pygeoapi-config.yml` file, and where it should place the `pygeoapi-openapi.yml` file that it generates. They are set in `starlette_app.py` and `flask_app.py` and should be fine by default.
 * Our instance needs a few environmental variables indicating the location of process-specific config files. They are set in `starlette_app.py` and `flask_app.py`, so you can adapt them there if you want, but they should be fine by default.


* Now install the actual pygeoapi module (note: if you want to actively develop on this server, consider enabling hot-reloading, see https://docs.pygeoapi.io/en/stable/running.html#hot-reloading)

```
cd /opt/pyg_aquainfra/pygeoapi
python3 setup.py install # or, for hot-reloading: pip3 install -e .
# this may throw some errors, solve them all...
```

* Let pygeoapi generate the `pygeoapi-openapi.yml`:

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

### First attempt

* Now try running in debug mode for the first time:

```
pygeoapi serve
# this is the same as: pygeoapi serve --flask
```

* It should be available on port 5000 (at least on localhost), so in another session, try: `curl localhost:5000`. Also try `curl localhost:5000/jobs`
* Try a first Hello-World process with: `curl -X POST localhost:5000/processes/hello-world/execution --header "Content-Type: application/json" --data '{"inputs": {"name": "Miss Piggy", "message": "Oink!"}}'`
* Try asynchronous: `curl -i -X POST localhost:5000/processes/hello-world/execution --header "Content-Type: application/json" -H "Prefer: respond-async" --data '{"inputs": {"name": "Miss Piggy", "message": "Oink!"}}'`
* See logs here: `tail -f /opt/pyg_aquainfra/flask-error-pygeoapi.log` and `tail -f /opt/pyg_aquainfra/flask-debug-pygeoapi.log` (these paths are specified in `flask_app.py`)
* This now runs via flask, but only a test setup, as we have not added a webserver and/or reverse proxy yet, so please continue with the next steps...



### Run pygeoapi via Starlette

Pygeoapi as a python application needs a layer that translates HTTP requests to something Python can handle. The interface between HTTP requests and Python applications is called WSGI, or ASGI for the asynchronous version. So the python application needs to implement the WSGI/ASGI interface. It is not necessary to reinvent the wheel: There are libraries who do this already, which pygeoapi can use (e.g. inherit from their base classes, add custom code). Some of these libraries/frameworks are Flask, Django, Bottle, Starlette. Both of these are supported in pygeoapi, there is a `flask_app.py` and a `starlette_app.py`.

When you run pygeoapi in the simplest setup (above), it uses Flask by default. For asynchronous usage, Starlette is recommended: "Starlette has built-in support for asynchronous operations using Python's async/await syntax, making it more suitable for high-performance asynchronous applications compared to Flask." (https://stackshare.io/stackups/flask-vs-starlette).


* Install the Starlette requirements

```
pip3 install -r requirements-starlette.txt
python setup.py install # TODO: maybe not needed if you enabled hot-loading? unsure!
```

* Try running with starlette

```
pygeoapi serve --starlette
```

* The log configuration is done in a JSON file here: `/opt/pyg_aquainfra/pygeoapi/logconfig.json`. It should be fine by default.
 * The debug log can be found at `/opt/pyg_aquainfra/pygeoapi-debug.log`
 * The error log can be found at `/opt/pyg_aquainfra/pygeoapi-warn.log`
 * The starlette app points to the log config in this line: `log_config='/opt/pyg_aquainfra/pygeoapi/logconfig.json',`, in the definition of `serve(ctx, ...)`.

* For testing, try the same requests as above
* This now runs via starlette, but it is not our final setting yet, as we have not added a webserver and/or reverse proxy yet, so please continue with the next steps...


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


* Create a file `/etc/systemd/system/pygeoapi.service` with this content (see file `deployment/pygeoapi.service`):

```
[Unit]
Description=Gunicorn instance to serve pygeoapi
After=network.target

[Service]
User=ubuntu   # TODO: pyguser
Group=ubuntu  # TODO: www-data
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
sudo tail -f /opt/pyg_aquainfra/pygeoapi-debug.log
```

* Also enable it, so it will be restarted automatically after a reboot:

```
sudo systemctl is-enabled pygeoapi
sudo systemctl enable pygeoapi
sudo systemctl is-enabled pygeoapi
```

### Asynchronous

* Note that asynchronous operation is enabled by default in the AquaINFRA branch by default, in `pygeoapi-config.yml`.
* If you set up pygeoapi from scratch, make sure to uncomment this section to enable asynchronous operations, and put a path to a directory/database file (pygeoapi, i.e. the user that runs pygeoapi, needs write permissions for that directory:


```
    manager:
        name: TinyDB
        connection: /opt/processdb/pygeoapi-process-manager.db
        output_dir: /opt/processdb
```

* **Important:** If you have two instances on the same machine (e.g. dev and prod), need different TinyDB files, otherwise it messes everything up! In that case you must change the above setting in at least one of the instances!


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

* On which port is pygeoapi running? Likely 8000 (gunicorn default). (TODO: Run on unix socket, instead of 8000 - more secure?)
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

* Which uid/gid to run pygeoapi and nginx in: Run pygeoapi as the user `pyguser`, group `www-data` - the latter allows to share files with nginx who runs as `www-data`!
* Styling, logos, favicon and contact info
* Testing and monitoring
* Sandbox and productive instance
* Test frontend with javascript client
* And last but not least, add the proper processes and process descriptions from their own repositories, together with process-specific config etc.


### How to add a process or a set of processes

* Processes that sit in a git repo: Go to `/opt/pyg_aquainfra/pygeoapi/pygeoapi/process/` and clone the git repo there.
* If you need specific environment variables, add them to the Flask and/or Starlette apps, close to this line `os.environ['PYGEOAPI_CONFIG'] = '/opt/pyg_aquainfra/pygeoapi/pygeoapi-config.yml'`
* For each process, add a line to `/opt/pyg_aquainfra/pygeoapi/pygeoapi/plugin.py`
* For each process, add a line line to `/opt/pyg_aquainfra/pygeoapi/pygeoapi-config.yml`
* To reflect those additions in the API file, regenerate it as follows:

```
source /opt/pyg_aquainfra/venv/bin/activate
export PYGEOAPI_CONFIG=pygeoapi-config.yml
export PYGEOAPI_OPENAPI=pygeoapi-openapi.yml
pygeoapi openapi generate $PYGEOAPI_CONFIG --output-file $PYGEOAPI_OPENAPI
```

If there are new dependencies:

* Add them to `/opt/pyg_aquainfra/pygeoapi/requirements.txt`
* Then run:

```
source /opt/pyg_aquainfra/venv3/bin/activate
which pip3
pip3 install -r /opt/pyg_aquainfra/pygeoapi/requirements.txt
```

* Finally, restart the service: `sudo systemctl restart pygeoapi`


## Containerized processes

### Preparing docker

* First, install docker using the official Docker documentation: https://docs.docker.com/engine/install/ubuntu/
* Test: `date; sudo docker run hello-world`
* Add your user to the docker group, in order to build the image without using sudo:

```
sudo groupadd docker
sudo usermod -aG docker $USER
# Log out and in again for changes to take effect
```

* Test without sudo:

```
date; docker run hello-world
```

* Add the proper docker executable to config:
 * Which executable of docker is used? `which docker` (probably something like: `/usr/bin/docker`)
 * Add it to config (`/opt/pyg_aquainfra/pygeoapi/config.json`), like this: `"docker_executable": "/usr/bin/docker",`
 * (In the processes, there should be some line that picks up the config setting and uses the provided path, e.g. `docker_executable = configJSON.get("docker_executable", "docker")` )



### How to deploy a containized service


* Go to processes dir: `cd /opt/pyg_aquainfra/pygeoapi/pygeoapi/process/`
* Clone the repo containing the process, go into the dir and checkout the right branch:

```
git clone https://github.com/AstraLabuce/aquainfra-usecase-Daugava.git
cd aquainfra-usecase-Daugava/
git checkout --track origin/containerize
```


* Build the docker image (if you cannot pull it from some Docker hub or repo)!
 * The image name has to correspond to the name that is called in the process, so checkout the process python file that you just added to `plugin.py`. You may find the image name using grep: `cat pygeoapi/process/aquainfra-usecase-Daugava/src/ogc_api_processes/points_att_polygon.py | grep "image_name"`. In our example, it is `daugava-workflow-image`.
 * You have to be in the directory where the corresponding `Dockerfile` is located! Check: `ls -1 | grep Dockerfile`
 * Build it: `date; docker build -t daugava-workflow-image . ; date` (this may take time!)
 * Check: `docker image ls | grep  daugava-workflow-image`


* Add the process to `plugin.py` and `pygeoapi-config.py`, as above!
* Rerun install...?

```
/opt/pyg_aquainfra/pygeoapi
source venv3/bin/activate
date; pip install . # instead of deprecated: python setup.py install
# restart:
date; sudo systemctl restart pygeoapi
```

* Now... Test?





## Regular updates

TODO: How to regularly pull the pygeoapi master branch!


## AquaINFRA instance: Freshwater Metadatabase Catalogue

We are not only hosting processes on our instance, but also a (test version? of) the
Freshwater Metadatabase Catalogue.

TODO: Where should we add this documentation, and the tinydb file?


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

