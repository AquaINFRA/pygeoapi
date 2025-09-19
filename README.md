# pygeoapi

[![DOI](https://zenodo.org/badge/121585259.svg)](https://zenodo.org/badge/latestdoi/121585259)
[![Build](https://github.com/geopython/pygeoapi/actions/workflows/main.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/main.yml)
[![Docker](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/containers.yml)
[![Vulnerabilities](https://github.com/geopython/pygeoapi/actions/workflows/vulnerabilities.yml/badge.svg)](https://github.com/geopython/pygeoapi/actions/workflows/vulnerabilities.yml)

[pygeoapi](https://pygeoapi.io) is a Python server implementation of the [OGC API](https://ogcapi.ogc.org) suite of standards. The project emerged as part of the next generation OGC API efforts in 2018 and provides the capability for organizations to deploy a RESTful OGC API endpoint using OpenAPI, GeoJSON, and HTML. pygeoapi is [open source](https://opensource.org/) and released under an [MIT license](https://github.com/geopython/pygeoapi/blob/master/LICENSE.md).

Please read the docs at [https://docs.pygeoapi.io](https://docs.pygeoapi.io) for more information.


## Set up the AquaINFRA instance

This explains the specifics of installing the AquaINFRA pygeoapi instance, in quite some details. The general steps are:

* Install pygeoapi according to pygeoapi's official docs
* Make it able to operate asynchronously
* Make sure you follow the advice about running in production, which includes:
   * Running pygeoapi using a proper webserver and ASGI support (we use starlette/gunicorn/uvicorn
   * Running pygeoapi behind a reverse proxy (we use nginx)
   * Adding TLS/SSL support (we use nginx, it does SSL termination for us)
* Add the AquaINFRA processes


Basically, you install and run pygeoapi following their official documentation at https://pygeoapi.io/, but instead of user their master branch, use this repo's `aquainfra_ci` branch. Here are the modified steps, with some aquainfra specifics.


* [Set up the AquaINFRA instance](#set-up-the-aquainfra-instance)
   * [Clone pygeoapi from GitHub](#clone-pygeoapi-from-github)
   * [Clone AquaINFRA-specific branch](#clone-aquainfra-specific-branch)
   * [Linux user and group](#linux-user-and-group)
   * [Configuration](#configuration)
   * [Logging](#logging) 
   * [Enable asynchronous processing](#enable-asynchronous-processing)
   * [Make virtualenv](#make-virtualenv) 
   * [Install dependencies into virtualenv](#install-dependencies-into-virtualenv) 
   * [Install pygeoapi module](#install-pygeoapi-module) 
   * [Generate pygeoapi-openapi.yml](#generate-pygeoapi-openapiyml) 
   * [Run for the first time...](#run-for-the-first-time)
   * [Add Starlette for ASGI](#add-starlette-for-asgi)
   * [Add Gunicorn (and Uvicorn) as webserver](#add-gunicorn-and-uvicorn-as-webserver)
   * [Create a systemd service file](#create-a-systemd-service-file)
* [Add nginx (reverse proxy and for SSL/TLS and static files)](#add-nginx-reverse-proxy-and-for-ssltls-and-static-files)
   * [Install and configure nginx webserver](#install-and-configure-nginx-webserver)
   * [Configure nginx to serve static files](#configure-nginx-to-serve-static-files)
   * [Configure nginx as a reverse proxy for pygeoapi (proxy_pass)](#configure-nginx-as-a-reverse-proxy-for-pygeoapi-proxy_pass)
   * [Configure TLS on nginx](#configure-tls-on-nginx)
* [How to deploy processes](#how-to-deploy-processes)
   * [General info on processes in AquaINFRA](#general-info-on-processes-in-aquainfra)
   * [Containerized processes (1/2): Prepare docker](#containerized-processes-12-prepare-docker)
   * [Containerized processes (2/2): Deploy process](#containerized-processes-22-deploy-process)
   * [How to deploy processes (not containerized)](#how-to-deploy-processes-not-containerized-)
* [TODO: Testing and Monitoring](#todo-testing-and-monitoring)
* [TODO: Regular clean up](#todo-regular-clean-up)
* [Map client](#map-client)
* [Regular updates](#regular-updates)


### Clone pygeoapi from GitHub


* Clone the pygeoapi git repo into some base directory:

```
cd /opt
sudo mkdir pyg_aquainfra
sudo chown ubuntu:ubuntu /opt/pyg_aquainfra  # TODO: Run as different user: pyguser
cd pyg_aquainfra
git clone https://github.com/geopython/pygeoapi.git # TODO: Maybe better clone via ssh not https?

```

If you want to have one "productive" and one "sandbox" instance, you need to do all this in a second directory, e.g. `dev_aquainfra`.


### Clone AquaINFRA-specific branch

To add the AquaINFRA-related stuff (e.g. landing page layout, contact info, small behavioural modifications, logging, styling, logos and favicons, ...) from this repo, add it as a remote and checkout the branch `aquainfra_ci`.

```
cd /opt/pyg_aquainfra/pygeoapi
git remote add aquainfra https://github.com/AquaINFRA/pygeoapi.git
git fetch aquainfra
git checkout -b aquainfra_ci aquainfra/aquainfra_ci
```


**Important**

At the time of cloning this repo, it **may be outdated** in comparison to pygeoapi's official master, as pygeoapi develops quite quickly! In this case, you are welcome to merge their newest developments into aquainfra_ci (or even rebase aquainfra_ci onto their master, if you know what you're doing). However, obviously, if they have change diverged too much, both merging or rebasing might break the functionality of the AquaINFRA stuff.


### Linux user and group

During testing, pygeoapi will be run as the Linux user (`uid` and `gid`) who is currently logged in and starts the service (e.g. `...`). In production, it is not advised to run any web service as such a personal Linux account, but rather to create a Linux system account for this, for security reasons.

The Linux user and group (`uid` and `gid`) who runs pygeoapi are specified in the systemd service file (see section below).

Currently, on aquarium, the processes run as user `ubuntu`, group `ubuntu`. On aqua, we set up a system user called `pyguser`. We run pygeoapi as `pyguser`, group `www-data` - the latter allows to share files with nginx who runs as `www-data`!

* TODO: How to create a system account, and which permissions/properties it needs
* TODO: Which files need to be changed to accomodate this?


### Configuration

* Make the following changes in `pygeoapi-config.yml`:
   * Change the `url` setting to whatever your IP or URL is. (This has to be the URL, port and possibly path where the service will be accessible from outside, so this must match your reverse proxy settings!) For testing, you can start with localhost and port 5000.
   * Change log level, if you like.
* The AquaINFRA instance needs a few environmental variables indicating the location of process-specific config files. They are set in `starlette_app.py` and `flask_app.py`. They should be fine by default, but you can adapt them there if necessary.
* AquaINFRA-specific config is contained in `/opt/pyg_aquainfra/pygeoapi/config.json`. Some of this will be needed by every process (e.g. directory where to store results so that the user can download them). It can also contain process-specific config, ideally in a nested way to avoid key collisions or cluttering the file. The processes can find that file by `config_file_path = os.environ.get('DAUGAVA_CONFIG_FILE', "./config.json")`. That environmental variable has to be set in the flask or starlette app, like this: `os.environ['DAUGAVA_CONFIG_FILE'] = '/opt/pyg_aquainfra/pygeoapi/config.json'`.

### Logging

In the final setup (i.e. running via starlette, etc.), the logging in configured by JSON file, which is here: `/opt/pyg_aquainfra/pygeoapi/logconfig.json`. It should be fine by default.

* The debug log can be found at `/opt/pyg_aquainfra/logs/pygeoapi-debug.log`
* The error log can be found at `/opt/pyg_aquainfra/logs/pygeoapi-warn.log`

You have to make the directory `mkdir /opt/pyg_aquainfra/logs`.

The starlette app points to the log config in this line: `log_config='/opt/pyg_aquainfra/pygeoapi/logconfig.json',`, in the definition of `serve(ctx, ...)`.


TODO: This serve(ctx) is only used in pre-testing???

During the first test after installation, which uses flask, the logs are here: `tail -f /opt/pyg_aquainfra/flask-error-pygeoapi.log` and `tail -f /opt/pyg_aquainfra/flask-debug-pygeoapi.log` (these paths are specified in `flask_app.py`).



### Enable asynchronous processing


* Create the directory where the jobs database will be created, and give the user that runs pygeoapi (e.g. `ubuntu` or `pyguser`) write permissions:

```
sudo mkdir /opt/processdb
sudo mkdir /opt/processdb/output_dir
sudo chown ubuntu:ubuntu /opt/processdb/ # TODO: Run as different user: pyguser
```

* Make the following changes in `pygeoapi-config.yml`:
   * Uncomment this section to enable asynchronous operations (already done in branch `aquainfra_ci`)
   * Put the path to a database file in the directory created above (e.g. `/opt/processdb/pygeoapi-process-manager.db`). The file will be created by pygeoapi, the directory must exist and be writeable.

```
    manager:
        name: TinyDB
        connection: /opt/processdb/pygeoapi-process-manager.db
        output_dir: /opt/processdb/output_dir
```

**Important:** If you have two instances on the same machine (e.g. dev and prod), need different TinyDB files, otherwise it messes everything up! In that case you must change the above setting in at least one of the instances!


### Make virtualenv

* All python dependencies (including pygeoapi itself) will be installed into a virtualenv located in `/opt/pyg_aquainfra/venv3`:

```
# make virtualenv:
cd /opt/pyg_aquainfra/
python3 -m venv venv3
source venv3/bin/activate
which python3
which pip3
```

### Install dependencies into virtualenv


* Install all python dependencies into the virtualenv located in `/opt/pyg_aquainfra/venv3`:

```
# open virtualenv:
source /opt/pyg_aquainfra/venv3/bin/activate
which pip3

# install dependencies:
pip3 install -r /opt/pyg_aquainfra/pygeoapi/requirements.txt
```

Every time you have added new dependencies, add them to `/opt/pyg_aquainfra/pygeoapi/requirements.txt` and then run the same lines again.

Note on **gdal:** We disabled installing gdal as a dependency. It is needed for various processes, but has caused conflicts, as different processes seem to need different versions. We hope installing it will not be needed anymore once all processes are ported to docker. However, if you do need it, install it manually via pip. Likely you will also have to install the underlying package via `date; sudo apt install libgdal-dev`



### Install pygeoapi module

Python needs to find the pygeoapi module on `PATH`, so it must be installed (in the virtual env) prior to running!

If you want to actively develop on this server, consider enabling hot-reloading, see https://docs.pygeoapi.io/en/stable/running.html#hot-reloading)

```
cd /opt/pyg_aquainfra/pygeoapi

# virtualenv:
source ../venv3/bin/activate

# install:
python3 setup.py install # or, for hot-reloading: pip3 install -e .
# this may throw some errors, solve them all...
# note: setup.py install is now deprecated! # TODO document the new best practice!
```


### Generate pygeoapi-openapi.yml


* Let pygeoapi generate the `pygeoapi-openapi.yml`:

```
cd /opt/pyg_aquainfra/pygeoapi
export PYGEOAPI_CONFIG=pygeoapi-config.yml
export PYGEOAPI_OPENAPI=pygeoapi-openapi.yml
pygeoapi openapi generate $PYGEOAPI_CONFIG --output-file $PYGEOAPI_OPENAPI
```

This needs to be redone after every change of `pygeoapi-config.yml`, I assume.


To run, pygeoapi needs to know where to find those two files (`pygeoapi-config.yml`, `pygeoapi-openapi.yml`), so it needs the two environmental variables `PYGEOAPI_CONFIG` and `PYGEOAPI_OPENAPI`. They are set in `starlette_app.py` and `flask_app.py` and should be fine by default.



### Run for the first time...

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


### Add Starlette for ASGI

Background: Pygeoapi as a python application needs a layer that translates HTTP requests to something Python can handle. The interface between HTTP requests and Python applications is called WSGI, or ASGI for the asynchronous version. So the python application needs to implement the **WSGI/ASGI interface**. It is not necessary to reinvent the wheel: There are libraries who do this already, which pygeoapi can use (e.g. inherit from their base classes, add custom code). Some of these libraries/frameworks are Flask, Django, Bottle, Starlette. Both of these are supported in pygeoapi, there is a `flask_app.py` and a `starlette_app.py`.

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

* To test, try the same curl requests as above
* As mentioned above, the log configuration is done in a JSON file here: `/opt/pyg_aquainfra/pygeoapi/logconfig.json` and should be fine by default.
   * The debug log can be found at `/opt/pyg_aquainfra/pygeoapi-debug.log`
   * The error log can be found at `/opt/pyg_aquainfra/pygeoapi-warn.log`
   * The starlette app points to the log config in this line: `log_config='/opt/pyg_aquainfra/pygeoapi/logconfig.json',`, in the definition of `serve(ctx, ...)`.


This now runs via starlette, but it is not our final setting yet, as we have not added a webserver and/or reverse proxy yet, so please continue with the next steps...


### Add Gunicorn (and Uvicorn) as webserver

Background: The pygeoapi docs state that "Running pygeoapi serve in production is not recommended or advisable" (https://docs.pygeoapi.io/en/latest/running.html#running-in-production), so we pick one of their recommended setups: Using the gunicorn webserver together with Starlette.


* Install dependencies:

```
pip3 install gunicorn
pip3 install uvicorn
```

* Try running on gunicorn:

```
gunicorn pygeoapi.starlette_app:APP -w 4 -k uvicorn.workers.UvicornH11Worker
```

* To test, try the same curl requests as above, but gunicorn now listens on port 8000, so try `curl localhost:8000/jobs` (the port can be specified using `-b`, see gunicorn docs at https://docs.gunicorn.org/en/stable/settings.html)




### Create a systemd service file

Here, we create a service file for running pygeoapi via systemd. This way, we don't need to start (TODO)


* Create a file `/etc/systemd/system/pygeoapi.service` with this content (see file `deployment/pygeoapi.service`):

```
[Unit]
Description=Gunicorn instance to serve pygeoapi
After=network.target

[Service]
User=ubuntu   # TODO: Run as different user: pyguser
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


## Add nginx (reverse proxy and for SSL/TLS and static files)

Run web server (TLS / SSL, reverse proxy, static files, ...)
WIPPI
* Install nginx as a reverse proxy that also does the TLS / SSL termination
* Enable nginx to serve the result files as static content


### Install and configure nginx webserver

* Install nginx according to nginx documentation (e.g. `apt install nginx`)
* Verify it is running: `systemctl status nginx`
* Verify you can see the welcome page by `curl http://<your-ip>`
* Verify whether the logs are located at `/var/log/nginx/error.log` and `/var/log/nginx/access.log`.


### Configure nginx to serve static files

* Create directory where to put any static (result) file:

```
cd /var/www/
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
* Later, we need this to be writeable by the user running pygeoapi (`ubuntu` or `pyguser`), and readable by nginx, so we run:

```
sudo chown ubuntu:www-data /var/www/nginx/download/  # TODO: Run as different user: pyguser
sudo chmod 751 /var/www/nginx/download/
```


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

If you want to have one "productive" and one "sandbox" instance, they share the same reverse proxy, but you must configure the location config so that different URLs lead to the different pygeoapi instances! For example, add a second one:

```
         location ^~ /pygeoapi-dev/ {
             include proxy_params;
             proxy_pass http://127.0.0.1:8001/;
             proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
             proxy_set_header X-Forwarded-Proto $scheme;
             proxy_set_header X-Forwarded-Host $host;
             proxy_set_header X-Forwarded-Prefix /;
         }

```


### Configure TLS on nginx

So far, we can only access nginx and pygeoapi via `http`, but we obviously want `https`.

As nginx is our reverse proxy in front of gunicorn/pygeoapi, we can let it do the SSL termination, so that we don't need to configure gunicorn/pygeoapi to handle SSL. Nginx will receive the `https` requests, handle the SSL stuff, and pass simple `http` requests on to gunicorn/pygeoapi.


```
... TODO TLS/SSL...
```

## How to deploy processes

### General info on processes in AquaINFRA

As a minimum, for each process, we need a process python file (the process) and a json file (the process description). These should sit in a GitHub repo which does not have to be (or should not be) a fork of the pygeoapi repo.

The python file must contain a class that inherits from `pygeoapi.process.base.BaseProcessor` and has the method `execute(self, data, outputs=None)`. For an example, see [here](https://github.com/glowabio/aqua90m/blob/main/pygeoapi_processes/geofresh/get_local_subcids.py). Ideally this calls the functionality from another module (which may be python, or for example R commands called using the `subprocess` module), so that pygeoapi process and functionality are separate and can be called separately. The functionality can also be packaged in a Docker container: In this case, the python file just runs the Docker container.

The JSON file must contain the service description and have the same name as the python file (except for the ending). Then it can be imported in the python file using this code:


```
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))
```

Processes may use process-specific config into the common `config.json`. See section on configuration how to read from that file.


### Containerized processes (1/2): Prepare docker

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
* Probably, the user running pygeoapi (`ubuntu` or `pyguser`) has to be added to group `docker` too!  # TODO: Run as different user: pyguser


### Containerized processes (2/2): Deploy process

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

* If you need specific environment variables, add them to the Flask and/or Starlette apps, close to this line `os.environ['PYGEOAPI_CONFIG'] = '/opt/pyg_aquainfra/pygeoapi/pygeoapi-config.yml'`
* For each process, add a line to `/opt/pyg_aquainfra/pygeoapi/pygeoapi/plugin.py`
* For each process, add a line line to `/opt/pyg_aquainfra/pygeoapi/pygeoapi-config.yml`
* Re-generate the `pygeoapi-openapi.yml` (see [above](#generate-pygeoapi-openapi-yml))
* If you did not install pygeoapi with hot-reloading, you need to reinstall pygeoapi, so it can find the process files (see [above](#install-pygeoapi-module))
* Restart pygeoapi: `date; sudo systemctl restart pygeoapi`
* Now test!


### How to deploy processes (not containerized)

* Processes that sit in a git repo: Go to `/opt/pyg_aquainfra/pygeoapi/pygeoapi/process/` and clone the git repo there.
* If you need specific environment variables, add them to the Flask and/or Starlette apps, close to this line `os.environ['PYGEOAPI_CONFIG'] = '/opt/pyg_aquainfra/pygeoapi/pygeoapi-config.yml'`
* For each process, add a line to `/opt/pyg_aquainfra/pygeoapi/pygeoapi/plugin.py`
* For each process, add a line line to `/opt/pyg_aquainfra/pygeoapi/pygeoapi-config.yml`
* To reflect those additions in the `pygeoapi-openapi.yml` file, re-generate it (see [above](#generate-pygeoapi-openapi-yml))
* If there are new dependencies, add them to `/opt/pyg_aquainfra/pygeoapi/requirements.txt` and reinstall them (see [above](#install-dependencies-into-virtualenv))
* If you did not install pygeoapi with hot-reloading, you need to reinstall pygeoapi, so it can find the process files (see [above](#install-pygeoapi-module))
* Finally, restart pygeoapi: `date; sudo systemctl restart pygeoapi`
* Now test!


## TODO: Testing and Monitoring

* You can manually test each process, e.g. using the curl request in the documentation on top of the process' python file - if the developer added one.
* Maybe the developer was nice and added a python test script that calls each process in their repo.
* TODO: How to automatically test?
* TODO: Set up some monitoring using Icinga or Nagios?


## TODO: Regular clean up

TODO: Describe how to set up the cronjob that regularly deletes old result files and input files.


## Map client

Some pygeoapi processes can be called from a JavaScript map client, which could be served by the nginx instance. For this, please check out [https://github.com/glowabio/aqua90m/tree/main/mapclient](https://github.com/glowabio/aqua90m/tree/main/mapclient).


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

