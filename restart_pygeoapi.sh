#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Check if Python 3.10 is available
PYTHON_VERSION=$(python3 --version 2>&1)
if [[ $PYTHON_VERSION != "Python 3.10"* ]]; then
  echo "Python 3.10 is required. Currently installed: $PYTHON_VERSION"
  exit 1
fi

# Install the required packages
# pip3 install -r requirements.txt

# Install pygeoapi
python3 setup.py install

# Copy the example configuration file
cp pygeoapi-config.yml example-config.yml

# Set environment variables
export PYGEOAPI_CONFIG=example-config.yml
export PYGEOAPI_OPENAPI=example-openapi.yml

# Generate the OpenAPI document
pygeoapi openapi generate $PYGEOAPI_CONFIG --output-file $PYGEOAPI_OPENAPI

# Start the pygeoapi server
pygeoapi serve
