#!/bin/bash

set -eux

export DEBIAN_FRONTEND=noninteractive

dpkg -i /work/dist/*.deb || true
apt-get install -qq -y -f
dpkg -i /work/dist/*.deb

trond --help
tronfig --help

/opt/venvs/tron/bin/python - <<EOF
from yaml import CSafeLoader
from yaml import CSafeDumper
EOF
