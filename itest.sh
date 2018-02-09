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

rm -rf /var/lib/tron
ln -s /work/example-cluster /var/lib/tron
rm -f /var/lib/tron/tron.pid
trond -v --nodaemon -c tronfig/ -l logging.conf &
TRON_PID=$!

sleep 5
kill -0 $TRON_PID

tronfig -p MASTER
tronfig MASTER /work/example-cluster/tronfig/MASTER.yaml

kill -9 $TRON_PID
