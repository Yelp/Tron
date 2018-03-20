#!/bin/bash

set -euxo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install gdebi-core curl --yes
gdebi --non-interactive /work/dist/*.deb

trond --help
tronfig --help

/opt/venvs/tron/bin/python --version | grep -q '3\.6'

/opt/venvs/tron/bin/python - <<EOF
from yaml import CSafeLoader
from yaml import CSafeDumper
EOF

rm -rf /var/lib/tron
ln -s /work/example-cluster /var/lib/tron
rm -f /var/lib/tron/tron.pid
trond -v --nodaemon -c tronfig/ -l logging.conf &
TRON_PID=$!

for i in {1..5}; do
    if curl localhost:8089/api/status; then
        break
    fi
    if [ "$i" == "5" ]; then
        echo "Failed to start"
        kill -9 $TRON_PID
        exit 1
    fi
    sleep 1
done
kill -0 $TRON_PID

curl localhost:8089/api/status | grep -qi alive

tronfig -p MASTER
tronfig -n MASTER /work/example-cluster/tronfig/MASTER.yaml
cat /work/example-cluster/tronfig/MASTER.yaml | tronfig -n MASTER -

kill -9 $TRON_PID
