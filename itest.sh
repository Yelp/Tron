#!/bin/bash

set -euxo pipefail

export DEBIAN_FRONTEND=noninteractive

dpkg -i /work/dist/*.deb || true
apt-get update >/dev/null
apt-get install -qq -y -f curl
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

tronfig -p MASTER
tronfig MASTER /work/example-cluster/tronfig/MASTER.yaml
cat /work/example-cluster/tronfig/MASTER.yaml | tronfig MASTER -

kill -9 $TRON_PID
