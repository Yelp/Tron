#!/bin/bash

set -euxo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install gdebi-core curl --yes
gdebi --non-interactive /work/dist/*.deb

trond --help
tronfig --help

/opt/venvs/tron/bin/python - <<EOF
from yaml import CSafeLoader
from yaml import CSafeDumper
EOF

rm -rf /var/lib/tron
ln -s /work/example-cluster /var/lib/tron
rm -f /var/lib/tron/tron.pid
export TRON_START_TIME=$(date +%s)

trond -v --nodaemon -c tronfig/ -l logging.conf &
TRON_PID=$!

for i in {1..5}; do
    if curl localhost:8089/api/status 2>/dev/null; then
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
tronfig -n MASTER /work/example-cluster/tronfig/MASTER.yaml
tronfig /work/example-cluster/tronfig/MASTER.yaml
cat /work/example-cluster/tronfig/MASTER.yaml | tronfig -n MASTER -

kill -SIGTERM $TRON_PID
wait $TRON_PID

/opt/venvs/tron/bin/python - <<EOF
import os
from tron.serialize.runstate.shelvestore import Py2Shelf
db = Py2Shelf('/var/lib/tron/tron_state')
ts = db[b'mcp_state___StateMetadata'][u'create_time']
print("assert db time {} > start time {}".format(ts, int(os.environ['TRON_START_TIME'])))
assert ts > int(os.environ['TRON_START_TIME'])
EOF
