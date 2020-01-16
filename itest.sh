#!/bin/bash

set -euxo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y software-properties-common gdebi-core curl
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update

gdebi --non-interactive /work/dist/*.deb

# TODO: change default MASTER config to not require ssh agent
apt-get install -y ssh
service ssh start
eval $(ssh-agent)

trond --help
tronfig --help

/opt/venvs/tron/bin/python --version | grep -q '3\.6'

/opt/venvs/tron/bin/python - <<EOF
from yaml import CSafeLoader
from yaml import CSafeDumper
EOF

export TRON_WORKDIR=/nail/tron
mkdir -p $TRON_WORKDIR
export TRON_START_TIME=$(date +%s)

trond --working-dir=$TRON_WORKDIR &
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

curl localhost:8089/api/status | grep -qi alive

tronfig -p MASTER
tronfig -n MASTER /work/example-cluster/tronfig/MASTER.yaml
tronfig /work/example-cluster/tronfig/MASTER.yaml
cat /work/example-cluster/tronfig/MASTER.yaml | tronfig -n MASTER -

if test -L /opt/venvs/tron/lib/python3.6/encodings/punycode.py; then
    echo "Whoa, the tron package shouldn't have an encoding symlink!"
    echo "Check out https://github.com/spotify/dh-virtualenv/issues/272"
    exit 1
fi

kill -SIGTERM $TRON_PID
wait $TRON_PID || true

/opt/venvs/tron/bin/python - <<EOF
import os
from tron.serialize.runstate.shelvestore import ShelveStateStore, ShelveKey
db = ShelveStateStore('$TRON_WORKDIR/tron_state')
key = ShelveKey('mcp_state', 'StateMetadata')
res = db.restore([key])
ts = res[key][u'create_time']
print("assert db time {} > start time {}".format(ts, int(os.environ['TRON_START_TIME'])))
assert ts > int(os.environ['TRON_START_TIME'])
EOF
