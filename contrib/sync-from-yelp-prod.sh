#!/bin/bash
rsync --exclude=.stderr --exclude=.stdout -aPv tron-prod:/nail/tron/*  example-cluster/
git checkout example-cluster/logging.conf
rm -f example-cluser/tron.lock

echo ""
echo "Now Run:"
echo ""
echo "    tox -e example-cluster"
echo "    ./example-cluster/start.sh"
