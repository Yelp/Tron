#!/usr/bin/env bash

if ! which ssh; then
  echo Setting up SSH
  apt-get -qq -y install ssh
  mkdir -p ~/.ssh
  cp example-cluster/insecure_key ~/.ssh/id_rsa
  cp example-cluster/insecure_key.pub ~/.ssh/aithorized_keys
  chmod -R 0600 ~/.ssh
  service ssh start
fi

echo Installing packages
pip install -e .

echo Starting Tron
exec trond -l logging.conf --nodaemon --working-dir=/nail/tron -v
