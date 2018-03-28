#!/usr/bin/env bash

set -eu

if ! dpkg -l ssh >/dev/null 2>&1; then
  echo Installing SSH
  apt-get install -y ssh
  service ssh start
fi

if [ ! -f /root/.ssh/id_rsa ]; then
  echo Setting up SSH keys
  yes | ssh-keygen -q -N "" -f /root/.ssh/id_rsa >/dev/null
  cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
  eval $(ssh-agent)
fi

echo Installing packages
pip3.6 install -q -e .

echo Starting trond
exec trond -l logging.conf --nodaemon --working-dir=/nail/tron -v
