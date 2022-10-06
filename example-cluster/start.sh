#!/usr/bin/env bash

set -e

if ! service ssh status >/dev/null 2>&1 ; then
  echo Setting up SSH
  apt-get -qq -y install ssh >/dev/null
  service ssh start
fi

if [ -z "$SSH_AUTH_SOCK" ]; then
  echo Setting up SSH agent
  mkdir -p ~/.ssh
  cp example-cluster/insecure_key ~/.ssh/id_rsa
  cp example-cluster/insecure_key.pub ~/.ssh/authorized_keys
  chmod -R 0600 ~/.ssh
  eval $(ssh-agent)
fi

if ! pip3.6 list --format=columns | grep 'tron.*/work' > /dev/null; then
  echo Installing packages
  pip3.6 install -q -r requirements.txt -e .
fi

echo Starting Tron
FAKETIME_X=${FAKETIME_X:-10}
exec faketime -f "+0.0y x$FAKETIME_X" \
  trond -l logging.conf -w /nail/tron -v --debug -H 0.0.0.0
