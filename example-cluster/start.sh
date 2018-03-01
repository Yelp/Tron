#/bin/sh

pip3.6 install -e .
eval $(ssh-agent)
export USER=root
exec trond -l logging.conf --nodaemon --working-dir=/nail/tron -v
