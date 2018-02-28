#/bin/sh

pip3 install wheel
pip3 install -e .
eval $(ssh-agent)
export USER=root
exec trond -l logging.conf --nodaemon --working-dir=/nail/tron -v
