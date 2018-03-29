#/bin/sh
pip install -e .
eval $(ssh-agent) || true
export USER=root
exec trond -l logging.conf --nodaemon --working-dir=/nail/tron -v
