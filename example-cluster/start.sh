#/bin/sh

pip install -e .
eval $(ssh-agent)
USER=root trond -l logging.conf --nodaemon --working-dir=/nail/tron -v
