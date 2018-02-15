#/bin/sh

pip install -e .
eval $(ssh-agent)
USER=root trond -c tronfig/ -l logging.conf --nodaemon -v
