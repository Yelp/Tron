#/bin/sh

eval $(ssh-agent)
USER=root trond -c tronfig/ -l logging.conf --nodaemon -v
