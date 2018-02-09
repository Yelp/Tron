#/bin/sh
eval $(ssh-agent)
USER=root bin/trond -l logging.conf --nodaemon -v
