#/bin/sh
eval $(ssh-agent)
USER=root bin/trond --nodaemon -v
