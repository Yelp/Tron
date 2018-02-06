#/bin/sh
eval $(ssh-agent)
ssh-add /work/example-cluster/insecure_key
USER=root bin/trond --nodaemon -v -w /work/example-cluster/workdir -l logging.conf
