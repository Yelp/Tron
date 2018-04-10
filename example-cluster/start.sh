#/bin/sh
pip install -e .
eval $(ssh-agent) || true
export USER=root
/etc/init.d/ssh start &
rm -f /nail/tron/tron.pid
exec trond -l logging.conf --nodaemon --working-dir=/nail/tron -v
