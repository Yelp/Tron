#/bin/sh
pip install -e .
export USER=root
/etc/init.d/ssh start &
rm -f /nail/tron/tron.pid
exec faketime -f '+0.0y x10' trond -l logging.conf --nodaemon --working-dir=/nail/tron -v
