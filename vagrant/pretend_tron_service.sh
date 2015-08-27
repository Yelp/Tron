#!/bin/sh

pidfile=$1
sleep_time=$2

if [ -f $pidfile ]; then
  echo "FATAL: pidfile already exists. But treat this as ok to see hwat tron does"
  exit 0
fi

echo $$ > $pidfile

while true; do
  echo "$(hostname): $(date)"
  sleep $sleep_time
done

rm -f $pidfile
