#!/bin/sh


tronfig - < MASTER.yaml
for config in $( ls *.yaml | grep -v MASTER.yaml ); do
  namespace=${config%%.yaml}
  tronfig $namespace - < $config
done

