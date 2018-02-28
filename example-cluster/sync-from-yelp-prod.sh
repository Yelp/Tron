#!/bin/bash
rsync --exclude=.stderr --exclude=.stdout -aPv tron-prod:/nail/tron/config  example-cluster/
rsync --exclude=.stderr --exclude=.stdout -aPv tron-prod:/nail/tron/tron_state_0.6.1.5.gdbm  example-cluster/
rm example-cluser/tron.pid
