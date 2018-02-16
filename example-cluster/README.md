# Example Cluster

This is a docker-compose setup for a working Tron setup. There is one example job
in the tronfig which gets deployed.

# To Run

```
$ tox -e example-cluster
```

# To start Tron (from inside the master container)

```
$ cd /work
$ ./example-cluster/start.sh
```
