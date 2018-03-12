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

# To test tronview, tronctl, etc
First, start Tron in a container using the above steps. Then in a different terminal,
you will attach to that running container. There you will be able to run `tronctl`,
`tronview` and others against the Trond master in the example cluster.

```
$ docker ps

[find container ID of examplecluster_master]

$ sudo docker exec -it <ID> /bin/bash
```
