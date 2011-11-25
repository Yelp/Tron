Overview
========

.. note::

    The configuration examples in this document are valid, but omit some YAML
    markup that is useful for validation. See :doc:`config-reference` for
    examples of best practices.

Batch process scheduling on single UNIX machines has historically been managed
by :command:`cron` and its derivatives. But if you have many batches and many
machines, maintaining config files across them may be difficult. Tron solves
this problem by centralizing the configuration and scheduling of jobs to a
daemon.

The Tron system is split into four programs:

:ref:`trond`
    Daemon responsible for scheduling, running, and saving state. Provides an
    HTTP interface to tools.

:ref:`tronview`
    View job state and results.

:ref:`tronctl`
    Start, stop, enable, disable, and otherwise control jobs and services.

:ref:`tronfig`
    Change Tron's configuration while the daemon is still running.

The config file uses YAML syntax and relies on several YAML features to
validate.

Nodes
-----

:command:`trond` is given access (via public key SSH) to one or more *nodes*.
For example, this configuration has two nodes, each of which is responsible for
a single job::

    --- !TronConfiguration

    nodes:
        - &node1
            hostname: 'batch1'
        - &node2
            hostname: 'batch2'

    jobs:
        -
            name: "job0"
            node: *node1
            schedule: "interval 20s"
            actions:
                -
                    name: "batch1action"
                    command: "sleep 3; echo asdfasdf"
        -
            name: "job1"
            node: *node2
            schedule: "interval 20s"
            actions:
                -
                    name: "batch2action"
                    command: "cat big.txt; sleep 10"

How the nodes are set up and assigned to jobs is entirely up to you. They may
have different operating systems, access to different databases, different
privileges for the Tron user, etc.

The line ``--- !TronConfiguration`` is mandatory. It tells the YAML parser how
to validate the document.

Node Pools
----------

Nodes can be grouped into *pools*. To continue the previous example::

    nodes:
        # ...
        - &pool !NodePool
            nodes: [*node1, *node2]

    jobs:
        # ...
        -
            name: "job2"
            node: *pool
            schedule: "interval 5s"
            actions:
                -
                    name: "pool_action"
                    command: "ls /; sleep 1"

``job2``'s action will be run on a random node from ``pool`` every 5 seconds.
(:doc:`services` behave slightly differently.)

Note the ``!NodePool`` tag on the node pool. If you do not include this in your
pool definition, ``tronfig`` will try to interpret it as a single node and
reject your configuration.

Actions
-------


