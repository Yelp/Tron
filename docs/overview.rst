Overview
========

.. note::

    The configuration examples in this document are valid, but omit some YAML
    markup that is useful for validation. See :doc:`config` for examples of
    best practices.

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

Nodes, Jobs and Actions
-----------------------

Tron's orders consist of *jobs* and *services*. :doc:`Jobs <jobs>` contain
:ref:`actions <job_actions>` which may depend on other actions in the same job
and run on a schedule.  :ref:`Services <overview_services>` are meant to be
available continuously.

:command:`trond` is given access (via public key SSH) to one or more *nodes* on
which to run jobs and services.  For example, this configuration has two nodes,
each of which is responsible for a single job::

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

See also:

* :doc:`jobs`
* :doc:`services`
* :doc:`config`

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
            cleanup_action:
                command: "echo 'all done'"

``job2``'s action will be run on a random node from ``pool`` every 5 seconds.
(:ref:`overview_services` behave slightly differently.) When ``pool_action`` is
complete, ``cleanup_action`` will run on the same node.

Note the ``!NodePool`` tag on the node pool. If you do not include this in your
pool definition, ``tronfig`` will try to interpret it as a single node and
reject your configuration.

.. _overview_services:

Services
--------

The job model is not appropriate for tasks that should be running continuously,
perhaps with more than one instance at once. For example, you might have a
set of worker processes that send emails::

    # ...
    services:
        -
            name: "email_worker"
            node: *pool
            count: 4
            monitor_interval: 60
            restart_interval: 120
            pid_file: "/var/run/batch/%(name)s-%(instance_number)s.pid"
            command: "/usr/local/bin/start_email_worker --pid_file=%(pid_file)s"

This configuration will cause ``start_email_worker`` to be run on the nodes
in the pool in the order ``node1``, ``node2``, ``node1``, ``node2`` (round
robin scheduling).

The ``start_email_worker`` script (written by you) starts the worker and writes
its pid to ``%(pid_file)s``. Every 60 seconds, `trond` will see if that pid is
still active on its node. If not, the service will be in a ``DEGRADED`` state
and a new service instance will be started on the same node after 120 seconds.

For more information, see :doc:`services`.

Notifications
-------------

If you configure notifications, `trond` will send you emails when something
fails::

    notification_options:
        smtp_host: localhost
        notification_addr: batch+live@example.com
