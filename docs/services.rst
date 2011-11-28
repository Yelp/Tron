Services
========

A service is composed of several *service instances* which are either on or
off. Service instances are interacted with as processes, but those processes
can represent daemons, third party web services, and more.

A service is started by invoking a command that writes a file containing the
service's **pid** to **pid_file**. Tron checks for that pid's existence every
**monitor_interval** seconds and restarts it after **restart_interval** seconds
if it goes down.

.. Keep this up to date with man_tronfig.rst

Required Fields
---------------

**name**
    Name of the service. Used in :command:`tronview` and :command:`tronctl`.

**node**
    Reference to the node or pool to service the job in. If a pool, instances
    are started by round robin scheduling of the nodes in the pool. This is an
    alias to an anchor specified in **nodes**.

**pid_file**
    File to write one service instance's pid to. This will typically include
    the command context variables ``name`` and ``instance_number``.

**command**
    Command to start the service. This command is responsible for writing the
    service pid to ``pid_file``.
    
**monitor_interval**
    Seconds between checks that the service is still up.

Optional Fields
---------------

**restart_interval** (default **never**)
    Seconds to wait before restarting to the service when it appears to be
    down. If not specified, service instances will not be restarted when down.

**count** (default **1**)
    Number of instances of this service to keep running at once. If a node pool
    is used, the instances are spread across all nodes in the pool evenly by
    round robin scheduling.

.. Keep this up to date with man_tronview.rst

States
------

**STARTING**
    The service has been started. The service will remain in this state until
    the first monitor interval runs.

**UP**
    The service is running normally. All instances were available during the
    last monitor period.

**DEGRADED**
    One or more instances of the service are unexpectedly not available. The
    service will go back to **UP** when the instance is restarted.

**FAILED**
    All instances of the service are unexpectedly unavailable.

**DOWN**
    Service has been stopped

Examples
--------

Here is the example from :ref:`Overview: Services <overview_services>`, but
with the correct anchor and tag::

    # ...
    services:
        - &email_worker !Service
            name: "email_worker"
            node: *pool
            count: 4
            monitor_interval: 60
            restart_interval: 120
            pid_file: "/var/run/batch/%(name)s-%(instance_number)s.pid"
            command: "/usr/local/bin/start_email_worker --pid_file=%(pid_file)s"
