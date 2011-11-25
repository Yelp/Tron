Services
========

A major cluster use case that :command:`cron` doesn't cover is the maintenance
of *services*. A service is composed of several *service instances* which are
either on or off. Service instances are interacted with as processes, but those
processes can represent daemons, third party web services, and more.

.. Keep this up to date with man_tronfig.rst

Required Fields
---------------

**name**
    Name of the service. Used in :command:`tronview` and :command:`tronctl`.

**node**
    Reference to the node or pool to service the job in. If a pool, instances
    are started by round robin scheduling of the nodes in the pool.

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

**restart_interval**
    Seconds to wait before restarting to the service when it appears to be
    down. If not specified, service instances will not be restarted when down.

**count**
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

See :ref:`Overview: Services <overview_services>` for an example of a service.
