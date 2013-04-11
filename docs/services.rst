Services
========

A service is composed of several *service instances* which are either `up` or
`down`. Service instances are daemon processes running on a node. Services
are required to manage a pid file, which is used to determine the state of
the service instance. Tron checks for the pid stored in the pid file every
**monitor_interval** seconds and restarts it after **restart_delay** seconds
if the process is no longer running.


Required Fields
---------------

**name**
    Name of the service. Used in :command:`tronview` and :command:`tronctl`.

**node**
    Reference to the node or node pool. If a pool, instances
    are started by round robin scheduling of the nodes in the pool.

**pid_file**
    Path to the pid file used by the service. This will typically include
    the command context variables ``name`` and ``instance_number``.

**command**
    Command to start the service. This command is responsible for writing the
    service pid to ``pid_file``.

**monitor_interval**
    The number of seconds between status checks of the services state (if the
    process is still running or not).

Optional Fields
---------------

**restart_delay** (default **never**)
    Seconds to wait before restarting the service when it appears to be
    down. If not specified, service instances will not be restarted when down.

**count** (default **1**)
    Number of instances of this service to keep running at once. If a node pool
    is used, the instances are spread across all nodes in the pool evenly by
    round robin scheduling.


States
------

The following is a list of states for a Service.

**STARTING**
    The service has been started. The service will remain in this state until
    the first monitor interval runs.

**UP**
    The service is running. All instances were up when they were last checked.

**DEGRADED**
    One or more instances of the service are unexpectedly not available. The
    service will go back to **UP** when the instance(s) are restarted.

**FAILED**
    All instances of the service are down.

**DOWN**
    Service has been stopped.

State Diagram
^^^^^^^^^^^^^

This diagram shows all the states and (where applicable) the command used to
transition between states.

Service Instance State
~~~~~~~~~~~~~~~~~~~~~~

.. image:: images/service_instance.png
    :width: 680px

Examples
--------

Here is the example from :ref:`Overview: Services <overview_services>`::

    services:
        -   name: "email_worker"
            node: service_pool
            count: 4
            monitor_interval: 60
            restart_delay: 120
            pid_file: "/var/run/batch/%(name)s-%(instance_number)s.pid"
            command: "/usr/local/bin/start_email_worker --pid_file=%(pid_file)s"
