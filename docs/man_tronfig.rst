.. _tronfig:

tronfig
=======

Synopsys
--------

``tronfig [--server server_name ] [--verbose | -v] [-]``

Description
-----------

**tronfig** allows live editing of the Tron configuration.  It retrieves
the configuration file for local editing, verifies the configuration,
and sends it back to the tron server. The configuration is applied
immediately.

Options
-------

``--server <server_name>``
    The server the tron instance is running on

``--verbose``
    Displays status messages along the way

``--version``
    Displays version string

``-``
    Read new config from ``stdin``.

Configuration
-------------

By default tron will run with a blank configuration file. Get the full
configuration docs at http://packages.python.org/tron/config.html.

Example Configuration
---------------------

::

    ssh_options:
      agent: true

    nodes:
        - name: node1
          hostname: 'machine1'
        - name: node2
          hostname: 'machine2'

    node_pools:
        - name: pool
          nodes: [node1, node2]

    command_context:
        PYTHON: /usr/bin/python

    jobs:
        - name: "job0"
          node: pool
          all_nodes: True
          schedule: "daily 12:00 MWF"
          queueing: False
          actions:
            - name: "start"
              command: "echo number 9"
              node: node1
            - name: "end"
              command: "echo love me do"
              requires: [start]

        - name: "job1"
          node: node1
          schedule: "interval 20s"
          queueing: False
          actions:
            - name: "echo"
              command: "echo %(PYTHON)s"
          cleanup_action:
            command: "echo 'cleaning up job1'"

    services:
        - name: "testserv"
          node: pool
          count: 8
          monitor_interval: 60
          restart_delay: 120
          pid_file: "/var/run/%(name)s-%(instance_number)s.pid"
          command: "/bin/myservice --pid-file=%(pid_file)s start"

Files
-----

/var/lib/tron/tron_config.yaml
    Default path to the config file. May be changed by passing the ``-c``
    option to **trond**.

Bugs
----

Post bugs to http://www.github.com/yelp/tron/issues.

See Also
--------

**trond** (8), **tronctl** (1), **tronview** (1),
