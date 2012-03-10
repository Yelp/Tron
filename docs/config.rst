Configuration
=============

.. note::

    **The configuration system has changed significantly since version 0.2.9.**
    All existing configurations should still work, but new configurations
    should follow the new conventions.

.. _config_syntax:

Syntax
------

The Tron configuration file uses YAML syntax. The recommended configuration
style requires only strings, decimal values, lists, and dictionaries: the
subset of YAML that can be losslessly transformed into JSON. (In fact, your
configuration can be entirely JSON, since YAML is mostly a strict superset
of JSON.)

Past versions of Tron used additional YAML-specific features such as tags,
anchors, and aliases. These features still work in version 0.3, but are not
recommended.

Basic Example
-------------

::

    ssh_options:
      agent: true

    notification_options:
      smtp_host: localhost
      notification_addr: <your email address>

    nodes:
      - name: local
        hostname: 'localhost'

    jobs:
      - name: "getting_node_info"
        node: local
        schedule: "interval 10 mins"
        actions:
          - name: "uname"
            command: "uname -a"
          - name: "cpu_info"
            command: "cat /proc/cpuinfo"
            requires: [uname]

.. _command_context_variables:

Command Context Variables
^^^^^^^^^^^^^^^^^^^^^^^^^

**command** attribute values may contain **command context variables** that are
inserted at runtime. The **command context** is populated both by Tron (see
:ref:`built_in_cc`) and by the config file (see :ref:`command_context`). For
example::

    jobs:
        - name: "command_context_demo"
          node: local
          schedule: "1st monday in june"
          actions:
            - name: "print_run_id"
              # prints 'command_context_demo.1' on the first run,
              # 'command_context_demo.2' on the second, etc.
              command: "echo %(runid)"

SSH
---

**ssh_options** (optional)
    These options affect how Tron connects to the nodes.

    **agent** (optional, default ``False``)
        Set to ``True`` if :command:`trond` should use an SSH agent

    **identities** (optional, default ``[]``)
        List of paths to SSH identity files

Example::

    ssh_options:
        agent: false
        identities:
            - /home/batch/.ssh/id_dsa-nopasswd

Notification Options
--------------------

**notification_options**
    Email settings for sending failure notices.

    **smtp_host** (required)
        SMTP host of the email server

    **notification_addr** (required)
        Email address to send mail to

Example::

    notification_options:
        smtp_host: localhost
        notification_addr: batch+errors@example.com

.. _time_zone:

Time Zone
---------

**time_zone** (optional)
    Local time as observed by the system clock. If your system is obeying a
    time zone with daylight savings time, then some of your jobs may run early
    or late on the days bordering each mode. See :ref:`dst_notes` for more
    information.

    ::

        time_zone: US/Pacific

.. _command_context:

Command Context
---------------

**command_context**
    Dictionary of custom :ref:`command context variables
    <command_context_variables>`. This attribute does *not* use a tag since it
    is an arbitrary set of key-value pairs rather than an object with a schema.

    ::

        command_context: # no tag
            PYTHON: /usr/bin/python
            TMPDIR: /tmp

.. Keep this synchronized with man_tronfig

.. _built_in_cc:

Built-In Command Context Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. warning::

    This section is incomplete. If something is missing, don't hesitate to
    `file an issue <http://www.github.com.com/yelp/Tron/issues/new>`_.

**shortdate**
    Current date in ``YYYY-MM-DD`` format. Supports simple arithmetic of the
    form ``%(shortdate+6)s``, ``%(shortdate-2)s``, etc.

**name**
    Name of the job or service

**actionname**
    Name of the action

**runid**
    Run ID of the job or service (e.g. ``sample_job.23``)

**node**
    Hostname of the node the action is being run on

**cleanup_job_status**
    ``SUCCESS`` if all actions have succeeded when the cleanup action runs,
    ``FAILURE`` otherwise. ``UNKNOWN`` if used in an action other than the
    cleanup action.

.. _config_logging:

Logging
-------

**syslog_address** (optional)
    Include this if you want to enable logging to syslog. Accepts paths as
    strings and ``[address, port]`` lists for sockets. Typical values for
    various platforms are::

        Linux: "/dev/log"
        OS X: "/var/run/syslog"
        Windows: ["localhost", 514]

Example::

    syslog_address: "/dev/log"

Nodes
-----

**nodes**
    List of nodes, each with a ``name`` and a ``hostname``. (This section may
    also contain node pools, but we recommend that you put those under
    ``node_pools``.) ``name`` defaults to ``hostname``. If you do not specify
    any nodes, Tron will create a node with name and hostname ``localhost``.

Example::

    nodes:
        - name: node1
          hostname: 'batch1'
        - hostname: 'batch2'    # name is 'batch2'

Node Pools
----------

**node_pools**
    List of node pools, each with a ``name`` and ``nodes`` list. ``name``
    defaults to the names of each node joined by underscores.

Example::

    node_pools:
        - name: pool
          nodes: [node1, batch1]
        - nodes: [batch1, node1]    # name is 'batch1_node1'

Jobs and Actions
----------------

**jobs**
    List of jobs for Tron to manage. See :doc:`jobs` for the options available
    to jobs and their actions.

Services
--------

**services**
    List of services for Tron to manage.  See :doc:`services` for the options
    available to services.
