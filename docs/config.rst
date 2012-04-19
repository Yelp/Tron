Configuration
=============

.. note::

    **The configuration system has changed significantly since version 0.2.10.**
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
anchors, and aliases. These features still work in version 0.3, but are now
deprecated.

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
    <command_context_variables>`. It is an arbitrary set of key-value pairs.

    ::

        command_context:
            PYTHON: /usr/bin/python
            TMPDIR: /tmp

.. Keep this synchronized with man_tronfig

.. _built_in_cc:

Built-In Command Context Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


**shortdate**
    Current date in ``YYYY-MM-DD`` format. Supports simple arithmetic of the
    form ``%(shortdate+6)s`` which returns a date 6 days in the future,
    ``%(shortdate-2)s`` which returns a date 2 days before the run date.

**year**
    Current year in ``YYYY`` format. Supports the same arithmetic operations
    as `shortdate`. For example, ``%(year-1)s`` would return the year previous
    to the run date.

**month**
    Current month in `MM` format. Supports the same arithmetic operations
    as `shortdate`. For example, ``%(month+2)s`` would return 2 months in the
    future.

**day**
    Current day in `DD` format. Supports the same arithmetic operations
    as `shortdate`. For example, ``%(day+1)s`` would return the day after the
    run date.

**unixtime**
    Current timestamp. Supports addition and subtraction of seconds. For
    example ``%(unixtime+20)s`` would return the timestamp 20 seconds after
    the jobs runtime.

**daynumber**
    Current day number as an ordinal (datetime.toordinal()). Supports addition
    and subtraction of days. For example ``%(daynumber-3)s`` would be 3 days
    before the run date.

**name**
    Name of the job or service

**node**
    Hostname of the node the action is being run on


Context variables only available to Jobs:

**runid**
    Run ID of the job or service (e.g. ``sample_job.23``)

**actionname**
    Name of the action

**cleanup_job_status**
    ``SUCCESS`` if all actions have succeeded when the cleanup action runs,
    ``FAILURE`` otherwise. ``UNKNOWN`` if used in an action other than the
    cleanup action.


Context variables only available to Services:

**pid_file**
    The filename of the pid file.

**instance_number**
    The number identifying this instance (will be 0 to n-1 where n is the
    total number of instances).


Output Stream Directory
-----------------------
**output_stream_dir** allows you to specific the directory used to store the
stdout/stderr logs from jobs.  It defaults to the `working_dir` option passed
to :ref:`trond`.


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


.. _config_logging:

Logging
-------

As of v0.3.3 Logging is no longer configured in the tron configuration file.

Tron uses Python's standard logging and by default uses a rotating log file
handler that rotates files each day. Logs go to /var/log/tron/tron.log.

To configure logging pass -l <logging.conf> to trond. You can modify the
default logging.conf by copying it from tron/logging.conf. See
http://docs.python.org/howto/logging.html#configuring-logging
