Configuration
=============

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

    nodes:
      - name: local
        hostname: 'localhost'

    jobs:
      "getting_node_info":
        node: local
        schedule: "cron */10 * * * *"
        actions:
          "uname":
            command: "uname -a"
          "cpu_info":
            command: "cat /proc/cpuinfo"
            requires: [uname]

.. _command_context_variables:

Command Context Variables
-------------------------

**command** attribute values may contain **command context variables** that are
inserted at runtime. The **command context** is populated both by Tron (see
:ref:`built_in_cc`) and by the config file (see :ref:`command_context`). For
example::

    jobs:
     "command_context_demo":
       node: local
       schedule: "1st monday in june"
       actions:
         "print_run_id":
           # prints 'command_context_demo.1' on the first run,
           # 'command_context_demo.2' on the second, etc.
           command: "echo {runid}"

SSH
---

**ssh_options** (optional)
    Options for SSH connections to Tron nodes. When tron runs a job
    on a node, it will add some jitter (random delay) to the run, which can be
    configured with the options below.

    **agent** (optional, default ``False``)
        Set to ``True`` if :command:`trond` should use an SSH agent. This requires
        that ``$SSH_AUTH_SOCK`` exists in the environment and points to the
        correct socket.

    **identities** (optional, default ``[]``)
        List of paths to SSH identity files

    **known_hosts_file** (optional, default ``None``)
        The path to an ssh known hosts file

    **connect_timeout** (optional, default ``30``)
        Timeout in seconds when establishing an ssh connection

    **idle_connection_timeout** (optional, default ``3600``)
        Timeout in seconds that an ssh connection can remain idle after which
        it is closed

    **jitter_min_load** (optional, default ``4``)
        Minimum `load` on a node before any jitter is introduced. See
        `jitter_load_factor` for a description of how load is calculated

    **jitter_max_delay** (optional, default ``20``)
        Maximum number of seconds to add to a run

    **jitter_load_factor** (optional, default ``1``)
        Factor used to increment the count of running actions for determining
        the upper bound of jitter to add (ex. A factor of 2 would increase the
        upper bound by 2 seconds per running action)

Example::

    ssh_options:
        agent:                    false
        known_hosts_file:         /etc/ssh/known_hosts
        identities:
            - /home/batch/.ssh/id_dsa-nopasswd

        connect_timeout:          30
        idle_connection_timeout:  3600

        jitter_min_load:          4
        jitter_max_delay:         20
        jitter_load_factor:       1

.. _time_zone:

Time Zone
---------

**time_zone** (optional)
    Local time as observed by the system clock. If your system is obeying a
    time zone with daylight savings time, then some of your jobs may run early
    or late on the days bordering each mode. See :ref:`dst_notes` for more
    information.

Example::

        time_zone: US/Pacific

.. _command_context:

Command Context
---------------

**command_context**
    Dictionary of custom :ref:`command context variables
    <command_context_variables>`. It is an arbitrary set of key-value pairs.

Example::

        command_context:
            PYTHON: /usr/bin/python
            TMPDIR: /tmp

See a list of :ref:`built_in_cc`.


Output Stream Directory
-----------------------
**output_stream_dir**
    A path to the directory used to store the stdout/stderr logs from jobs.
    It defaults to the ``--working_dir`` option passed to :ref:`trond`.

Example::

    output_stream_dir: "/home/tronuser/output/"


.. _config_state:

State Persistence
-----------------
**state_persistence**
    Configure how trond should persist its state to disk. By default a `shelve`
    store is used and saved to `./tron_state` in the working directory.

    **store_type**
        Valid options are:
            **shelve** - uses the `shelve` module and saves to a local file

            **sql** - uses `sqlalchemy <http://www.sqlalchemy.org/>`_ to save to a database (tested with version 0.7).

            **yaml** - uses `yaml` and saves to a local file (this is not recommend and is provided to be backwards compatible with previous versions of Tron).

        You will need the appropriate python module for the option you choose.

    **name**
        The name of this store. This will be the filename for a **shelve** or
        **yaml** store. It is just a label when used with an **sql** store.

    **connection_details**
        Ignored by **shelve** and **yaml** stores.

        A connection string (see `sqlalchemy engine configuration <http://docs.sqlalchemy.org/en/latest/core/engines.html>`_) when using an **sql** store.

        Valid keys are: hostname, port, username, password.
        Example: ``"hostname=localhost&port=5555"``

    **buffer_size**
        The number of save calls to buffer before writing the state.  Defaults to 1,
        which is no buffering.


Example::

    state_persistence:
        store_type: sql
        name: local_sqlite
        connection_details: "sqlite:///dest_state.db"
        buffer_size: 1 # No buffer


.. _action_runners:

Action Runners
--------------

**Note:** this is an experimental feature


**action_runner**
    Action runner configuration allows you to run Job actions through a script
    which records it's pid. This provides support for a max_runtime option
    on jobs, and allows you to stop or kill the action from :command:`tronctl`.

    **runner_type**
        Valid options are:
            **none**
                Run actions without a wrapper. This is the default

            **subprocess**
                Run actions with a script which records the pid and runs the
                action command in a subprocess (on the remote node). This
                requires that :command:`bin/action_runner.py` and
                :command:`bin/action_status.py` are available on the remote
                host.

    **remote_status_path**
        Path used to store status files. Defaults to `/tmp`.

    **remote_exec_path**
        Directory path which contains :command:`action_runner.py` and
        :command:`action_status.py` scripts.


Example::

    action_runner:
        runner_type:        "subprocess"
        remote_status_path: "/tmp/tron"
        remote_exec_path:   "/usr/local/bin"


Nodes
-----

**nodes**
    List of nodes. Each node has the following options:

    **hostname** (required)
        The hostname or IP address of the node

    **name** (optional, defaults to ``hostname``)
        A name to refer to this node

    **username** (optional, defaults to current user)
        The name of the user to connect with

    **port** (optional, defaults to 22)
        The port number of the node


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

.. _config_logging:

Logging
-------

As of v0.3.3 Logging is no longer configured in the tron configuration file.

Tron uses Python's standard logging and by default uses a rotating log file
handler that rotates files each day. The default log directory is
``/var/log/tron/tron.log``.

To configure logging pass -l <logging.conf> to trond. You can modify the
default logging.conf by copying it from tron/logging.conf. See
http://docs.python.org/howto/logging.html#configuring-logging

Interesting logs
~~~~~~~~~~~~~~~~

Most tron logs are named by using pythons `__file__` which uses the modules
name.  There are a couple special cases:

**twisted**
    Twisted sends its logs to the `twisted` log

**tron.api.www.access**
    API access logs are sent to this log at the INFO log level.  They follow
    a standard apache combined log format.
