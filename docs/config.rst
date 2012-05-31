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

            **mongo** - uses `pymongo` to save to a mongodb (tested with version 2.2).

            **yaml** - uses `yaml` and saves to a local file (this is not recommend and is provided to be backwards compatible with previous versions of Tron).

        You will need the appropriate python module for the option you choose.

    **name**
        The name of this store. This will be the filename for a **shelve** or
        **yaml** store, or the database name for a **mongo** store. It is
        just a label when used with an **sql** store.

    **connection_details**
        Ignored by **shelve** and **yaml** stores.

        A connection string (see `sqlalchemy engine configuration <http://docs.sqlalchemy.org/en/latest/core/engines.html>`_) when using an **sql** store.

        An HTTP query string when using **mongo**. Valid keys are: hostname, port, username, password.
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


Nodes
-----

**nodes**
    List of nodes, each with a ``name`` and a ``hostname``.  ``name`` defaults
    to ``hostname``. Each of these nodes should be configured to allow SSH
    connections from :command:`trond`.

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
handler that rotates files each day. The default log directory is
``/var/log/tron/tron.log``.

To configure logging pass -l <logging.conf> to trond. You can modify the
default logging.conf by copying it from tron/logging.conf. See
http://docs.python.org/howto/logging.html#configuring-logging
