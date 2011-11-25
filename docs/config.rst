Configuration
=============

Syntax
------

The Tron configuration file uses YAML syntax. In addition to simple key-value
and list syntax, it uses tags and repeated nodes. This section outlines the
subset of YAML used by Tron configuration files.

Basic Syntax
^^^^^^^^^^^^

YAML is a (mostly) strict superset of JSON, so JSON syntax works, including
integers, floating point numbers, strings with quotes, lists, and
dictionaries::

    {'key': [1, 2, "value"], 50: "hooray"}

It adds whitespace-sensitive syntax for these same structures and makes
quotation marks optional for strings without whitespace::

    key:
        - 1
        - 2
        - "value"
    50: hooray

Repeated Nodes
^^^^^^^^^^^^^^

You can reference any object later in the document using *repeated nodes*. The
original object (dictionary, list, etc.) is marked with an *anchor*, specified
by an ampersand (``&``), and aliased later with an asterisk(``*``).

Tron uses this syntax in several places. The simplest is when specifying nodes
for jobs::

    nodes:
        - &node1
            hostname: 'batch1'
    jobs:
        -
            name: "job1"
            node: *node1

It is also used for specifying :ref:`action dependencies <job_actions>` and
:ref:`node pools <overview_pools>`. In general, you should specify an anchor
for all nodes, jobs, actions, and services.

Tags
^^^^

Tags begin with exclamation marks (``!``) and give the YAML parser additional
information about the data type of the object it is next to.

Tron uses *application-specific tags* to determine how to parse its config
file. The most prominent example is the mandatory data type line at the top
of the file::

    --- !TronConfiguration

While Tron is able to figure out basic data types such as ``!Node``, ``!Job``,
``!Action``, and ``!Service``, you must always use a tag for ``!NodePool`` or
Tron will try to interpret it as a ``!Node``. It is generally a good idea to
always use the tag for whatever data type you are writing so as to avoid
ambiguity.

The remaining examples in this file will all use the correct tags.

.. _command_context_variables:

Command Context Variables
^^^^^^^^^^^^^^^^^^^^^^^^^

All **command** attribute values are run through Python's string templating
function, and some variables are provided. For example::

    jobs:
        - &command_context_demo !Job
          name: "command_context_demo"
          node: *node1
          schedule: "1st monday in june"
          actions:
            - &print_run_id
                name: "print_run_id"
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

    ssh_options: !SSHOptions
        agent: false
        identities:
            - /home/batch/.ssh/id_dsa-nopasswd

Notification Options
--------------------

**notification_options**
    Email settings for sending failure notices.

        notification_options: !NotificationOptions
            smtp_host: localhost
            notification_addr: batch+errors@example.com

Command Context
---------------

**command_context**
    Dictionary of custom :ref:`command context variables
    <command_context_variables>`.

    ::

        command_context:
            PYTHON: /usr/bin/python
            TMPDIR: /tmp

.. Keep this synchronized with man_tronfig

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
    List of `Node` and `NodePool` objects. Each one should have an anchor or
    it won't be able to be used by anything else in the file.

Example::

    nodes:
        - &node1
            hostname: 'batch1'
        - &node2
            hostname: 'batch2'
        - &pool !NodePool
            nodes: [*node1, *node2]

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
