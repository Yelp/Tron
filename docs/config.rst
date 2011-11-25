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

SSH
^^^

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

Logging
^^^^^^^

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

See :doc:`jobs` for the options available to jobs and their actions.

Services
--------

See :doc:`services` for the options available to services.
