Tutorial
========

To install Tron you will need:

* A copy of the most recent Tron release from either
  `github <http://github.com/yelp/Tron>`_ or `pypi <http://pypi.python.org/pypi/tron>`_
  (see :ref:`installing_tron`).
* A server on which to run :command:`trond`.
* One or more batch boxes which will run the Jobs.
* An SSH key and a user that will allow the tron daemon to login to all of the
  batch machines without a password prompt.

.. _installing_tron:

Installing Tron
---------------

The easiest way to install Tron is from PyPI::

    $ sudo pip install tron

You can also get a copy of the current development release from
`github <http://github.com/yelp/Tron>`_. See `setup.py` in the source package
for a full list of required packages.

If you are interested in working on Tron development see :ref:`developing`
for additional requirements and setting up a dev environment.


Running Tron
-------------

Tron runs as a single daemon, :command:`trond`.

On your management node, run::

    $ sudo -u <tron user> trond

The chosen user will need SSH access to all your worker nodes, as well as
permission to write to the working directory, log file, and lock file
(see ``trond --help`` for defaults).  You can change these directories using
command line options. Also see :ref:`config_logging` on how to change the
default logging settings.


Once :command:`trond` is running, you can view its status using :command:`tronview`
(by default tronview will connect to localhost, use ``--server=<host>:<port> -s``
to specify a different server, and have that setting saved in ``~/.tron``)::

    $ tronview

    Jobs:
    No jobs

Configuring Tron
----------------

There are a few options on how to configure tron, but the most straightforward
is through tronfig::

    $ tronfig

This will open your configured :envvar:`$EDITOR` with the current configuration
file. Edit your file to be something like this::

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

After you exit your editor, the configuration will be validated and uploaded to `trond`.

Now if you run :command:`tronview` again, you'll see ``getting_node_info`` as a
configured job. Note that it is configured to run 10 minutes from now. This
should give you time to examine the job to ensure you really want to run it.

::

    Jobs:
    Name              State      Scheduler            Last Success
    getting_node_info ENABLED    INTERVAL:0:10:00     None

You can quickly disable a job by using :command:`tronctl`::

    $ tronctl disable getting_node_info
    Job getting_node_info is disabled

This will stop scheduled jobs and prevent anymore from being scheduled. You are
now in manual control. To manually execute a job immediately, do this::

    $ tronctl start getting_node_info
    New job getting_node_info.1 created

You can monitor this job run by using :command:`tronview`::

    $ tronview getting_node_info.1
    Job Run: getting_node_info.1
    State: SUCC
    Node: localhost

    Action ID & Command  State  Start Time           End Time             Duration
    .uname               SUCC   2011-02-28 16:57:48  2011-02-28 16:57:48  0:00:00
    .cpu_info            SUCC   2011-02-28 16:57:48  2011-02-28 16:57:48  0:00:00

    $ tronview getting_node_info.1.uname
    Action Run: getting_node_info.1.uname
    State: SUCC
    Node: localhost

    uname -a

    Requirements:

    Stdout:
    Linux dev05 2.6.24-24-server #1 SMP Wed Apr 15 15:41:09 UTC 2009 x86_64 GNU/Linux
    Stderr:

Tron also provides a simple, optional web UI that can be used to get tronview data in a browser. See :doc:`tronweb` for setup
instructions.

That's it for the basics. You might want to look at :doc:`overview` for a more
comprehensive description of how Tron works.
