Tutorial
========

To get Tron up and running, you need:

* Tron to be installed (see below)
* A management node to run the tron daemon on
* A set of nodes to run your processes and services on (can include the
  management node)
* SSH keys and a user that will allow the tron daemon to login to all your
  working nodes

Installing Tron
---------------

The easiest way to install Tron is from PyPI::

    > sudo pip install tron

This will install :py:mod:`tron` and its dependencies (currently
:py:mod:`twisted` and :py:mod:`PyYAML`).

If you're working on Tron itself, use a `virtualenv` to isolate the
dependencies.

If you'd rather install from source, you must also install the dependencies
by hand, via :command:`pip` or your package manager. Some package managers
may also require you to install :py:mod:`pyasn1` due to missing package
dependencies.

Starting Tron
-------------

Tron runs as a single daemon, :command:`trond`.

On your management node, run::

    > sudo -u <tron user> trond

What user you choose to run tron as is very important. The tron daemon will
need SSH access to all your worker nodes, as well as permission to write to
certain directories in /var for storing state (unless you change the working
directory). If you want to further control permissions, take a look at the
multitude of options for trond.

Now check to make sure `trond` is running::

    > tronview
    Connected to tron server http://localhost:8089

    Services:
    No services

    Jobs:
    No jobs
  
Configuring Tron
----------------

There are a few options on how to configure tron, but the most straightforward
is through tronfig::

    > tronfig
  
This will open your configured :envvar:`$EDITOR` with the current configuration
file. Edit your file to be something like this::

    --- !TronConfiguration
    ssh_options:
        agent: true

    notification_options:
        smtp_host: localhost
        notification_addr: <your email address>

    nodes:
        - &node0
            hostname: 'localhost'

    jobs:
        - &node_info
            name: "getting_node_info"
            node: *node0
            schedule: "interval 10 mins"
            actions:
                - &unameAction
                    name: "uname"
                    command: "uname -a"
                - 
                    name: "cpu_info"
                    command: "cat /proc/cpuinfo"
                    requires: [*unameAction]

After you exit your editor, the configuration will be validated and uploaded to `trond`

Now if you run :command:`tronview` again, you'll see ``getting_node_info`` as a
configured job. Note that it is configured to run 10 minutes from now. This
should give you time to examine the job to ensure you really want to run it.

::

    Services:
    No services

    Jobs:
    Name              State      Scheduler            Last Success        
    getting_node_info ENABLED    INTERVAL:0:10:00     None

You can quickly disable a job by using :command:`tronctl`::

    > tronctl disable getting_node_info
    Job getting_node_info is disabled

This will stop scheduled jobs and prevent anymore from being scheduled. You are
now in manual control. To manually execute a job immediately, do this::

    > tronctl start getting_node_info
    New job getting_node_info.1 created

You can monitor this job run by using :command:`tronview`::

    > tronview getting_node_info.1
    Job Run: getting_node_info.1
    State: SUCC
    Node: localhost

    Action ID & Command  State  Start Time           End Time             Duration  
    .uname               SUCC   2011-02-28 16:57:48  2011-02-28 16:57:48  0:00:00   
    .cpu_info            SUCC   2011-02-28 16:57:48  2011-02-28 16:57:48  0:00:00   

    > tronview getting_node_info.1.uname
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
