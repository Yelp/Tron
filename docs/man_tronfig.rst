.. _tronfig:

tronfig
=======

Synopsys
--------

**tronfig** [**--server** *server_name* ] [**--verbose** | **-v**]

Description
-----------

**tronfig** edits the configuration for tron.  It retrieves the configuration
file for local editing, verifies the configuration, loads it back to the tron
server and makes the changes to tron.

Options
-------

--server <server_name>
    The server the tron instance is running on

--verbose
    Displays status messages along the way

--version
    Displays version string

Configuration
-------------

If you start tron without a configuration file, a template will be created for you.
 
Field are described below:

ssh_options
    These options are how we connect to the nodes we run commands on.

    agent (optional)
        boolean to indicate we should use an SSH Agent

    identities (optional)
        list of paths to SSH identity files

command_context
    Dictionary mapping variable names to values that will be interpolated in
    the command string. For example, if you include `animal: cat`, then the
    command `cat %(animal)s` will become `cat cat`.

syslog_address
    Include this if you want to enable logging to syslog. Accepts paths as strings
    and [address, port] lists for sockets. Typical values for various platforms are::

        Linux: "/dev/log"
        OS X: "/var/run/syslog"
        Windows: ["localhost", 514]

notification_options
    Who to email failures to.

        smtp_host
            SMTP server to use
        notification_addr
            Email address to send mail to

time_zone (optional)
    Local time as observed by the system clock. If your system is obeying a
    time zone with daylight savings time, then some of your jobs may run early
    or late on the days bordering each mode.

    ::

        time_zone: US/Pacific

nodes
    List of Node and NodePool objects which tron connects to.

    For node:
        hostname - Host to connect to

    For node pool:
        nodes - List of pointers to nodes in the pool

command_context
    Dictionary of environment variables that can be used inside job and service
    command strings.

jobs
    Accepts a list of Job objects. A Job objects accepts the following options:

        name
            The name of the job
        node
            Reference to the Node or NodePool object this job runs on
        schedule
            The schedule the job follows
        actions
            The list of action objects (see below) within the job
        cleanup_action (optional)
            An action (not including name or requirements) to be run after the
            success or failure of the job
        all_nodes (optional, default False)
            boolean indicating job should run on all nodes in the NodePool
        queueing  (optional, default True)
            boolean indicating overlapping job runs should queue rather than cancel
        run_limit (optional, default 50)
            Number of runs to store in history

Action objects
    These are the required options for action objects. **The exception is
    cleanup actions, which only use the 'command' option.**

    name
        Name of the action. Must be unique within the job
    command
        Command line to execute
    requires
        (optional) list of actions that must have already completed
    node
        (optional) node to run the action on (if different from the job)

services
    Services are long running processes that we will periodically monitor. A
    Service can be configured with the following options:

    name
        The name of the service (must be unique, and not conflict with jobs)
    node
        The Node or NodePool the service instances should run on
    count
        The number of instances of this service that should be created
    monitor_interval
        Seconds between monitoring the pid of this service
    restart_interval
        Seconds to wait before restarting the service
    pid_file
        Where the monitor will find the pid
    command
        Command to be executed to start a new instance

Built-In Command Context Variables
----------------------------------

shortdate

    Current date in YYYY-MM-DD format. Supports simple arithmetic of the form
    %(shortdate+6)s, %(shortdate-2)s, etc.

name
    Name of the job or service

actionname
    Name of the action

runid
    Run ID of the job or service (e.g. sample_job.23)

node
    Hostname of the node the action is being run on

cleanup_job_status
    "SUCCESS" if all actions have succeeded when the cleanup action runs,
    "FAILURE" otherwise. "UNKNOWN" if used in an action other than the cleanup
    action.

Example Configuration
---------------------

::

    --- !TronConfiguration

    ssh_options: !SSHOptions
        agent: true

    nodes:
        - &node1
            hostname: 'machine1'
        - &node2
            hostname: 'machine2'
        - &pool !NodePool
            nodes: [*node1, *node2]

    command_context:
        PYTHON: /usr/bin/python

    jobs:
        - &job0
            name: "job0"
            node: *pool
            all_nodes: True # Every time the Job is scheduled it runs on every node in its node pool
            schedule: "interval 20s"
            queueing: False
            actions:
                - &start
                    name: "start"
                    command: "echo number 9"
                    node: *node1
                - 
                    name: "end"
                    command: "echo love me do"
                    requires: [*start]

        - &job1
            name: "job1"
            node: *node1
            schedule: "interval 20s"
            queueing: False
            actions:
                - &action
                    name: "echo"
                    command: "echo %(PYTHON)s"
            cleanup_action:
                command: "echo 'cleaning up job1'"

    services:
        -
            name: "testserv"
            node: *pool
            count: 8
            monitor_interval: 60
            restart_interval: 120
            pid_file: "/var/run/%(name)s-%(instance_number)s.pid"
            command: "/bin/myservice --pid-file=%(pid_file)s start"

Files
-----

/var/lib/tron/tron.yaml
    Default path to the config file. May be changed by passing the **-c**
    option to **trond**.

Bugs
----

Post bugs to http://www.github.com/yelp/tron/issues.

See Also
--------

**trond** (8), **tronctl** (1), **tronview** (1),
