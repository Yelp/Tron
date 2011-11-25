.. _trond:

trond
=====

Synopsys
--------

**trond** [**--working-dir=<working dir>**] [**--verbose** | **-v**] [**--debug**]

Description
-----------

**trond** is the tron daemon that manages all jobs and actions.

Options
-------

--version
    show program's version number and exits

-h, --help
    show this help message and exit

--working-dir=WORKING_DIR
    Directory where tron's state and output is stored (default /var/lib/tron/)

-l LOG_FILE, --log-file=LOG_FILE
    Where the logs are stored (default /var/log/tron/tron.log)

-c CONFIG_FILE, --config-file=CONFIG_FILE
    Configuration file to load (default in working dir)

-v, --verbose
    Verbose logging

--debug
    Debug mode, extra error reporting, no daemonizing

--nodaemon
    Indicates we should not fork and daemonize the process (default False)

--pid-file=PIDFILE
    Where to store pid of the executing process (default /var/run/tron.pid)

-P LISTEN_PORT, --port=LISTEN_PORT
    What port to listen on, defaults 8089

-H LISTEN_HOST, --host=LISTEN_HOST
    What host to listen on defaults to localhost

Files
-----

Working directory
    The directory where state and saved output of processes are stored.
    The config file, log file, and pid file are also stored in this directory
    by default.

Pid file
    Contains the pid of the daemonized process.

Log file
    trond logs messages here in addition to other logging you have set up.

State file
    trond saves state here when it terminates and reloads it when it starts
    up again.

Bugs
----

trond has issues around daylight savings time and may run jobs an hour early
at the boundary.

Post further bugs to http://www.github.com/yelp/tron/issues.

See Also
--------

**tronctl** (1), **tronfig** (1), **tronview** (1),
