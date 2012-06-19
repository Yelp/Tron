.. _trond:

trond
=====

Synopsys
--------

``trond [--working-dir=<working dir>] [--verbose] [--debug]``

Description
-----------

**trond** is the tron daemon that manages all jobs and services.

Options
-------

``--version``
    show program's version number and exit

``-h, --help``
    show this help message and exit

``--working-dir=WORKING_DIR``
    Directory where tron's state and output is stored (default /var/lib/tron/)

``-l LOG_CONF, --log-conf=LOG_CONF``
    Logging configuration file to setup python logger

``-c CONFIG_FILE, --config-file=CONFIG_FILE``
    Configuration file to load (default in working dir)

``-v, --verbose``
    Verbose logging

``--debug``
    Debug mode, extra error reporting, no daemonizing

``--nodaemon``
    Indicates we should not fork and daemonize the process (default False)

``--pid-file=PIDFILE``
    Where to store pid of the executing process (default /var/run/tron.pid)

``-P LISTEN_PORT, --port=LISTEN_PORT``
    What port to listen on, defaults 8089

``-H LISTEN_HOST, --host=LISTEN_HOST``
    What host to listen on defaults to localhost

Files
-----

Working directory
    The directory where state and saved output of processes are stored.

Pid file
    Contains the pid of the daemonized process.

Log File
    trond error log, configured from logging.conf


Signals
-------

`SIGINT`
    Graceful shutdown. Waits for running jobs to complete.

`SIGTERM`
    Does some cleanup before shutting down.

`SIGHUP`
    Reload the configuration file.

`SIGUSR1`
    If running with ``--nodaemon`` will drop into an ipdb debugging prompt.

Logging
-------

Tron uses Python's standard logging and by default uses a rotating log file
handler that rotates files each day. Logs go to ``/var/log/tron/tron.log``.

To configure logging pass -l <logging.conf> to trond. You can modify the
default logging.conf by coping it from tron/logging.conf. See
http://docs.python.org/howto/logging.html#configuring-logging


Bugs
----

trond has issues around daylight savings time and may run jobs an hour early
at the boundary.

Post further bugs to http://www.github.com/yelp/tron/issues.

See Also
--------

**tronctl** (1), **tronfig** (1), **tronview** (1),
