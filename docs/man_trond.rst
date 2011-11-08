.. We are forced to use the .SH syntax for sections due to a bug in Sphinx.

.SH SYNOPSYS

**trond** [**--working-dir=<working dir>**] [**--verbose** | **-v**] [**--debug**]

.SH DESCRIPTION

**trond** is the tron daemon that manages all jobs and actions.

.SH OPTIONS

--working-dir
    The file directory for storage of objects like config, state and stdout/stderr output

--nodaemon
    Indicates trond should not daemonize

--pid-file
    Where to store PID of the daemonized process

-l, --log-file
    Where to store logs

-P, --port
    Port to listen on (default 8089)

-H, --host
    What host to bind to (defaults to localhost)

--verbose
    Displays status messages along the way

--debug
    Enters debug mode
