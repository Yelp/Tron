.. _tronview:

tronview
========

Synopsys
--------

**tronview** [**-n** *numshown*] [**--server** *server_name*] [**--verbose** | **-v**] [*job_name* | *job_run_id* | *action_run_id*]

Description
-----------

**tronview** displays the status of tron scheduled jobs and services.

tronview
    Show all configured jobs and services

tronview <job_name|service_name>
    Shows details for specied job or service. Ex::

    > tronview my_job

tronview <job_run_id|service_instance_id>
    Show details for specific run or instance. Ex::

    > tronview my_job.0

tronview <action_run_id>
    Show details for specific action run. Ex::

    > tronview my_job.0.my_action

Options
-------

--version
    show program's version number and exit

-h, --help
    show this help message and exit

-v, --verbose
    Verbose logging

-n NUM_DISPLAYS, --numshown=NUM_DISPLAYS
    The maximum number of job runs or lines of output to display(0 for show
    all).  Does not affect the display of all jobs and the display of actions
    for given job.

--server=SERVER
    Server URL to connect to

-z, --hide-preface
    Don't display preface

-c, --color
    Display in color

-o, --stdout
    Solely displays stdout

-e, --stderr
    Solely displays stderr

-w, --warn
    Solely displays warnings and errors

--events
    Show events for the specified entity

Job States
----------

Jobs will be described with the following states:

ENABLED
    Scheduled and ready to go
DISABLED
    No job runs scheduled
RUNNING
    Job run currently in progress

Job Run States
--------------

SCHE
    The run is scheduled for a specific time
RUNN
    The run is currently running
SUCC
    The run completed successfully 
FAIL
    The run failed
QUE
    The run is queued behind another run(s) and will start when said runs finish
CANC
    The run is cancelled. Does not run at scheduled time and the job run queue
    ignores the run
UNKWN
    The run is in and unknown state.  This state occurs when tron restores a
    job that was running at the time of shutdown

Service States
--------------

STARTING
    The service has been started. The service will remain in this state until
    the first monitor interval runs.
UP
    The service is running normally, all instances were available during the
    last monitor period
DEGRADED
    One or more instances of the service are unexpectedly not available.
FAILED
    All instances of the service are unexpectedly unavailable.
DOWN
    Service has been stopped

Bugs
----

Post bugs to http://www.github.com/yelp/tron/issues.

See Also
--------

**trond** (8), **tronctl** (1), **tronfig** (1),
