.. _tronctl:

tronctl
=======

Synopsys
--------

**tronctl** [**--server** *server_name*] [**--verbose** | **-v**] *command* *<job_name | job_run_id | action_run_id>*

Description
-----------

**tronctl** is the control interface for tron. Through tronctl you can start,
cancel, succeed, and fail job runs and action runs.

Options
-------

--server=<config-file>      Config file containing the address of the server the
                            tron instance is running on
--verbose                   Displays status messages along the way
--run-date=<YYYY-MM-DD>     For starting a new job, specifies the run date that
                            should be set. Defaults to today.

Job Commands
------------

disableall
    Disables all jobs

enableall
    Enables all jobs

disable <job_name>
    Disables the specified job. Cancels all scheduled and queued runs. Doesn't
    schedule any more.

enable <job_name>
    Enables the specified job by starting the oldest job that still needs to run

start <job_name>
    Creates a new run of the specified job. If no other instance is running, it starts

start <job_run_id>
    Tries to start the given job or action run. A Job run only starts if no
    other instance is running. If the job has already started, start continues
    by retrying failed runs Valid states that you can run "start" on: SCHE,
    FAIL, QUE, CANC, UNKWN

start <action_run_id>
    Starts the action run regardless of anything else running.  Valid states
    that you can run "start" on: SCHE, FAIL, QUE, CANC, UNKWN

restart <job_run_id>
    Resets the given Job Run and starts it over.  Valid states that you can run
    "restart" on: SCHE, FAIL, QUE, CANC, UNKWN

cancel <job_run_id | action_run_id>
    Cancels the specified job run or action run.  Cancelled runs don't start at
    their scheduled time and they are skipped over when there is a job run
    queue.  Valid states that you can run "cancel" on: SCHE, QUE

succeed <job_run_id | action_run_id>
    Marks the specified job run or action run as succeeded.  This behaves the
    same as the run actually completing.  Dependant actions are ran and queued
    runs start.  Valid states that you can run "succeed" on: SCHE, FAIL, QUE,
    CANC, UNKWN

fail <job_run_id | action_run_id>
    Marks the specified job run or action run as failed.  This behaves the same
    as the job actually failing.  Dependant actions are queued and following
    jobs are queued or cancelled Valid states that you can run "fail" on: SCHE,
    QUE, CANC, UNKWN

Service Commands
----------------

start <service name>
    Start instances the named service

stop <service name>
    Stop instances of the named service

zap <service_id | service_instance_id>
    Marks the specified service or service instance as **DOWN** without
    taking any other action (such as actually stopping the service)

Examples
--------

::

    $ tronctl start job0
    New Job Run job0.2 created

    $ tronctl start job0.3
    Job Run job0.3 now in state RUNN

    $ tronctl cancel job0.4
    Job Run job0.4 now in state CANC

    $ tronctl fail job0.4
    Job Run job0.4 now in state FAIL

    $ tronctl restart job0.4
    Job Run job0.4 now in state RUNN

    $ tronctl succeed job0.5
    Job Run job0.5 now in state SUCC

Bugs
----

Post bugs to http://www.github.com/yelp/tron/issues.

See Also
--------

**trond** (8), **tronfig** (1), **tronview** (1),
