.. _tronctl:

tronctl
=======

Synopsis
--------

``tronctl [--server <host:port>] [--verbose] <command> <job_name | job_run_id | action_run_id>``

Description
-----------

**tronctl** is the control interface for Tron. :command:`tronctl` allows you to
enable, disable, start, stop and cancel Tron Jobs and Services.

Options
-------

``--server=<config-file>``
    Config file containing the address of the server the tron instance is running on

``--verbose``
        Displays status messages along the way

``--run-date=<YYYY-MM-DD>``
        For starting a new job, specifies the run date that should be set. Defaults to today.

``--start-date=<YYYY-MM-DD>``
        For backfills, specifies the starting date of the first job of the backfill.
        Note that many jobs operate on the previous day's data.

``--end-date=<YYYY-MM-DD>``
        For backfills, specifies the final date of the backfill. Defaults to today.
        Note that many jobs operate on the previous day's data.

Job Commands
------------

disable <job_name>
    Disables the job. Cancels all scheduled and queued runs. Doesn't
    schedule any more.

enable <job_name>
    Enables the job and schedules a new run.

start <job_name>
    Creates a new run of the specified job and runs it immediately.

start <job_run_id>
    Attempt to start the given job run. A Job run only starts if no
    other instance is running. If the job has already started, it will attempt
    to start any actions in the SCH or QUE state.

start <action_run_id>
    Attempt to start the action run.

restart <job_run_id>
    Creates a new job run with the same run time as this job.

retry <action_run_id>
    Re-run an action within an existing job run.

rerun <job_run_id>
    Creates a new job run with the same run time as this job (same as restart).

backfill <job_id>
    Creates a series of start jobs for a sequence of dates.
    --start-date must be provided for a backfill.

cancel <job_run_id | action_run_id>
    Cancels the specified job run or action run.

success <job_run_id | action_run_id>
    Marks the specified job run or action run as succeeded.  This behaves the
    same as the run actually completing.  Dependent actions are run and queued
    runs start.

skip <action_run_id>
    Marks the specified action run as skipped.  This allows dependent actions
    to run.

fail <job_run_id | action_run_id>
    Marks the specified job run or action run as failed.  This behaves the same
    as the job actually failing.

stop <action_run_id>
    Stop an action run

kill <action_run_id>
    Force stop (SIGKILL) an action run


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

    $ tronctl success job0.5
    Job Run job0.5 now in state SUCC

    $ tronctl retry MASTER.job.5.action1
    Retrying ActionRun: MASTER.job.5.action1

Bugs
----

Post bugs to http://www.github.com/yelp/tron/issues.

See Also
--------

**trond** (8), **tronfig** (1), **tronview** (1),
