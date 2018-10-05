Jobs
====

A job consists of a name, a node/node pool, a list of actions, a schedule, and
an optional cleanup action. They are periodic events that do not interact with
other jobs while running.

If all actions exit with status 0, the job has succeeded. If any action exists
with a nonzero status, the job has failed.


Required Fields
---------------

**name**
    Name of the job. Used in :command:`tronview` and :command:`tronctl`.

**node**
    Reference to the node or pool to run the job in. If a pool, the job is
    run in a random node in the pool.

**schedule**
    When to run this job. Schedule fields can take multiple forms. See
    :ref:`job_scheduling`.

**actions**
    List of :ref:`actions <job_actions>`.

Optional Fields
---------------

**monitoring** (default **{}**)
    (Beta Feature) Dictionary of key: value pairs to inform the monitoring
    framework on how to alert teams for job failures.

**queueing** (default **True**)
    If a job run is still running when the next job run is to be scheduled,
    add the next run to a queue if this is **True**. Otherwise, cancel
    the job run. Note that if the scheduler used for this job is
    not defined to queue overlapping then this setting is ignored.
    The ConstantScheduler will not queue overlapping.

**allow_overlap** (default **False**)
    If **True** new job runs will start even if the previous run is still running.
    By default new job runs are either cancelled or queued (see **queuing**).

**run_limit** (default **50**)
    Number of runs which will be stored. Once a Job has more then run_limit
    runs, the output and state for the oldest run are removed. Failed runs
    will not be removed.

**all_nodes** (default **False**)
    If **True** run this job on each node in the
    node pool list. If a node appears more than once in the list, the job will
    be run on that node once for each appearance.

    If **False** run this job on a random node
    from the node pool list. If a node appears more than once in the list, the
    job will be more likely to run on that node, proportionate to the number of
    appearances.

    If **node** is not a node pool, this option has no effect.

**cleanup_action**
    Action to run when either all actions have succeeded or the job has failed.
    See :ref:`job_cleanup_actions`.

**enabled** (default **True**)
    If **False** the job will not be scheduled to run. This configuration option
    is only relevant when a Job is first added to the configuration, after
    which this value will be ignored.

**max_runtime** (default **None**)
    A time interval (ex: "2 hours") that limits the duration of each job run.
    If the job run is still running after this duration, all of it's actions
    are sent SIGTERM.

    Note: This requires an :ref:`action_runners` to be configured. If
    `action_runner` is none max_runtime does nothing.

**time_zone** (default **None**)
    Time zone used for calculating when a job should run. Defaults to
    None, which means it will use the default time_zone set in the master
    config.

**expected_runtime** (default **24h**)
    A time interval (ex: "2 hours") that specifies the maximum expected duration of each job run.
    Monitoring will alert if a job run is still running after this duration.
    Use max_runtime instead if hard limit is needed.


.. _job_actions:

Actions
-------

Actions consist primarily of a **name** and **command**. An action's command is
executed as soon as its dependencies (specified by **requires**) are satisfied.
So if your job has 10 actions, 1 of which depends on the other 9, then Tron
will launch the first 9 actions in parallel and run the last one when all have
completed successfully.

If any action exits with nonzero status, the job will continue to run any
actions which do not depend on the failed action.


Required Fields
^^^^^^^^^^^^^^^

**name**
    Name of the action. Used in :command:`tronview` and :command:`tronctl`.

**command**
    Command to run on the specified node. A common mistake here is to use
    shell expansions or expressions in your command. Commands are run using
    ``exec`` so bash (or other shell) expressions will not work, and could
    cause the job to fail.

Optional Fields
^^^^^^^^^^^^^^^

**requires**
    List of action names that must complete successfully before this
    action is run. Actions can only require actions in the same job.

**node**
    Node or node pool to run the action on if different from the rest of the
    job.

**retries**
    An integer representing how many times Tron is allowed to automatically
    retry the command. Tron will immediately re-run the command if it fails,
    and the action will not enter the failed state until retries are exhausted.
    Defaults to None (0 retries allowed).

**retries_delay** (beta)
    A timedelta to wait in between retries.

**expected_runtime** (default **24h**)
    A time interval (ex: "2 hours") that specifies the maximum expected duration of each action run.
    Monitoring will alert if a action run is still running after this duration.

Example Actions
^^^^^^^^^^^^^^^

::

    jobs:
        - name: convert_logs
          node: node1
          schedule:
            start_time: 04:00:00
          actions:
            - name: verify_logs_present
              command: "ls /var/log/app/log_{shortdate-1}.txt"
            - name: convert_logs
              command: "convert_logs /var/log/app/log_{shortdate-1}.txt /var/log/app_converted/log_{shortdate-1}.txt"
              requires: [verify_logs_present]

.. _job_scheduling:

Scheduling
----------

Tron supports four methods for configuring the schedule of a job. Schedulers
support a jitter parameter that allows them to vary their runtime by a
random time delta.


Interval
^^^^^^^^

Run the job every X seconds, minutes, hours, or days. The time expression
is ``<interval> days|hours|minutes|seconds``, where the units can be
abbreviated.

Short form::

    schedule: "interval 20s"

Long form::

    schedule:
        type:   "interval"
        value:  "5 mins"
        jitter: "10 seconds"        # Optional

With alias::

    schedule:
        type:   "interval"
        value:  "hourly"

Daily
^^^^^

Run the job on specific days at a specific time. The time expression is
``HH:MM:SS[ MTWRFSU]``.

Short form::

    schedule: "daily 04:00:00"

Short form with days::

    schedule: "daily 04:00:00 MWF"

Long form::

    schedule:
        type:   "daily"
        value:  "07:00:00 MWF"
        jitter: "10 min"            # Optional

Cron
^^^^

Schedule a job using cron syntax.  Tron supports predefined schedules, ranges,
and lists for each field. It supports the *L* in day of month field only (which
schedules the job on the last day of the month). Only one of the day fields
(day of month and day of week) can have a value.


Short form::

    schedule: "cron */5 * * 7,8 *"  # Every 5 minutes in July and August

::

    schedule: "cron 0 3-6 * * *"    # Every hour between 3am and 6am

Long form::

    schedule:                       # long form
        type: "cron"
        value: "30 4 L * *"         # The last day of the month at 4:30am


Complex
^^^^^^^

More powerful version of the daily scheduler based on the one used by Google
App Engine's cron library. To use this scheduler, use a string in this format
as the schedule::

    ("every"|ordinal) (days) ["of|in" (monthspec)] (["at"] HH:MM)

**ordinal**
    Comma-separated list of ``1st`` and so forth. Use ``every`` if you don't want
    to limit by day of the month.

**days**
    Comma-separated list of days of the week (for example, ``mon``, ``tuesday``,
    with both short and long forms being accepted); ``every day`` is equivalent
    to ``every mon,tue,wed,thu,fri,sat,sun``

**monthspec**
    Comma-separated list of month names (for example, ``jan``, ``march``, ``sep``).
    If omitted, implies every month. You can also say ``month`` to mean every
    month, as in ``1,8th,15,22nd of month 09:00``.

**HH:MM**
    Time of day in 24 hour time.

Some examples::

    2nd,third mon,wed,thu of march 17:00
    every monday at 09:00
    1st monday of sep,oct,nov at 17:00
    every day of oct at 00:00

In the config::

    schedule: "every monday at 09:00"

::

    schedule:
        type: "groc daily"
        value: "every day 11:22"
        jitter: "5 min"

.. _dst_notes:

Notes on Daylight Saving Time
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Some system clocks are configured to track local time and may observe daylight
savings time. For example, on November 6, 2011, 1 AM occurred twice.  Prior to
version 0.2.9, this would cause Tron to schedule a daily midnight job to be run
an hour early on November 7, at 11 PM. For some jobs this doesn't matter, but
for jobs that depend on the availability of data for a day, it can cause a
failure.

Similarly, some jobs on March 14, 2011 were scheduled an hour late.

To avoid this problem, set the :ref:`time_zone` config variable. For example::

    time_zone: US/Pacific

If a job is scheduled at a time that occurs twice, such as 1 AM on "fall back",
it will be run on the *first* occurrence of that time.

If a job is scheduled at a time that does not exists, such as 2 AM on "spring
forward", it will be run an hour later in the "new" time, in this case 3 AM. In
the "old" time this is 2 AM, so from the perspective of previous jobs, it runs
at the correct time.

In general, Tron tries to schedule a job as soon as is correct, and no sooner.
A job that is schedule for 2:30 AM will not run at 3 AM on "spring forward"
because that would be half an hour too soon from a pre-switch perspective (2
AM).

.. note::

    If you experience unexpected scheduler behavior, `file an issue on Tron's
    Github page <http://www.github.com/yelp/tron/issues/new>`_.

.. _job_cleanup_actions:

Cleanup Actions
---------------

Cleanup actions run after the job succeeds or fails. They are specified just
like regular actions except that there is only one per job and it has no name
or requirements list.

If your job creates shared resources that should be destroyed after a run
regardless of success or failure, such as intermediate files or Amazon Elastic
MapReduce job flows, you can use cleanup actions to tear them down.

The command context variable ``cleanup_job_status`` is provided to cleanup
actions and has a value of ``SUCCESS`` or ``FAILURE`` depending on the job's
final state. For example::

    -
        # ...
        cleanup_action:
          command: "python -m mrjob.tools.emr.job_flow_pool --terminate MY_POOL"


States
------

The following are the possible states for a Job and Job Run.

Job States
^^^^^^^^^^

**ENABLED**
    A run is scheduled and new runs will continue to be scheduled.

**DISABLED**
    No new runs will be scheduled, and scheduled runs will be cancelled.

**RUNNING**
    Job run currently in progress.

Job Run States
^^^^^^^^^^^^^^

**SCHE**
    The run is scheduled for a specific time

**RUNN**
    The run is currently running

**SUCC**
    The run completed successfully

**FAIL**
    The run failed

**QUE**
    The run is queued behind another run(s) and will start when said runs finish

**CANC**
    The run was scheduled, but later cancelled.

**UNKWN**
    The run is in and unknown state.  This state occurs when tron restores a
    job that was running at the time of shutdown.


Action States
^^^^^^^^^^^^^

Job states are derived from the aggregate state of their actions.  The following
is a state diagram for an action.

.. image:: images/action.png
    :width: 680px
