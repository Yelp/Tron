Jobs
====

A job consists of a name, a node/node pool, set of actions, schedule, and
optional cleanup action. They are periodic events that do not interact with
other jobs while running.

If all actions exit with status 0, the job has succeeded. If any action exists
with a nonzero status, the job has failed.

.. Keep this up to date with man_tronfig.rst

Required Fields
---------------

**name**
    Name of the job. Used in :command:`tronview` and :command:`tronctl`.

**node**
    Reference to the node or pool to run the job in. If a pool, the job is
    run in a random node in the pool. This is an alias to an anchor specified
    in **nodes**.

**schedule**
    When to run this job. Schedule fields can take multiple forms. See
    :ref:`job_scheduling`.

**actions**
    List of :ref:`actions <job_actions>`.

Optional Fields
---------------

**queueing** (default **True**)
    If a job run is still running when the next job run is to be scheduled,
    add the next run to a queue if this is **True**. Otherwise, drop it.

**run_limit** (default **50**)
    Number of previous runs to store output and state for.

**all_nodes** (default **False**)
    If **True** and **node** is a node pool, run this job on each node in the
    node pool list. If a node appears more than once in the list, the job will
    be run on that node once for each appearance.

    If **False** and **node** is a node pool, run this job on a random node
    from the node pool list. If a node appears more than once in the list, the
    job will be more likely to run on that node, proportionate to the number of
    appearances.

    If **node** is not a node pool, this option has no effect.

**cleanup_action**
    Action to run when either all actions have succeeded or the job has failed.
    See :ref:`job_cleanup_actions`.

.. _job_actions:

Actions
-------

Actions consist primarily of a **name** and **command**. An action's command is
executed as soon as its dependencies (specified by **requires**) are satisfied.
So if your job has 10 actions, 1 of which depends on the other 9, then Tron
will launch the first 9 actions in parallel and run the last one when all have
completed successfully.

If any action exits with nonzero status, the job is aborted and no further
actions are run.

Required Fields
^^^^^^^^^^^^^^^

**name**
    Name of the action. Used in :command:`tronview` and :command:`tronctl`.

**command**
    Command to run on the specified node.

Optional Fields
^^^^^^^^^^^^^^^

**requires**
    List of pointers to actions that must complete successfully before this
    action is run. These actions must have been specifid earlier in the config
    file.

**node**
    Node or node pool to run the action on if different from the rest of the
    job. This is an alias to an anchor specified in **nodes**.

Example Actions
^^^^^^^^^^^^^^^

Here is a typical job setup. The configuration would work without the ``!Job``
and ``!Action`` tags, but it would produce worse error messages if there were
problems. The ``!DailyScheduler`` tag is necessary for Tron to know what kind
of scheduler you are using.

::

    # ...
    jobs:
        - !Job
            name: convert_logs
            node: *node1
            schedule: !DailyScheduler
                start_time: 04:00:00
            actions:
                - &verify_logs_present !Action
                    name: verify_logs_present
                    command: >
                        ls /var/log/app/log_%(shortdate-1).txt
                - &convert_logs !Action
                    name: convert_logs
                    command: >
                        convert_logs /var/log/app/log_%(shortdate-1).txt \
                            /var/log/app_converted/log_%(shortdate-1).txt
                    requires: [*verify_logs_present]

.. _job_scheduling:

Scheduling
----------

Tron supports three different kinds of schedules in config files.

Interval
^^^^^^^^

Run the job every X seconds, minutes, hours, or days. The time expression
is ``<int>[ ]months|days|hours|minutes|seconds``, where the units can be
abbreviated.

::

    schedule: "interval 20s"

::

    schedule: !IntervalScheduler
        interval: "5 mins"

Daily
^^^^^

Run the job on specific weekdays at a specific time. The time expression is
``HH:MM:SS[ [MTWRFSU]]``.

::

    schedule: "daily 04:00:00"

::

    schedule: "daily 04:00:00 MWF"

::

    schedule: !DailyScheduler
        start_time: "07:00:00"
        days: "MWF"

Complex
^^^^^^^

More powerful version of the daily scheduler based on the one used by Google
App Engine's cron library. To use this scheduler, use a string in this format
as the schedule::

    ("every"|ordinal) (days) ["of|in" (monthspec)] (["at"] HH:MM)

**ordinal**
    Comma-separated list of "1st" and so forth. Use "every" if you don't want
    to limit by day of the month.

**days**
    Comma-separated list of days of the week (for example, "mon", "tuesday",
    with both short and long forms being accepted); "every day" is equivalent
    to "every mon,tue,wed,thu,fri,sat,sun"

**monthspec**
    Comma-separated list of month names (for example, "jan", "march", "sep").
    If omitted, implies every month. You can also say "month" to mean every
    month, as in "1,8th,15,22nd of month 09:00".

**HH:MM**
    Time of day in 24 hour time.

Some examples::

    2nd,third mon,wed,thu of march 17:00
    every monday at 09:00
    1st monday of sep,oct,nov at 17:00
    every day of oct at 00:00

In the config::

    schedule: "every monday at 09:00"

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
regardless of success or failure, such as intermedmiate files or Amazon Elastic
MapReduce job flows, you can use cleanup actions to tear them down.

The command context variable ``cleanup_job_status`` is provided to cleanup
actions and has a value of ``SUCCESS`` or ``FAILURE`` depending on the job's
final state. For example::

    - !Job
        # ...
        cleanup_action: !CleanupAction
            command: "python -m mrjob.tools.emr.job_flow_pool --terminate MY_POOL"

.. Keep this up to date with man_tronfig.rst

States
------

Job States
^^^^^^^^^^

**ENABLED**
    Scheduled and ready to go

**DISABLED**
    No job runs scheduled

**RUNNING**
    Job run currently in progress

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
    The run is cancelled. Does not run at scheduled time and the job run queue
    ignores the run

**UNKWN**
    The run is in and unknown state.  This state occurs when tron restores a
    job that was running at the time of shutdown
