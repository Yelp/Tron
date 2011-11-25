Jobs
====

A job consists of a name, a node/node pool, set of actions, schedule, and
optional cleanup action. They are periodic events that do not interact with
other jobs while running.

.. Keep this up to date with man_tronfig.rst

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

**queueing** (default **True**)
    If a job run is still running when the next job run is to be scheduled,
    add the next run to a queue if this is **True**. Otherwise, drop it.

**run_limit** (default 50)
    Number of previous runs to store output and state for.

**all_nodes** (default **False**)
    If **True**, run on all nodes in the node pool if **node** is a node pool.
    Otherwise, run on a single random node in the pool.

**cleanup_action**
    Action to run when either all actions have succeeded or the job has failed.
    See :ref:`job_cleanup_actions`.

.. _job_actions:

Actions
-------

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
    action is run.

**node**
    Node or node pool to run the action on if different from the rest of the
    job.

Example Actions
^^^^^^^^^^^^^^^

Here is a typical job setup. The configuration would work without the ``!Job``
and ``!Action`` tags, but it would produce worse error messages if there were
problems. The ``!DailyScheduler`` tag is necessary for Tron to know what kind
of scheduler you are using.

::

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

    every 12 hours
    every 5 minutes from 10:00 to 14:00
    2nd,third mon,wed,thu of march 17:00
    every monday 09:00
    1st monday of sep,oct,nov 17:00
    every day 00:00

In the config::

    schedule: "every 12 hours"

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
        cleanup_action:
            command: "python -m mrjob.tools.emr.job_flow_pool --terminate MY_POOL"
