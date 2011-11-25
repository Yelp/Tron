Jobs
====

A job consists of a name, a node/node pool, set of actions, schedule, and
optional cleanup action.


.. Keep this up to date with man_tronfig.rst

Required Fields
---------------

**name**
    Name of the job. Used in :command:`tronview` and :command:`tronctl`.

**node**
    Reference to the node or pool to run the job in.

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

.. _job_cleanup_actions:

Cleanup Actions
---------------
