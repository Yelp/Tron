
.. _built_in_cc:

Built-In Command Context Variables
==================================

Tron includes some built in command context variables that can be used in
command configuration for actions.

These variables can be used in the command of an action, using Python's format syntax (``{}``).

For example::

    # myservice.yaml
    myjob:
      node: localhost
      actions:
        myaction1:
          command: "Hello world! I'm {action} for job {name} running on {node}"

The command would get rendered at job runtime to::

    Hello world! I'm myaction1 for myservice.myjob running on localhost


**shortdate**
    Run date in ``YYYY-MM-DD`` format. Supports simple arithmetic of the
    form ``{shortdate+6}`` which returns a date 6 days in the future,
    ``{shortdate-2}`` which returns a date 2 days before the run date.

**ym, ymd, ymdh, ymdhm**
    Same as ``shortdate`` but better granularity. Arithmetic works with most
    granular unit: ``ymdh+1`` is  +1 hours, ``ymdhm+1`` is +1 minute.

**year**
    Current year in ``YYYY`` format. Supports the same arithmetic operations
    as `shortdate`. For example, ``{year-1}`` would return the year previous
    to the run date.

**month**
    Current month in `MM` format. Supports the same arithmetic operations
    as `shortdate`. For example, ``{month+2}`` would return 2 months in the
    future.

**day**
    Current day in `DD` format. Supports the same arithmetic operations
    as `shortdate`. For example, ``{day+1}`` would return the day after the
    run date.

**hour**
    Current hour in `HH` (0-23) format. Supports the same arithmetic operations
    as `shortdate`. For example, ``{hour+1}`` would return the hour after the
    run hour (mod 24).

**unixtime**
    Current timestamp. Supports addition and subtraction of seconds. For
    example ``{unixtime+20}`` would return the timestamp 20 seconds after
    the jobs runtime.

**daynumber**
    Current day number as an ordinal (datetime.toordinal()). Supports addition
    and subtraction of days. For example ``{daynumber-3}`` would be 3 days
    before the run date.

**name**
    Name of the job (e.g. ``myservice.myjob``).

**actionnname**
    The name of the action (e.g. ``myaction1``).

**node**
    Hostname of the node the action is being run on (e.g. ``localhost``).

**runid**
    Run ID of the job run (e.g. ``sample_job.23``)

**cleanup_job_status**
    ``SUCCESS`` if all actions have succeeded when the cleanup action runs,
    ``FAILURE`` otherwise. ``UNKNOWN`` if used in an action other than the
    cleanup action.

**last_success**
    The last successful run date (defaults to current date if there was no
    previous successful run). Supports date arithmetic using the form
    ``{last_success#shortdate-1}``.

**manual**
    ``true`` if the job was run manually. ``false`` otherwise.
    Manual job runs are those runs launched via the ``tronctl start`` command (as opposed to those launched by the scheduler).
    This variable is useful changing the behavior when jobs are run manually, like adding more verbose loggin::

    command: "myjob --verbose={manual}"

**namespace**
    The namespace of the config where the job comes from. Often ``MASTER`` or ``servicename``.
    Usually matches the name of service where the code runs.
    For example, if the job name is ``myservice.mycooljob.1.myaction``, ``{namespace}`` would be rendered as ``myservice``.
