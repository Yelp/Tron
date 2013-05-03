What's New
==========

0.6.1
-----
* tronweb was replaced with a clientside version
* more ssh options are now configurable
* adding an experimental feature to support a max_runtime on jobs
* adding tronctl kill to SIGKILL a service
* add a `--no-header` option to tronfig

0.6.0
-----
* `action.requires` must be a list (string has been deprecated since 0.3.3)
* `tronctl zap` has been removed (it shouldn't be necessary anymore)
* service monitoring code has been re-written (services should not longer get stuck in a stopping state)
* hosts can not be validated by specifying a `known_hosts` file
* additional validation for ssh options and context variables has been moved into configuration validation
* tronview now displays additional details about jobs and services
* the API has changed slightly (href is now url, service status is now state)

0.5.2
-----
* Tron now supports the ability to use different users per node connection.
* Fragmented configuration is now possible by using namespaced config files.
* Additional cleanup and stability patches have been applied.
* State persistence configuration can now be changed without restarting `trond`
* State saving now includes a namespace, you will need to run
  `tools/migration/migrate_state.py` to migrate old state.
* `trond` now expects a configuration directory. Use
  `tools/migration/migrate_config_0.5.1_to_0.5.2.py` to convert your existing
  config to the new format.
* Patched an issue with SSH connections that caused an exception on
  channel close

0.5.1
-----
* Jobs which are disabled will no longer be re-enabled when part of their
  configuration changes.
* Individual actions for a Job can no longer be started independently before
  a job is started. This was never intentionally supported.
* Adding a new configuration option `allow_overlap` for Jobs, which allows
  job runs to overlap each other.
* Jobs can now be configured using crontab syntax. see :ref:`job_scheduling`


0.5.0
-----
* Names for nodes, jobs, actions and service can now contain underscore characters
  but are restricted to 255 characters.
* trond now supports a graceful shutdown. Send trond SIGINT to have it wait for
  all currently running jobs to complete before shutting down. SIGTERM
  also performs some cleanup before terminating.
* State serialization has changed.  See :ref:`config_state` for configuration
  options.  `tools/migration/migrate_state.py` is included to migrate your
  existing Tron state to a new store.  YAML store is now deprecated.
* All relative path options to :ref:`trond` and relative paths in the configuration
  will now be relative to the ``--working-dir`` directory instead of the current
  working directory.
* Old style config, which was deprecated in 0.3 will no longer work.


0.4.1
-----
* :ref:`tronview` will once again attempt to find the tty width even when stdout is not a tty.
* Fixed last_success for job context.
* Job runs which are manually cancelled will now continue to schedule new runs.


0.4.0
-----

* Jobs now continue to run all possible actions after one of its actions fail
* Enabling a disabled job now schedules the next run using current time instead
  of the last successful run (which could cause many runs to be
  scheduled in the past if the job had been disabled for a while)
* Command context is now better defined. see :ref:`built_in_cc`. Also adds support for a
  last_success keyboard which supports date arithmetic.
* Resolved many inconsistencies and bugs around Job scheduling.


0.3.3
-----

* Logging is now configured from logging.conf, see :ref:`config_logging`
* Old style configuration files can be converted using `tools/migration/migrate_config_0.2_to_0.3.py`
* working_dir in the configuration has been replaced by output_stream_dir


0.3.0
-----

* **!** (tags), **\*** (references), and **&** (anchors) are now deprecated in the :ref:`trond`
  configuration file.  Support will be removed for them in 0.5.
* Adding an enabled option for jobs, so they can be configured as disabled by default
* tron commands (:ref:`tronview`, :ref:`tronfig`, :ref:`tronctl`) now support a global
  config (defaults to /etc/tron/tron.yaml)
* tronview will now pipe its output through ``less`` if appropriate


0.2.10
------

* ssh_options is actually optional
* Cleanup actions no longer cause jobs using an interval scheduler to stop being scheduled if an action fails
* Failed actions can be skipped, causing dependent actions to run


0.2.9
-----

* :ref:`tronweb` works and is documented.
* Daylight Savings Time behavior is more well-defined. See :ref:`dst_notes` for
  more information.
* Jobs that fail after running over their next scheduled time are no longer
  forgotten.
* Reconfiguring syslog no longer requires restarting `trond` to take effect.
* Syslog formatter is more meaningful (0.2.8.1).
* Prebuilt man pages are included so you don't need Sphinx to have them
  (0.2.8.1).

0.2.8
-----

Features
^^^^^^^^

* New HTML documentation. Hello!
* Cleanup actions let you run a command after the success or failure of a job.
  You can use them to clean up temp files, shut down Elastic MapReduce job
  flows, and more. See :ref:`job_cleanup_actions`.
* Log to syslog by setting **syslog_address** in your config. See
  :ref:`config_logging`.
* "zap" command for services lets you force Tron to see a service or service
  instance as **DOWN**. See :doc:`man_tronctl`.
* ``simplejson`` is no longer a dependency for Python 2.6 and up

Bug Fixes
^^^^^^^^^

* Fixed weekday-specified jobs (mon, tues, ...) running a day late
* Fixed services being allowed in jobs list and causing weird crashes
* Fixed missing import in www.py
