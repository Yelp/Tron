What's New
==========

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
