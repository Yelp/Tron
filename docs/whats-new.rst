What's New
==========

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
