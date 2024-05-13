.. _tronview:

tronview
========

Synopsis
--------

``tronview [-n <numshown>] [--server <server_name>] [--verbose] [<job_name> | <job_run_id> | <action_run_id>]``

Description
-----------

**tronview** displays the status of tron scheduled jobs.

tronview
    Show all configured jobs

tronview <job_name>
    Shows details for a job. Ex::

    $ tronview my_job

tronview <job_run_id>
    Show details for specific run or instance. Ex::

    $ tronview my_job.0

tronview <action_run_id>
    Show details for specific action run. Ex::

    $ tronview my_job.0.my_action

Options
-------

``--version``
    show program's version number and exit

``-h, --help``
    show this help message and exit

``-v, --verbose``
    Verbose logging

``-n NUM_DISPLAYS, --numshown=NUM_DISPLAYS``
    The maximum number of job runs or lines of output to display(0 for show
    all).  Does not affect the display of all jobs and the display of actions
    for given job.

``--server=SERVER``
    Server URL to connect to

``-c, --color``
    Display in color

``--nocolor``
    Display without color

``-o, --stdout``
    Solely displays stdout

``-e, --stderr``
    Solely displays stderr

``-s, --save``
    Save server and color options to client config file (~/.tron)


States
----------
For complete list of states with a diagram of valid transitions see
http://packages.python.org/tron/jobs.html#states


Bugs
----

Post bugs to http://www.github.com/yelp/tron/issues.

See Also
--------

**trond** (8), **tronctl** (1), **tronfig** (1),
