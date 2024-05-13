.. _tronfig:

tronfig
=======

Synopsis
--------

``tronfig [--server server_name ] [--verbose | -v] [<namespace>] [-p] [-]``

Description
-----------

**tronfig** allows live editing of the Tron configuration.  It retrieves
the configuration file for local editing, verifies the configuration,
and sends it back to the tron server. The configuration is applied
immediately.

Options
-------

``--server <server_name>``
    The server the tron instance is running on

``--verbose``
    Displays status messages along the way

``--version``
    Displays version string

``-p``
    Print the configuration

``namespace``
    The configuration namespace to edit. Defaults to MASTER

``-``
    Read new config from ``stdin``.

Configuration
-------------

By default tron will run with a blank configuration file. The config file is
saved to ``<working_dir>/config/`` by default. See the full documentation at
http://tron.readthedocs.io/en/latest/config.html.


Bugs
----

Post bugs to http://www.github.com/yelp/tron/issues.

See Also
--------

**trond** (8), **tronctl** (1), **tronview** (1),
