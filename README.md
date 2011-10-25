Tron - Batch Scheduling System
==============================

Tron is a centralized system for managing periodic batch processes and services
across a cluster. If you find [cron](http://en.wikipedia.org/wiki/Cron) or
[fcron](http://fcron.free.fr/) to be insufficient for managing complex work
flows across multiple computers, Tron might be for you.

Installation
------------

Tron ships with a `setup.py` file for installation as well as scripts for building a debian package.

See [QuickStart](http://github.com/Yelp/Tron/wiki/QuickStart) for more details.

Documentation
-------------

Sample configuration files (and man pages) are in the docs/ directory.

Full documentation can be found on the projects [Wiki](http://github.com/Yelp/Tron/wiki)

Contributing
------------

Use Github. We're friendly I swear. Contributions welcome.

Any issues should be either posted and discussed at http://github.com/Yelp/Tron/issues
or emailed to yelplabs@yelp.com

Running Tests
-------------

The easiest way to run tron's tests is to:

* Make a virtualenv
* Install testify in the virtualenv
* Call ``python `which testify` test`` from the repository root. You need to do this instead of just calling ``testify`` in order to make it use the correct Python executable (the one from your virtualenv).
