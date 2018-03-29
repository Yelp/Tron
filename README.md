Tron - Batch Scheduling System
==============================

[![Build Status](https://travis-ci.org/Yelp/Tron.svg?branch=master)](https://travis-ci.org/Yelp/Tron)
[![Documentation Status](https://readthedocs.org/projects/tron/badge/?version=latest)](http://tron.readthedocs.io/en/latest/?badge=latest)

Tron is a centralized system for managing periodic batch processes
across a cluster. If you find [cron](http://en.wikipedia.org/wiki/Cron) or
[fcron](http://fcron.free.fr/) to be insufficient for managing complex work
flows across multiple computers, Tron might be for you.

Install with:

    > sudo pip install tron

Or look at the [tutorial](http://tron.readthedocs.io/en/latest/tutorial.html).

The full documentation is available [on ReadTheDocs](http://tron.readthedocs.io/en/latest/).

Versions / Roadmap
------------------

Tron is changing and under active development.

It is being transformed from an ssh-based execution engine to a [Mesos
framework](http://mesos.apache.org/documentation/latest/frameworks/).

Tron development is specifically targeting Yelp's needs and not designed to be
a general solution for other companies.

* <= v0.6.2 - Stable version, recommended for non-Yelp installations.
* >= v0.7.x - Development version. Many features removed and experimental
  features added.

Contributing
------------

Read [Working on Tron](http://tron.readthedocs.io/en/latest/developing.html) and
start sending pull requests!

Any issues should be posted [on Github](http://github.com/Yelp/Tron/issues).

BerkeleyDB on Mac OS X
----------------------

    $ brew install berkeley-db
    $ export BERKELEYDB_DIR=$(brew --cellar)/berkeley-db/<installed version>
    $ export YES_I_HAVE_THE_RIGHT_TO_USE_THIS_BERKELEY_DB_VERSION=1
