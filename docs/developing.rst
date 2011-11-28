Working on Tron
===============

Setting Up an Environment
-------------------------

Tron works well with `virtualenv <http://www.virtualenv.org>`_, so let's make
one of those with `virtualenvwrapper
<http://www.doughellmann.com/projects/virtualenvwrapper/>`_::

    > mkvirtualenv tron --distribute --no-site-packages
    > pip install < req_dev.txt

Here we used ``req_dev.txt`` instead of ``req.txt`` because `Testify
<https://github.com/yelp/testify>`_ is required to run the tests and `Sphinx
<http://sphinx.pocoo.org/>`_ is required to build the documentation.

Coding Standards
----------------

Although the code is not currently `PEP8
<http://www.python.org/dev/peps/pep-0008/>`_`-compliant, all new code should
comply as much as possible. The documentation must also be kept up to date with
any changes in functionality, especially the man pages.

If and when you come across non-PEP8-compliant code, avoid reformatting it in
the same branch as functional changes so that code reviews are less confusing.

Running Tron in a Sandbox
-------------------------

If you're testing Tron by hand, you typically don't want to pollute your main
directory structure with your test jobs and configs. To put everything in one
directory, launch `trond` like this::

    > mkdir wd
    > bin/trond --working-dir=wd --log-file=wd/tron.log --pid-file=wd/tron.pid --verbose

You may be tempted to run with ``--nodaemon``, but all the interesting output
goes to ``tron.log``, so you're better off running ``tail -f wd/tron.log`` in a
terminal. Kill ``trond`` when you're done with ``cat wd/tron.pid | xargs
kill``.

Running the Tests
-----------------

If you're working in a virtualenv and have installed Testify there (the
recommended practice), you can run the tests with ``python `which testify`
test`` from the repository root. You do this instead of just calling
``testify`` in order for the correct Python executable and module search path
to be used.

Contributing
------------

If there's no Github issue associated with what you're working on, make one.

Create a feature branch in Git and incorporate the issue number if applicable,
probably as a suffix. (For example, this paragraph was written in
``doc_refactor_49``.)

Update the documentation (including the man pages and ``debian/changelog``)
with your changes and `submit a pull request to yelp/tron on Github
<http://www.github.com/yelp/tron/pull/new>`_.
