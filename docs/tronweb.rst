.. _tronweb:

tronweb
========

Tron comes with a simple but functional web-based UI to tronview. You can use
tronweb to view your jobs and drill down into individual job runs, actions and
action output. Currently, tronweb.py lives in the ``web/`` directory of the
tron source distribution.

tronweb requires the Tornado library.

Installation
------------

You can run tronweb directly from the tron source distribution. It will require
the tron package to be installed, and depends on `Tornado
<http://www.tornadoweb.org>`_.

Running tronweb
----------------

Before you run tronweb, you'll need to set up a ``config.py`` file in the
tronweb root directory. An example config file is included,
``config.py.example``.  The config file only includes a few attributes, most of
which are self-explanatory::

    config={
        'port': 8888,
        'trond_url':'http://localhost:8089',
        'output_url': "http://localhost:8889/{job}/{run}/{action}",
    }

``port`` defines the port tronweb runs on to handle requests. ``trond_url`` is
the URL that trond listens on for requests. You can generally leave these two
fields alone.

``output_url`` is used by tronweb to build the URL used for downloading job
output. tronweb does not serve these files itself because they may be quite
large. You may wish to configure Apache, nginx, or some other web server to
serve them.

``job``, ``run``, and ``action`` will be replaced by the job name, job run
name, and action name, which map directly to the path on disk where ``trond``
stores output files. For example, given run 10 of the job 'call_center_report'
with a single action 'report', the url mapping above will generate stdout and
stderr links of::

    http://localhost:8889/call_center_report/call_center_report.10/call_center_report.10.report.stdout
    http://localhost:8889/call_center_report/call_center_report.10/call_center_report.10.report.stderr

If you want a simple solution for serving output files, you can use Python's
built-in HTTP server::

    > cd working/directory/for/tron
    > python -m SimpleHTTPServer 8889

Once you have ``config.py`` set up, you simply start the server by running it::

    > python tronweb.py # or
    > nohup python tronweb.py &
