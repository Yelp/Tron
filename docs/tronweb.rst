tronweb
========
Tron comes with a simple but functional web-based UI to tronview. You can use tronweb to view your jobs and drill down
into individual job runs, actions and action output. Currently, tronweb.py lives in the web\ directory of the tron
source distribution. 

Installation
------------
You can run tronweb directly from the tron source distribution. It will require the tron package to be installed, and
depends on `Tornado <http://www.tornadoweb.org>`_. 

Running tronweb
----------------
Before you run tronweb, you'll need to set up a config.py file in the tronweb root directory. An example config
file is included, config.py.example. The config file only includes a few attributes, most of which are self-explanatory::

  config={
    'port' : 8888,
    'trond_url' :'http://localhost:8089',
    'output_url' : "http://foo.bar.com/jobs/{job}/{run}/{action}"
  }

'port' defines the port tronweb runs on to handle requests. 'trond_url' is the url that trond listens on for requests. You can
generally leave these two fields as is.

'output_url' is used by tronweb to build the url used for downloading job output. In our case, we use apache to serve these files
as they can be quite large. job/run/action will be replaced by the job name, job run name, and action name -- which map directly
to the path on disk where trond stores output files. For example, given run 10 of the job 
'call_center_report' with a single action 'report', the url mapping above will generate stdout and stderr links of::

  http://foo.bar.com/jobs/call_center_report/call_center_report.10/call_center_report.10.report.stdout
  http://foo.bar.com/jobs/call_center_report/call_center_report.10/call_center_report.10.report.stderr

Once you have confg.py sorted, you simply start the server by running it::

  python tronweb.py # or
  nohup python tronweb.py &

That's it.

