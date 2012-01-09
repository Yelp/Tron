import tornado.httpserver
import tornado.ioloop
import tornado.web

import urlparse

from tron import cmd

DEFAULT = "http://localhost:8089"
DATA = "http://localhost:8888/data/"

class JobsHandler(tornado.web.RequestHandler):
    @tornado.web.addslash
    def get(self):
        data = self.get_data()
        self.render("jobs.html", title="Jobs", data=data)

    def get_data(self):
        status, content = cmd.request(DEFAULT, 'jobs')
        if status == cmd.OK:
            return content
        return None

class JobHandler(tornado.web.RequestHandler):
    @tornado.web.addslash
    def get(self, job):
        data = self.get_data(job)
        self.render("job.html", title=data['name'], data=data)

    def get_data(self, job):
        status, content = cmd.request(DEFAULT, 'jobs/%s/' % job)
        if status == cmd.OK:
            return content
        return None

class JobRunHandler(tornado.web.RequestHandler):
    @tornado.web.addslash
    def get(self, job, run_id):
        data, run_data = self.get_data(job, run_id)
        self.render("job_run.html", title=data['id'], data=data, run_data=run_data)
    def get_data(self, job, run_id):
        status, data = cmd.request(DEFAULT, 'jobs/%s/%s/' % (job, run_id))
        if status == cmd.OK:
            run_data=[]
            for run in data['runs']:
              status, run_info = cmd.request(DEFAULT, 'jobs/%s/%s/%s' % (job, run_id, run["name"]))
              if status == cmd.OK:
                run_data.append(run_info)
            return (data, run_data)
        return (None, None)

class ActionRunHandler(tornado.web.RequestHandler):
    @tornado.web.addslash
    def get(self, job, run_id, action):
        data = self.get_data(job, run_id, action)
        self.render("action_run.html", title=data['id'], data=data)

    def get_data(self, job, run_id, action):
        status, content = cmd.request(DEFAULT, 'jobs/%s/%s/%s/' % (job, run_id, action))
        if status == cmd.OK:
            return content
        return None

