import tornado.httpserver
import tornado.ioloop
import tornado.web

from tron.commands import client

try:
    from config import config
except ImportError:
    raise Exception("Missing config.py, or file poorly formed")


DEFAULT = "http://localhost:8089"

def output_url(job, run, action):
    return config['output_url'].format(job=job, run=run, action=action)


def trond_url():
    return config.get('trond_url', DEFAULT)


class JobsHandler(tornado.web.RequestHandler):

    @tornado.web.addslash
    def get(self):
        data = self.get_data()
        self.render("jobs.html", title="Jobs", data=data)

    def get_data(self):
        status, content = client.request(trond_url(), 'jobs')
        if status == client.OK:
            return content
        return None


class JobHandler(tornado.web.RequestHandler):

    @tornado.web.addslash
    def get(self, job):
        data = self.get_data(job)
        self.render("job.html", title=data['name'], data=data)

    def get_data(self, job):
        status, content = client.request(trond_url(), 'jobs/%s/' % job)
        if status == client.OK:
            return content
        return None


class JobRunHandler(tornado.web.RequestHandler):

    @tornado.web.addslash
    def get(self, job, run_id):
        data, run_data = self.get_data(job, run_id)
        self.render("job_run.html", title=data['id'], data=data,
                    run_data=run_data, job=job, output_url=output_url)

    def get_data(self, job, run_id):
        status, data = client.request(trond_url(), 'jobs/%s/%s/' % (job, run_id))
        if status == client.OK:
            run_data=[]
            for run in data['runs']:
                status, run_info = client.request(trond_url(),
                                               'jobs/%s/%s/%s' % (
                                                   job, run_id, run["id"]))
                if status == client.OK:
                    run_data.append(run_info)
            return (data, run_data)
        return (None, None)

    def output_url(job, run, action):
        return config['output_url'].format(job=job, run=run, action=action)


class ActionRunHandler(tornado.web.RequestHandler):

    @tornado.web.addslash
    def get(self, job, run_id, action):
        data = self.get_data(job, run_id, action)
        self.render("action_run.html", title=data['id'], data=data)

    def get_data(self, job, run_id, action):
        status, content = client.request(trond_url(),
                                      'jobs/%s/%s/%s/' % (job, run_id, action))
        if status == client.OK:
            return content
        return None
