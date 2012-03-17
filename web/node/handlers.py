import tornado.httpserver
import tornado.ioloop
import tornado.web

from tron.commands import client

DEFAULT = "http://localhost:8089"

class NodeHandler(tornado.web.RequestHandler):
    def get(self):
        content = self.get_jobs()
        job_names = [j['name'] for j in content['jobs']]
        self.render("../templates/base.html", title="My title", items=job_names)

    def get_jobs(self):
        status, content = client.request(DEFAULT, 'jobs')
        if status == client.OK:
            return content
        return None

