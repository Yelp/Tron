import tornado.httpserver
import tornado.ioloop
import tornado.web

from tron import cmd

DEFAULT = "http://localhost:8089"

class RootHandler(tornado.web.RequestHandler):
    def get(self):
        content = self.get_jobs()
        job_names = [j['name'] for j in content['jobs']]
        print job_names
        self.render("../templates/base.html", title="My title", items=job_names)

    def get_jobs(self):
        status, content = cmd.request(DEFAULT, 'jobs')
        print status
        print content
        if status == cmd.OK:
            return content
        return None

