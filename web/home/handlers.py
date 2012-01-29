import tornado.httpserver
import tornado.ioloop
import tornado.web

from tron import cmd

DEFAULT = "http://localhost:8089"

class RootHandler(tornado.web.RequestHandler):
    def get(self):
        self.redirect("/jobs")
        
