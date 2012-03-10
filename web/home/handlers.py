import tornado.httpserver
import tornado.ioloop
import tornado.web

DEFAULT = "http://localhost:8089"

class RootHandler(tornado.web.RequestHandler):
    def get(self):
        self.redirect("/jobs")
        
