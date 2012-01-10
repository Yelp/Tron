import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.template

import os.path

from home.handlers import RootHandler
from job.handlers import JobsHandler, JobHandler, JobRunHandler, ActionRunHandler
from node.handlers import NodeHandler
try:
  from config import config
except ImportError:
  raise Exception("Missing config.py, or file poorly formed")

loader = tornado.template.Loader(os.path.join(os.path.dirname(__file__), "templates"))

settings = {
    'static_path': os.path.join(os.path.dirname(__file__), 'static'),
    'template_loader': loader,
}

application = tornado.web.Application([
    (r'/?$', RootHandler),
    (r'/jobs/?$', JobsHandler),
    (r'/jobs/(\w+)/?$', JobHandler),
    (r'/jobs/(\w+)/(\w+)/?$', JobRunHandler),
    (r'/jobs/(\w+)/(\w+)/(\w+)/?$', ActionRunHandler),
    (r'/nodes/?$', NodeHandler),
], **settings)

if __name__ == "__main__":
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen( config.get('port',8888) )
    tornado.ioloop.IOLoop.instance().start()

