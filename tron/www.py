"""Management Web Services Interface

Got to know what's going on ?
"""
import logging

from twisted.internet import reactor

from twisted.cred import checkers
from twisted.web import server, resource, http, error
from twisted.web.woven import simpleguard

import simplejson

from tron.utils import time

# Sample code for http auth
# from twisted.cred import checkers
# from twisted.internet import reactor
# from twisted.web import server, resource
# from twisted.web.woven import simpleguard
# 
# class SimpleResource(resource.Resource):
# 
#     def getChild(self, path, request):
#         return self
# 
#     def render_GET(self, request):
#         auth = request.getComponent(simpleguard.Authenticated)
#         if auth:
#             return "hello my friend "+auth.name
#         else:
#             return """
#                 I don't think we've met
#         <a href="perspective-init">login</a>
#             """
# 
# checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()
# checker.addUser("bob", "12345")
# 
# reactor.listenTCP(8889, server.Site(
#       resource = simpleguard.guardResource(SimpleResource(), [checker])))
# reactor.run()

log = logging.getLogger("tron.www")

def respond(request, response_dict, code=http.OK, headers=None):
    """Helper to generate a json response"""
    request.setResponseCode(code)
    request.setHeader('content-type', 'text/json')
    if headers:
        for key, val in headers.iteritems():
            request.setHeader(key, val)
    if response_dict:
        return simplejson.dumps(response_dict)
    return None


class JobRunResource(resource.Resource):
    isLeaf = True
    def __init__(self, run):
        self._run = run
        resource.Resource.__init__(self)
    

class JobResource(resource.Resource):
    """A resource that describes a particular job"""
    isLeaf = False
    def __init__(self, job):
        self._job = job
        resource.Resource.__init__(self)

    def render_GET(self, request):
        schedule_output = str(self._job.scheduler)

        run_output = []
        for job_run in self._job.runs:
            if job_run.is_done:
                if job_run.is_success:
                    state = "S"
                else:
                    state = "F"
            elif job_run.is_running:
                state = "R"
            else:
                state = "W"
                
            run_output.append({
                'id': job_run.id,
                'href': self.server.childLink(job_run.id),
                'run_time': job_run.run_time and str(job_run.run_time),
                'start_time': job_run.start_time and str(job_run.start_time),
                'end_time': job_run.end_time and str(job_run.end_time),
                'exit_status': job_run.exit_status,
                'state': state,
            })

        resources_output = []

        output = {
            'name': self._job.name,
            'node': self._job.node.hostname,
            'scheduler': schedule_output,
            'runs': run_output,
            'resources': resources_output,
        }
        return respond(request, output)

    def _queue(self, request):
        """Queue up a run for the current job"""
        # Let's see if there is already a queued run
        if self._job.runs and not self._job.runs[-1].is_done:
            last_run = self._job.runs[-1]
            log.info("Request to queue job %s but run %s is already waiting", self._job.name, last_run.id)

            run_href = "/jobs/%s/%s" % (self._job.name, last_run.id)
            log.debug("Redirecting to %s", run_href)
            return respond(request, {'reason': "Job already queued"}, code=http.SEE_OTHER, headers={'Location': run_href})
        else:
            log.debug("Creating new run for %s", self._job.name)
            new_run = self._job.build_run()
            new_run.run_time = time.current_time()
            run_href = "/jobs/%s/%s" % (self._job.name, new_run.id)
            return respond(request, None, code=http.SEE_OTHER, headers={'Location': run_href})

    def render_POST(self, request):
        log.debug("Handling post request for %s", self._job.name)
        if request.args['action'][0] == "queue":
            return self._queue(request)
        else:
            log.warning("Unknown request action %s", request.args['action'])
            request.setResponseCode(http.NOT_IMPLEMENTED)
            return
            

class JobsResource(resource.Resource):
    """Resource for all our daemon's jobs"""

    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)
    
    def getChild(self, name, request):
        if name == '':
            return self
        else:
            found_job = self._master_control.jobs.get(name)
            if found_job is None:
                return error.NoResource()
            else:
                return JobResource(found_job)
    
    def render_GET(self, request):
        request.setHeader("content-type", "text/json")
        
        job_list = []
        for current_job in self._master_control.jobs.itervalues():
            last_success = None
            status = ""
            if current_job.runs:
                last_job = current_job.runs[-1]
                if last_job.is_running:
                    status = "running"
                elif not last_job.is_done:
                    status = "waiting"
                
                for job_run in reversed(current_job.runs):
                    if job_run.is_success:
                        last_success = str(job_run.end_time)
                        break

            job_desc = {
                'name': current_job.name,
                'href': self.server.childLink(current_job.name),
                'node': current_job.node.hostname,
                'scheduler': str(current_job.scheduler),
                'status': status,
                'last_success': last_success,
            }
            job_list.append(job_desc)

        output = {
            'jobs': job_list,
        }
        return respond(request, output)


class RootResource(resource.Resource):
    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)

        # Setup children
        self.putChild('jobs', JobsResource(master_control))

    def getChild(self, name, request):
        if name == '':
            return self
        else:
            return resource.Resource.getChild(self, name, request)

    def render_GET(self, request):
        return respond(request, {'status': "I'm alive biatch"})

if __name__ == '__main__':
    from twisted.internet import reactor
    from testify.utils import turtle
    master_control = turtle.Turtle()
    master_control.jobs = {
        'test_job': turtle.Turtle(name="test_job", node=turtle.Turtle(hostname="batch0")),
    }
    reactor.listenTCP(8082, server.Site(RootResource(master_control)))
    reactor.run()
    
    