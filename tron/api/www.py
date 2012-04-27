"""
Web Services Interface used by command-line clients and web frontend to
view current state, event history and send commands to trond.
"""

import datetime
import logging
import urllib

try:
    import simplejson as json
    assert json # pyflakes
except ImportError:
    import json

from twisted.web import http, resource, server

from tron import service, event
from tron.api import adapter
from tron.core import actionrun
from tron.api import requestargs


log = logging.getLogger(__name__)


class JSONEncoder(json.JSONEncoder):
    """Custom JSON for certain objects"""

    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.strftime("%Y-%m-%d %H:%M:%S")

        if isinstance(o, datetime.date):
            return o.isoformat()

        return super(JSONEncoder, self).default(o)


def respond(request, response_dict, code=http.OK, headers=None):
    """Helper to generate a json response"""
    request.setResponseCode(code)
    request.setHeader('content-type', 'text/json')
    if headers:
        for key, val in headers.iteritems():
            request.setHeader(key, val)
    if response_dict:
        return json.dumps(response_dict, cls=JSONEncoder)
    return ""


class ActionRunResource(resource.Resource):

    isLeaf = True

    def __init__(self, job_run, action_name):
        resource.Resource.__init__(self)
        self._job_run           = job_run
        self._action_name       = action_name

    def render_GET(self, request):
        num_lines = requestargs.get_integer(request, 'num_lines')
        run_adapter = adapter.ActionRunAdapter(
                self._job_run, self._action_name, num_lines)
        return respond(request, run_adapter.get_repr())

    def render_POST(self, request):
        cmd = requestargs.get_string(request, 'command')
        log.info("Handling '%s' request for action run %s",
                 cmd, self._job_run.id, self._action_name)

        if cmd not in ('start', 'success', 'cancel', 'fail', 'skip'):
            log.warning("Unknown request command %s", cmd)
            return respond(request, None, code=http.NOT_IMPLEMENTED)

        action_run = self._job_run.action_runs[self._action_name]
        try:
            resp = getattr(action_run, cmd)()
        except actionrun.Error:
            resp = None

        if not resp:
            msg = "Failed to %s action run %s." % (cmd, action_run)
        else:
            msg = "Action run now in state %s" % action_run.state.short_name
        return respond(request, {'result': msg})


class JobRunResource(resource.Resource):

    isLeaf = False

    def __init__(self, run, master_control):
        resource.Resource.__init__(self)
        self._run = run
        self._master_control = master_control

    def getChild(self, act_name, request):
        if act_name == '':
            return self
        if act_name == '_events':
            return EventResource(self._run)
        if act_name in self._run.action_runs:
            return ActionRunResource(self._run, act_name)

        return resource.NoResource("Cannot find action '%s' for job run '%s'" %
                                   (act_name, self._run.id))

    def render_GET(self, request):
        run_adapter = adapter.JobRunAdapter(self._run, include_action_runs=True)
        return respond(request, run_adapter.get_repr())

    def render_POST(self, request):
        cmd = requestargs.get_string(request, 'command')
        log.info("Handling '%s' request for job run %s", cmd, self._run.id)

        if cmd not in ['start', 'restart', 'success', 'fail', 'cancel']:
            log.warning("Unknown request command %s", cmd)
            return respond(request, None, code=http.NOT_IMPLEMENTED)

        getattr(self, '_%s' % cmd)()
        return respond(request, {'result': "Job run now in state %s" %
                                 self._run.state.short_name})

    def _restart(self):
        log.info("Resetting all action runs to scheduled state")
        job_name = self._run.job_name
        job_sched = self._master_control.jobs[job_name]
        job_sched.manual_start(self._run.run_time)

    def _start(self):
        if self._run.start():
            log.info("Starting job run %s", self._run.id)
        else:
            log.warning("Failed to start job run %s" % self._run)

    def _success(self):
        if self._run.success():
            log.info("Marking job run %s for success", self._run.id)
        else:
            log.warning("Request to mark job run %s succeed when it has"
                        " already", self._run.id)

    def _cancel(self):
        if self._run.cancel():
            log.info("Cancelling job %s", self._run.id)
            self._run.cancel()
        else:
            log.warning("Request to cancel job run %s when it's already"
                        " cancelled", self._run.id)

    def _fail(self):
        if self._run.fail():
            log.info("Marking job run %s as failed", self._run.id)
        else:
            log.warning("Request to fail job run %s when it's already running"
                        " or done", self._run.id)


class JobResource(resource.Resource):
    """A resource that describes a particular job"""

    isLeaf = False

    def __init__(self, job_sched, master_control):
        self._job_sched = job_sched
        self._master_control = master_control
        resource.Resource.__init__(self)

    def getChild(self, run_id, request):
        job = self._job_sched.job
        if run_id == '':
            return self
        if run_id == '_events':
            return EventResource(self._job_sched.job)

        run_id = run_id.upper()
        if run_id == 'HEAD':
            run = job.runs.get_newest()
        elif run_id.isdigit():
            run = job.runs.get_run_by_num(int(run_id))
        else:
            run = job.runs.get_run_by_state_short_name(run_id)

        if run:
            return JobRunResource(run, self._master_control)
        return resource.NoResource(
                "Cannot find run number '%s' for job '%s'" % (run_id, job.name))

    def render_GET(self, request):
        include_action_runs = requestargs.get_bool(request, 'include_action_runs')
        job_adapter = adapter.JobAdapter(
                self._job_sched.job, True, include_action_runs)
        return respond(request, job_adapter.get_repr())

    def render_POST(self, request):
        cmd = requestargs.get_string(request, 'command')
        log.info("Handling '%s' request for job run %s",
                cmd, self._job_sched.job.name)

        if cmd == 'enable':
            self._job_sched.enable()
            msg = "Job %s is enabled" % self._job_sched.job.name

        elif cmd == 'disable':
            self._job_sched.disabled()
            msg = "Job %s is disabled" % self._job_sched.job.name

        elif cmd == 'start':
            run_time = requestargs.get_datetime(request, 'run_time')
            runs = self._job_sched.manual_start(run_time=run_time)
            msg = "New Job Runs %s created" % ",".join([r.id for r in runs])

        else:
            return respond(request, None, code=http.NOT_IMPLEMENTED)

        return respond(request, {'result': msg})


class JobsResource(resource.Resource):
    """Resource for all our daemon's jobs"""

    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)

    def getChild(self, name, request):
        if name == '':
            return self

        job_sched = self._master_control.jobs.get(name)
        if job_sched is None:
            return resource.NoResource("Cannot find job '%s'" % name)

        return JobResource(job_sched, self._master_control)

    def get_data(self, include_job_run=False, include_action_runs=False):
        job_adapter = adapter.JobAdapter
        return [
            job_adapter(job.job, include_job_run, include_action_runs).get_repr()
            for job in self._master_control.jobs.itervalues()
        ]

    def render_GET(self, request):
        include_job_runs = requestargs.get_bool(request, 'include_job_runs')
        include_action_runs = requestargs.get_bool(request, 'include_action_runs')
        output = dict(jobs=self.get_data(include_job_runs, include_action_runs))
        return respond(request, output)

    def render_POST(self, request):
        cmd = requestargs.get_string(request, 'command')
        log.info("Handling '%s' request on all jobs", cmd)

        if cmd == 'disableall':
            self._master_control.disable_all()
            return respond(request, {'result': "All jobs are now disabled"})

        if cmd == 'enableall':
            self._master_control.enable_all()
            return respond(request, {'result': "All jobs are now enabled"})

        log.warning("Unknown request command %s for all jobs", cmd)
        return respond(request, None, code=http.NOT_IMPLEMENTED)


class ServiceInstanceResource(resource.Resource):

    isLeaf = True

    def __init__(self, service_instance, master_control):
        self._service_instance = service_instance
        self._master_control = master_control
        resource.Resource.__init__(self)

    def render_POST(self, request):
        cmd = requestargs.get_string(request, 'command')
        log.info("Handling '%s' request on service %s",
                 cmd, self._service_instance.id)

        if cmd == 'stop':
            self._service_instance.stop()
            return respond(request, {'result': "Service instance stopping"})

        if cmd == 'zap':
            self._service_instance.zap()
            return respond(request, {'result': "Service instance zapped"})

        if cmd == 'start':
            try:
                self._service_instance.start()
            except service.InvalidStateError:
                msg = ("Failed to start: Service is already %s" %
                       self._service_instance.state)
                return respond(request, {'result': msg})

            return respond(request, {'result': "Service instance starting"})

        log.warning("Unknown request command %s for service %s", cmd,
                    self._service_instance.id)
        return respond(request, None, code=http.NOT_IMPLEMENTED)


class ServiceResource(resource.Resource):
    """A resource that describes a particular service"""
    def __init__(self, service, master_control):
        self._service = service
        self._master_control = master_control
        resource.Resource.__init__(self)

    def getChild(self, name, request):
        if name == '':
            return self
        if name == '_events':
            return EventResource(self._service)

        for instance in self._service.instances:
            if str(instance.instance_number) == str(name):
                return ServiceInstanceResource(instance, self._master_control)
        else:
            return resource.NoResource("Cannot find service '%s'" % name)

    def get_instance_data(self, instance):
        return {
            'id':           instance.id,
            'node':         instance.node.hostname if instance.node else None,
            'state':        instance.state.name,
        }

    # TODO: create an adapter
    def render_GET(self, request):
        instance_output = [
            self.get_instance_data(instance)
            for instance in self._service.instances
        ]

        output = {
            'name':         self._service.name,
            'state':        self._service.state.name.upper(),
            'count':        self._service.count,
            'command':      self._service.command,
            'instances':    instance_output,
            'node_pool':    [n.hostname for n in self._service.node_pool.nodes]
        }
        return respond(request, output)

    def render_POST(self, request):
        cmd = requestargs.get_string(request, 'command')
        log.info("Handling '%s' request on service %s",
                 cmd, self._service.name)

        if cmd == 'stop':
            self._service.stop()
            return respond(request, {'result': "Service stopping"})

        if cmd == 'zap':
            self._service.zap()
            return respond(request, {'result': "Service zapped"})

        if cmd == 'start':
            try:
                self._service.start()
            except service.InvalidStateError:
                msg = ("Failed to start: Service is already %s" %
                       self._service.state)
                return respond(request, {'result': msg})

            return respond(request, {'result': "Service starting"})

        log.warning("Unknown request command %s for service %s",
                    cmd, self._service.name)
        return respond(request, None, code=http.NOT_IMPLEMENTED)


class ServicesResource(resource.Resource):
    """Resource for all our daemon's services"""

    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)

    def getChild(self, name, request):
        if name == '':
            return self

        found = self._master_control.services.get(name)
        if found is None:
            return resource.NoResource("Cannot find service '%s'" % name)

        return ServiceResource(found, self._master_control)

    def get_data(self):
        service_list = []
        for current_service in self._master_control.services.itervalues():
            try:
                status = current_service.state.name.upper()
            except Exception, e:
                log.error("Unexpected service state: %s" % e)
                status = "BROKEN"
            try:
                count = current_service.count
            except Exception, e:
                log.error("Unexpected service count: %s" % e)
                count = -1

            service_desc = {
                'name': current_service.name,
                'count': count,
                'href': "/services/%s" % urllib.quote(current_service.name),
                'status': status,
            }
            service_list.append(service_desc)

        return service_list

    def render_GET(self, request):
        request.setHeader("content-type", "text/json")
        return respond(request, dict(services=self.get_data()))


class ConfigResource(resource.Resource):
    """Resource for configuration changes"""

    isLeaf = True

    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)

    def render_GET(self, request):
        return respond(request,
                       {'config': self._master_control.config_lines()})

    def render_POST(self, request):
        log.info("Handling reconfig request")
        new_config = requestargs.get_string(request, 'config')
        self._master_control.rewrite_config(new_config)

        # TODO: This should be a more informative response
        response = {'status': "I'm alive biatch"}
        try:
            self._master_control.reconfigure()
        except Exception, e:
            log.exception("Failure doing live reconfig")
            response['error'] = str(e)

        return respond(request, response)


class StatusResource(resource.Resource):

    isLeaf = True

    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)

    def render_GET(self, request):
        return respond(request, {'status': "I'm alive biatch"})


class EventResource(resource.Resource):

    isLeaf = True

    def __init__(self, recordable):
        resource.Resource.__init__(self)
        self._recordable = recordable

    def render_GET(self, request):
        response_data = []

        recorder = event.EventManager.get_instance().get(self._recordable)
        if not recorder:
            return respond(request, dict(data=[]))

        for evt in recorder.list():
            entity_desc = "UNKNOWN" if not evt.entity else str(evt.entity)
            response_data.append({
                'level':        evt.level,
                'name':         evt.name,
                'entity':       entity_desc,
                'time':         evt.time
            })
        return respond(request, dict(data=response_data))


class RootResource(resource.Resource):
    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)

        # Setup children
        self.putChild('jobs',       JobsResource(master_control))
        self.putChild('services',   ServicesResource(master_control))
        self.putChild('config',     ConfigResource(master_control))
        self.putChild('status',     StatusResource(master_control))
        self.putChild('events',     EventResource(master_control))

    def getChild(self, name, request):
        if name == '':
            return self
        return resource.Resource.getChild(self, name, request)

    def render_GET(self, request):
        request.setHeader("content-type", "text/json")

        # We're going to load a big response with a bunch of stuff we know
        # about this tron instance

        jobs_resource = self.children["jobs"]
        services_resource = self.children["services"]

        response = {
            'jobs':             jobs_resource.get_data(),
            'jobs_href':        request.uri + request.childLink('jobs'),
            'services':         services_resource.get_data(),
            'services_href':    request.uri + request.childLink('services'),
            'config_href':      request.uri + request.childLink('config'),
            'status_href':      request.uri + request.childLink('status'),
        }
        return respond(request, response)


if __name__ == '__main__':
    from twisted.internet import reactor
    from testify.utils import turtle
    master_control = turtle.Turtle()
    master_control.jobs = {
        'test_job': turtle.Turtle(name="test_job",
                                  node=turtle.Turtle(hostname="batch0")),
    }
    reactor.listenTCP(8082, server.Site(RootResource(master_control)))
    reactor.run()
