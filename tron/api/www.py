"""
Web Services Interface used by command-line clients and web frontend to
view current state, event history and send commands to trond.
"""

import datetime
import logging

try:
    import simplejson as json
    assert json # pyflakes
except ImportError:
    import json

from twisted.web import http, resource

from tron import event
from tron.api import adapter, controller
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


def handle_command(request, api_controller, obj):
    """Handle a request to perform a command."""
    command = requestargs.get_string(request, 'command')
    log.info("Handling '%s' request on %s", command, obj)
    try:
        response = api_controller.handle_command(command)
    except controller.UnknownCommandError, e:
        log.warning("Unknown command %s for service %s", command, obj)
        response = {'error': str(e)}
        return respond(request, response, code=http.NOT_IMPLEMENTED)

    return respond(request, {'result': response})


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

    # TODO: controller
    def render_POST(self, request):
        cmd = requestargs.get_string(request, 'command')
        log.info("Handling '%s' request for action run %s.%s",
                 cmd, self._job_run.id, self._action_name)

        if cmd not in ('start', 'success', 'cancel', 'fail', 'skip'):
            log.warning("Unknown request command %s", cmd)
            return respond(request, None, code=http.NOT_IMPLEMENTED)

        action_run = self._job_run.action_runs[self._action_name]

        # An action can only be started if the job run has been started
        if cmd == 'start' and self._job_run.is_scheduled:
            resp = None
        else:
            try:
                resp = getattr(action_run, cmd)()
            except actionrun.Error:
                resp = None

        if not resp:
            msg = "Failed to %s action run %s is in state %s." % (
                    cmd, action_run, action_run.state)
        else:
            msg = "Action run now in state %s" % action_run.state.short_name
        return respond(request, {'result': msg})


class JobRunResource(resource.Resource):

    def __init__(self, run, job_scheduler):
        resource.Resource.__init__(self)
        self._run = run
        self.job_scheduler = job_scheduler

    def getChild(self, act_name, _):
        if act_name == '':
            return self
        if act_name == '_events':
            return EventResource(self._run.id)
        if act_name in self._run.action_runs:
            return ActionRunResource(self._run, act_name)

        return resource.NoResource("Cannot find action '%s' for job run '%s'" %
                                   (act_name, self._run.id))

    def render_GET(self, request):
        run_adapter = adapter.JobRunAdapter(self._run, include_action_runs=True)
        return respond(request, run_adapter.get_repr())

    # TODO: controller
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
        self.job_scheduler.manual_start(self._run.run_time)

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

    def __init__(self, job_scheduler):
        self.job_scheduler = job_scheduler
        resource.Resource.__init__(self)

    def getChild(self, run_id, _):
        job = self.job_scheduler.job
        if run_id == '':
            return self
        if run_id == '_events':
            return EventResource(self.job_scheduler.job.name)

        run_id = run_id.upper()
        if run_id == 'HEAD':
            run = job.runs.get_newest()
        elif run_id.isdigit():
            run = job.runs.get_run_by_num(int(run_id))
        else:
            run = job.runs.get_run_by_state_short_name(run_id)

        if run:
            return JobRunResource(run, self.job_scheduler)
        msg = "Cannot find job run '%s' for job '%s'"
        return resource.NoResource(msg % (run_id, job))

    def render_GET(self, request):
        include_action_runs = requestargs.get_bool(request, 'include_action_runs')
        job_adapter = adapter.JobAdapter(
                self.job_scheduler.job, True, include_action_runs)
        return respond(request, job_adapter.get_repr())

    # TODO: controller
    def render_POST(self, request):
        cmd = requestargs.get_string(request, 'command')
        log.info("Handling '%s' request for job run %s",
                cmd, self.job_scheduler.job.name)

        if cmd == 'enable':
            self.job_scheduler.enable()
            msg = "Job %s is enabled" % self.job_scheduler.job.name

        elif cmd == 'disable':
            self.job_scheduler.disable()
            msg = "Job %s is disabled" % self.job_scheduler.job.name

        elif cmd == 'start':
            run_time = requestargs.get_datetime(request, 'run_time')
            runs = self.job_scheduler.manual_start(run_time=run_time)
            msg = "New Job Runs %s created" % ",".join([r.id for r in runs])

        else:
            return respond(request, None, code=http.NOT_IMPLEMENTED)

        return respond(request, {'result': msg})


class JobCollectionResource(resource.Resource):
    """Resource for all our daemon's jobs"""

    def __init__(self, master_control):
        self.mcp        = master_control
        self.controller = controller.JobCollectionController(master_control)
        resource.Resource.__init__(self)

    def getChild(self, name, request):
        if name == '':
            return self

        job_sched = self.mcp.get_job_by_name(name)
        if job_sched is None:
            return resource.NoResource("Cannot find job '%s'" % name)

        return JobResource(job_sched)

    def get_data(self, include_job_run=False, include_action_runs=False):
        jobs = (sched.job for sched in self.mcp.get_jobs())
        return adapter.adapt_many(adapter.JobAdapter, jobs,
            include_job_run, include_action_runs)

    def render_GET(self, request):
        include_job_runs = requestargs.get_bool(request, 'include_job_runs')
        include_action_runs = requestargs.get_bool(request, 'include_action_runs')
        output = dict(jobs=self.get_data(include_job_runs, include_action_runs))
        return respond(request, output)

    def render_POST(self, request):
        return handle_command(request, self.controller, self.mcp)


class ServiceInstanceResource(resource.Resource):

    isLeaf = True

    def __init__(self, service_instance):
        resource.Resource.__init__(self)
        self.service_instance = service_instance
        self.controller = controller.ServiceInstanceController(service_instance)

    def render_POST(self, request):
        return handle_command(request, self.controller, self.service_instance)


class ServiceResource(resource.Resource):
    """A resource that describes a particular service"""
    def __init__(self, service):
        resource.Resource.__init__(self)
        self.service    = service
        self.controller = controller.ServiceController(self.service)

    def getChild(self, name, _):
        if name == '':
            return self
        if name == '_events':
            return EventResource(str(self.service))

        number = int(name) if name.isdigit() else None
        instance = self.service.instances.get_by_number(number)
        if instance:
            return ServiceInstanceResource(instance)

        return resource.NoResource("Cannot find service instance: %s" % name)

    def render_GET(self, request):
        return respond(request, adapter.ServiceAdapter(self.service).get_repr())

    def render_POST(self, request):
        return handle_command(request, self.controller, self.service)


class ServiceCollectionResource(resource.Resource):
    """Resource for ServiceCollection."""

    def __init__(self, mcp):
        self.collection = mcp.get_service_collection()
        resource.Resource.__init__(self)

    def getChild(self, name, _):
        if name == '':
            return self

        service = self.collection.get_by_name(name)
        if service is None:
            return resource.NoResource("Cannot find service '%s'" % name)

        return ServiceResource(service)

    def get_data(self):
        return adapter.adapt_many(adapter.ServiceAdapter, self.collection)

    def render_GET(self, request):
        return respond(request, dict(services=self.get_data()))


class ConfigResource(resource.Resource):
    """Resource for configuration changes"""

    isLeaf = True

    def __init__(self, master_control):
        self.controller = controller.ConfigController(master_control)
        resource.Resource.__init__(self)

    def render_GET(self, request):
        config_name = requestargs.get_string(request, 'name')
        if not config_name:
            return respond(request, {'error': "'name' for config is required."})
        return respond(request, self.controller.read_config(config_name))

    def render_POST(self, request):
        config_content = requestargs.get_string(request, 'config')
        name = requestargs.get_string(request, 'name')
        config_hash = requestargs.get_string(request, 'hash')
        log.info("Handling reconfigure request: %s, %s" % (name, config_hash))
        if not name:
            return respond(request, {'error': "'name' for config is required."})

        response = {'status': "Active"}
        error = self.controller.update_config(name, config_content, config_hash)
        if error:
            response['error'] = error
        return respond(request, response)


class StatusResource(resource.Resource):

    isLeaf = True

    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)

    def render_GET(self, request):
        return respond(request, {'status': "I'm alive."})


class EventResource(resource.Resource):

    isLeaf = True

    def __init__(self, entity_name):
        resource.Resource.__init__(self)
        self.entity_name = entity_name

    def render_GET(self, request):
        recorder        = event.get_recorder(self.entity_name)
        adapt_class     = adapter.EventAdapter
        response_data   = [adapt_class(e).get_repr() for e in recorder.list()]
        return respond(request, dict(data=response_data))


class RootResource(resource.Resource):
    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)

        # Setup children
        self.putChild('jobs',       JobCollectionResource(master_control))
        self.putChild('services',   ServiceCollectionResource(master_control))
        self.putChild('config',     ConfigResource(master_control))
        self.putChild('status',     StatusResource(master_control))
        self.putChild('events',     EventResource(''))

    def getChild(self, name, request):
        if name == '':
            return self
        return resource.Resource.getChild(self, name, request)

    def render_GET(self, request):
        """Load a big response."""
        # TODO: why?

        # TODO: add namespaces
        response = {
            'jobs':             self.children["jobs"].get_data(),
            'jobs_href':        request.uri + request.childLink('jobs'),
            'services':         self.children["services"].get_data(),
            'services_href':    request.uri + request.childLink('services'),
            'config_href':      request.uri + request.childLink('config'),
            'status_href':      request.uri + request.childLink('status'),
        }
        return respond(request, response)
