"""
Web Services Interface used by command-line clients and web frontend to
view current state, event history and send commands to trond.
"""

import datetime
import logging

try:
    import simplejson as json
    _silence_pyflakes = [json]
except ImportError:
    import json

from twisted.web import http, resource, static, server

from tron import event
from tron.api import adapter, controller
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


def handle_command(request, api_controller, obj, **kwargs):
    """Handle a request to perform a command."""
    command = requestargs.get_string(request, 'command')
    log.info("Handling '%s' request on %s", command, obj)
    try:
        response = api_controller.handle_command(command, **kwargs)
    except controller.UnknownCommandError, e:
        log.warning("Unknown command %s for service %s", command, obj)
        response = {'error': str(e)}
        return respond(request, response, code=http.NOT_IMPLEMENTED)

    return respond(request, {'result': response})


class ActionRunResource(resource.Resource):

    isLeaf = True

    def __init__(self, action_run, job_run):
        resource.Resource.__init__(self)
        self.job_run    = job_run
        self.action_run = action_run
        self.controller = controller.ActionRunController(action_run, job_run)

    def render_GET(self, request):
        num_lines = requestargs.get_integer(request, 'num_lines')
        run_adapter = adapter.ActionRunAdapter(
            self.action_run, self.job_run, num_lines)
        return respond(request, run_adapter.get_repr())

    def render_POST(self, request):
        return handle_command(request, self.controller, self.action_run)


class JobRunResource(resource.Resource):

    def __init__(self, job_run, job_scheduler):
        resource.Resource.__init__(self)
        self.job_run       = job_run
        self.job_scheduler = job_scheduler
        self.controller    = controller.JobRunController(job_run, job_scheduler)

    def getChild(self, action_name, _):
        if action_name == '':
            return self
        if action_name == '_events':
            return EventResource(self.job_run.id)
        if action_name in self.job_run.action_runs:
            action_run = self.job_run.action_runs[action_name]
            return ActionRunResource(action_run, self.job_run)

        msg = "Cannot find action %s for %s"
        return resource.NoResource(msg % (action_name, self.job_run))

    def render_GET(self, request):
        #include_runs = requestargs.get_bool(request, 'include_action_runs')
        #include_graph = requestargs.get_bool(request, 'include_action_graph')
        run_adapter = adapter.JobRunAdapter(self.job_run,
            include_action_runs=True,
            include_action_graph=True)
        return respond(request, run_adapter.get_repr())

    def render_POST(self, request):
        return handle_command(request, self.controller, self.job_run)


class JobResource(resource.Resource):
    """A resource that describes a particular job"""

    def __init__(self, job_scheduler):
        resource.Resource.__init__(self)
        self.job_scheduler = job_scheduler
        self.controller    = controller.JobController(job_scheduler)

    def getChild(self, run_id, _):
        job = self.job_scheduler.job
        if run_id == '':
            return self
        if run_id == '_events':
            return EventResource(self.job_scheduler.get_name())

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
                self.job_scheduler.get_job(), True, include_action_runs)
        return respond(request, job_adapter.get_repr())

    def render_POST(self, request):
        run_time = requestargs.get_datetime(request, 'run_time')
        return handle_command(request, self.controller, self.job_scheduler,
            run_time=run_time)


class JobCollectionResource(resource.Resource):
    """Resource for all our daemon's jobs"""

    def __init__(self, job_collection):
        self.job_collection = job_collection
        self.controller     = controller.JobCollectionController(job_collection)
        resource.Resource.__init__(self)

    def getChild(self, name, request):
        if name == '':
            return self

        job_sched = self.job_collection.get_by_name(name)
        if job_sched is None:
            return resource.NoResource("Cannot find job '%s'" % name)

        return JobResource(job_sched)

    def get_data(self, include_job_run=False, include_action_runs=False):
        jobs = (sched.get_job() for sched in self.job_collection)
        return adapter.adapt_many(adapter.JobAdapter, jobs,
            include_job_run, include_action_runs)

    def render_GET(self, request):
        include_job_runs = requestargs.get_bool(request, 'include_job_runs')
        include_action_runs = requestargs.get_bool(request, 'include_action_runs')
        output = dict(jobs=self.get_data(include_job_runs, include_action_runs))
        return respond(request, output)

    def render_POST(self, request):
        return handle_command(request, self.controller, self.job_collection)


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

    def __init__(self, service_collection):
        self.collection = service_collection
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

    def get_namespaces(self):
        return self.controller.get_namespaces()

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
        recorder      = event.get_recorder(self.entity_name)
        response_data = adapter.adapt_many(adapter.EventAdapter, recorder.list())
        return respond(request, dict(data=response_data))


class RootResource(resource.Resource):
    def __init__(self, mcp, web_path):
        self._master_control = mcp
        resource.Resource.__init__(self)

        # Setup children
        self.putChild('jobs',     JobCollectionResource(mcp.get_job_collection()))
        self.putChild('services', ServiceCollectionResource(mcp.get_service_collection()))
        self.putChild('config',   ConfigResource(mcp))
        # TODO: namespaces
        self.putChild('status',   StatusResource(mcp))
        self.putChild('events',   EventResource(''))
        self.putChild('web',      static.File(web_path))

    def getChild(self, name, request):
        if name == '':
            return self
        return resource.Resource.getChild(self, name, request)

    def urls_from_child(self, child_name):
        def name_url_dict(source):
            return dict((i['name'], i['url']) for i in source)
        return name_url_dict(self.children[child_name].get_data())

    def render_GET(self, request):
        """Return an index of urls for resources."""
        response = {
            'jobs':             self.urls_from_child('jobs'),
            'services':         self.urls_from_child('services'),
            'jobs_url':         request.uri + request.childLink('jobs'),
            'services_url':     request.uri + request.childLink('services'),
            'config_url':       request.uri + request.childLink('config'),
            'status_url':       request.uri + request.childLink('status'),
            'namespaces':       self.children['config'].get_namespaces()
        }
        return respond(request, response)


class LogAdapter(object):

    def __init__(self, logger):
        self.logger = logger

    def write(self, line):
        self.logger.info(line.rstrip('\n'))

    def close(self):
        pass


class TronSite(server.Site):
    """Subclass of a twisted Site to customize logging."""

    access_log = logging.getLogger('%s.access' % __name__)

    @classmethod
    def create(cls, mcp, web_path):
        return cls(RootResource(mcp, web_path))

    def startFactory(self):
        server.Site.startFactory(self)
        self.logFile = LogAdapter(self.access_log)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.resource)
