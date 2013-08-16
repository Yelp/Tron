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
    for key, val in (headers or {}).iteritems():
        request.setHeader(key, val)
    return json.dumps(response_dict, cls=JSONEncoder) if response_dict else ""


def handle_command(request, api_controller, obj, **kwargs):
    """Handle a request to perform a command."""
    command = requestargs.get_string(request, 'command')
    log.info("Handling '%s' request on %s", command, obj)
    try:
        response = api_controller.handle_command(command, **kwargs)
        return respond(request, {'result': response})
    except controller.UnknownCommandError, e:
        log.warning("Unknown command %s for %s", command, obj)
        return respond(request, {'error': str(e)}, code=http.NOT_IMPLEMENTED)


def resource_from_collection(collection, name, child_resource):
    """Return a child resource from a collection by name.  If no item is found,
    return NoResource.
    """
    item = collection.get_by_name(name)
    if item is None:
        return resource.NoResource("Cannot find child %s" % name)
    return child_resource(item)


class ActionRunResource(resource.Resource):

    isLeaf = True

    def __init__(self, action_run, job_run):
        resource.Resource.__init__(self)
        self.action_run = action_run
        self.job_run    = job_run
        self.controller = controller.ActionRunController(action_run, job_run)

    def render_GET(self, request):
        run_adapter = adapter.ActionRunAdapter(
            self.action_run,
            self.job_run,
            requestargs.get_integer(request, 'num_lines'),
            include_stdout=requestargs.get_bool(request, 'include_stdout'),
            include_stderr=requestargs.get_bool(request, 'include_stderr'))
        return respond(request, run_adapter.get_repr())

    def render_POST(self, request):
        return handle_command(request, self.controller, self.action_run)


class JobRunResource(resource.Resource):

    def __init__(self, job_run, job_container):
        resource.Resource.__init__(self)
        self.job_run       = job_run
        self.job_container = job_container
        self.controller    = controller.JobRunController(job_run, job_container)

    def getChild(self, action_name, _):
        if not action_name:
            return self
        if action_name == '_events':
            return EventResource(self.job_run.id)
        if action_name in self.job_run.action_runs:
            action_run = self.job_run.action_runs[action_name]
            return ActionRunResource(action_run, self.job_run)

        msg = "Cannot find action %s for %s"
        return resource.NoResource(msg % (action_name, self.job_run))

    def render_GET(self, request):
        include_runs = requestargs.get_bool(request, 'include_action_runs')
        include_graph = requestargs.get_bool(request, 'include_action_graph')
        run_adapter = adapter.JobRunAdapter(self.job_run,
            include_action_runs=include_runs,
            include_action_graph=include_graph)
        return respond(request, run_adapter.get_repr())

    def render_POST(self, request):
        return handle_command(request, self.controller, self.job_run)


def is_negative_int(string):
    return string.startswith('-') and string[1:].isdigit()


class JobResource(resource.Resource):

    def __init__(self, job_container):
        resource.Resource.__init__(self)
        self.job_container = job_container
        self.controller    = controller.JobController(job_container)

    def get_run_from_identifier(self, run_id):
        job_runs = self.job_container.get_runs()
        if run_id.upper() == 'HEAD':
            return job_runs.get_newest()
        if run_id.isdigit():
            return job_runs.get_run_by_num(int(run_id))
        if is_negative_int(run_id):
            return job_runs.get_run_by_index(int(run_id))
        return job_runs.get_run_by_state_short_name(run_id)

    def getChild(self, run_id, _):
        if not run_id:
            return self
        if run_id == '_events':
            return EventResource(self.job_container.get_name())

        run = self.get_run_from_identifier(run_id)
        if run:
            return JobRunResource(run, self.job_container)

        job = self.job_container
        if run_id in job.action_graph.names:
            action_runs = job.job_runs.get_action_runs(run_id)
            return ActionRunHistoryResource(action_runs)
        msg = "Cannot find job run %s for %s"
        return resource.NoResource(msg % (run_id, job))

    def render_GET(self, request):
        include_action_runs = requestargs.get_bool(request, 'include_action_runs')
        include_graph = requestargs.get_bool(request, 'include_action_graph')
        num_runs = requestargs.get_integer(request, 'num_runs')
        job_adapter = adapter.JobAdapter(
                self.job_container,
                include_job_runs=True,
                include_action_runs=include_action_runs,
                include_action_graph=include_graph,
                num_runs=num_runs)
        return respond(request, job_adapter.get_repr())

    def render_POST(self, request):
        run_time = requestargs.get_datetime(request, 'run_time')
        return handle_command(
            request,
            self.controller,
            self.job_container,
            run_time=run_time)


class ActionRunHistoryResource(resource.Resource):

    isLeaf = True

    def __init__(self, action_runs):
        resource.Resource.__init__(self)
        self.action_runs = action_runs

    def render_GET(self, request):
        return respond(request,
            adapter.adapt_many(adapter.ActionRunAdapter, self.action_runs))


class JobCollectionResource(resource.Resource):

    def __init__(self, job_collection):
        self.job_collection = job_collection
        self.controller     = controller.JobCollectionController(job_collection)
        resource.Resource.__init__(self)

    def getChild(self, name, request):
        if not name:
            return self
        return resource_from_collection(self.job_collection, name, JobResource)

    def get_data(self, include_job_run=False, include_action_runs=False, namespace=None, hostname=None):
        collection = self.job_collection
        if namespace:
            collection = [job for job in collection if job in
                          self.job_collection.get_jobs_by_namespace(namespace)]
        if hostname:
            collection = [job for job in collection if job in
                          self.job_collection.get_jobs_by_hostname(hostname)]

        return adapter.adapt_many(adapter.JobAdapter,
            collection,
            include_job_run,
            include_action_runs,
            num_runs=5)

    def get_job_index(self):
        jobs = adapter.adapt_many(
            adapter.JobIndexAdapter, self.job_collection)
        return dict((job['name'], job['actions']) for job in jobs)

    def render_GET(self, request):
        include_job_runs = requestargs.get_bool(request, 'include_job_runs')
        include_action_runs = requestargs.get_bool(request, 'include_action_runs')
        namespace = requestargs.get_string(request, 'namespace')
        hostname = requestargs.get_string(request, 'hostname')
        output = dict(jobs=self.get_data(include_job_runs, include_action_runs,
            namespace, hostname))
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
        if not name:
            return self
        if name == '_events':
            return EventResource(str(self.service))

        number = int(name) if name.isdigit() else None
        instance = self.service.instances.get_by_number(number)
        if instance:
            return ServiceInstanceResource(instance)

        return resource.NoResource("Cannot find service instance: %s" % name)

    def render_GET(self, request):
        include_events = requestargs.get_integer(request, 'include_events')
        response = adapter.ServiceAdapter(self.service,
            include_events=include_events).get_repr()
        return respond(request, response)

    def render_POST(self, request):
        return handle_command(request, self.controller, self.service)


class ServiceCollectionResource(resource.Resource):
    """Resource for ServiceCollection."""

    def __init__(self, service_collection):
        self.collection = service_collection
        resource.Resource.__init__(self)

    def getChild(self, name, _):
        if not name:
            return self
        return resource_from_collection(self.collection, name, ServiceResource)

    def get_data(self, namespace=None, hostname=None):
        collection = self.collection
        if namespace:
            collection = [job for job in collection if job in
                          self.collection.get_services_by_namespace(namespace)]
        if hostname:
            collection = [job for job in collection if job in
                          self.collection.get_services_by_hostname(hostname)]

        return adapter.adapt_many(adapter.ServiceAdapter, collection)

    def get_service_index(self):
        return self.collection.get_names()

    def render_GET(self, request):
        namespace = requestargs.get_string(request, 'namespace')
        hostname = requestargs.get_string(request, 'hostname')
        return respond(request, dict(services=self.get_data(namespace, hostname)))


class ConfigResource(resource.Resource):
    """Resource for configuration changes"""

    isLeaf = True

    def __init__(self, master_control):
        self.controller = controller.ConfigController(master_control)
        resource.Resource.__init__(self)

    def get_config_index(self):
        return self.controller.get_namespaces()

    def render_GET(self, request):
        config_name = requestargs.get_string(request, 'name')
        no_header = requestargs.get_bool(request, 'no_header')
        if not config_name:
            return respond(request, {'error': "'name' for config is required."})
        response = self.controller.read_config(
                config_name, add_header=not no_header)
        return respond(request, response)

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


class ApiRootResource(resource.Resource):

    def __init__(self, mcp):
        self._master_control = mcp
        resource.Resource.__init__(self)

        # Setup children
        self.putChild('jobs',
            JobCollectionResource(mcp.get_job_collection()))
        self.putChild('services',
            ServiceCollectionResource(mcp.get_service_collection()))
        self.putChild('config',   ConfigResource(mcp))
        self.putChild('status',   StatusResource(mcp))
        self.putChild('events',   EventResource(''))
        self.putChild('', self)

    def render_GET(self, request):
        """Return an index of urls for resources."""
        response = {
            'jobs':             self.children['jobs'].get_job_index(),
            'services':         self.children['services'].get_service_index(),
            'namespaces':       self.children['config'].get_config_index()
        }
        return respond(request, response)


class RootResource(resource.Resource):

    def __init__(self, mcp, web_path):
        resource.Resource.__init__(self)
        self.web_path = web_path
        self.mcp = mcp
        self.putChild('api', ApiRootResource(self.mcp))
        self.putChild('web', static.File(web_path))
        self.putChild('', self)

    def render_GET(self, request):
        request.redirect(request.prePathURL() + 'web')
        request.finish()
        return server.NOT_DONE_YET

    def __str__(self):
        return "%s(%s, %s)" % (type(self).__name__, self.mcp, self.web_path)

class LogAdapter(object):

    def __init__(self, logger):
        self.logger = logger

    def write(self, line):
        self.logger.info(line.rstrip('\n'))

    def close(self):
        pass


class TronSite(server.Site):
    """Subclass of a twisted Site to customize logging."""

    access_log = logging.getLogger('tron.api.www.access')

    @classmethod
    def create(cls, mcp, web_path):
        return cls(RootResource(mcp, web_path))

    def startFactory(self):
        server.Site.startFactory(self)
        self.logFile = LogAdapter(self.access_log)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.resource)
