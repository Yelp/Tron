"""
Web Services Interface used by command-line clients and web frontend to
view current state, event history and send commands to trond.
"""
import collections
import datetime
import logging
import traceback

try:
    import simplejson as json
    _silence_pyflakes = [json]
except ImportError:
    import json

from twisted.web import http, resource, static, server

from tron import __version__
from tron.api import adapter, controller
from tron.api import requestargs
from tron.api.async_resource import AsyncResource
from tron.metrics import view_all_metrics
from tron.metrics import meter
from tron.utils import maybe_decode

log = logging.getLogger(__name__)


class JSONEncoder(json.JSONEncoder):
    """Custom JSON for certain objects"""

    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.strftime("%Y-%m-%d %H:%M:%S")

        if isinstance(o, datetime.date):
            return o.isoformat()

        if isinstance(o, collections.KeysView):
            return list(o)

        return super(JSONEncoder, self).default(o)


def respond(request, response, code=None, headers=None):
    """Helper to generate a json response"""
    if code is None:
        if type(response) is dict and response.get('error'):
            code = http.INTERNAL_SERVER_ERROR
        else:
            code = http.OK
    request.setResponseCode(code)
    request.setHeader(b'content-type', b'application/json; charset=utf-8')
    request.setHeader(b'Access-Control-Allow-Origin', b'*')
    for key, val in (headers or {}).items():
        request.setHeader(str(key), str(val))

    result = json.dumps(
        response,
        cls=JSONEncoder,
    ) if response else ""

    if type(result) is not bytes:
        result = result.encode('utf8')

    return result


def handle_command(request, api_controller, obj, **kwargs):
    """Handle a request to perform a command."""
    command = requestargs.get_string(request, 'command')
    log.info("Handling '%s' request on %s", command, obj)
    try:
        response = api_controller.handle_command(command, **kwargs)
        return respond(request=request, response={'result': response})
    except controller.UnknownCommandError:
        error_msg = f"Unknown command '{command}' for '{obj}'"
        log.warning(error_msg)
        return respond(
            request=request, response={'error': error_msg}, code=http.NOT_IMPLEMENTED
        )
    except Exception as e:
        log.exception('%r while executing command %s for %s', e, command, obj)
        trace = traceback.format_exc()
        return respond(request=request, response={'error': trace})


class ErrorResource(resource.Resource):
    """ Equivalent to resource.NoResource, except error message is returned
    as JSON, not HTML """

    def __init__(self, error='No Such Resource', code=http.NOT_FOUND):
        resource.Resource.__init__(self)
        self.code = code
        self.error = error

    @AsyncResource.bounded
    def render_GET(self, request):
        return respond(request=request, response={'error': self.error}, code=self.code)

    @AsyncResource.exclusive
    def render_POST(self, request):
        return respond(request=request, response={'error': self.error}, code=self.code)

    def getChild(self, chnam, request):
        """ Overrided getChild to ensure a NoResource is not returned """
        return self


def resource_from_collection(collection, name, child_resource):
    """Return a child resource from a collection by name.  If no item is found,
    return ErrorResource.
    """
    item = collection.get_by_name(name)
    if item is None:
        return ErrorResource("Cannot find child '%s'" % name)
    return child_resource(item)


class ActionRunResource(resource.Resource):

    isLeaf = True

    def __init__(self, action_run, job_run):
        resource.Resource.__init__(self)
        self.action_run = action_run
        self.job_run = job_run
        self.controller = controller.ActionRunController(action_run, job_run)

    @AsyncResource.bounded
    def render_GET(self, request):
        run_adapter = adapter.ActionRunAdapter(
            self.action_run,
            self.job_run,
            requestargs.get_integer(request, 'num_lines'),
            include_stdout=requestargs.get_bool(request, 'include_stdout'),
            include_stderr=requestargs.get_bool(request, 'include_stderr'),
        )
        return respond(request=request, response=run_adapter.get_repr())

    @AsyncResource.exclusive
    def render_POST(self, request):
        return handle_command(request, self.controller, self.action_run)


class JobRunResource(resource.Resource):
    def __init__(self, job_run, job_scheduler):
        resource.Resource.__init__(self)
        self.job_run = job_run
        self.job_scheduler = job_scheduler
        self.controller = controller.JobRunController(job_run, job_scheduler)

    def getChild(self, action_name, _):
        if not action_name:
            return self

        action_name = maybe_decode(action_name)
        if action_name in self.job_run.action_runs:
            action_run = self.job_run.action_runs[action_name]
            return ActionRunResource(action_run, self.job_run)

        return ErrorResource(
            f"Cannot find action '{action_name}' for "
            f"'{self.job_run}'"
        )

    @AsyncResource.bounded
    def render_GET(self, request):
        include_runs = requestargs.get_bool(request, 'include_action_runs')
        include_graph = requestargs.get_bool(request, 'include_action_graph')
        run_adapter = adapter.JobRunAdapter(
            self.job_run,
            include_action_runs=include_runs,
            include_action_graph=include_graph,
        )
        return respond(request=request, response=run_adapter.get_repr())

    @AsyncResource.exclusive
    def render_POST(self, request):
        return handle_command(request, self.controller, self.job_run)


def is_negative_int(string):
    return string.startswith('-') and string[1:].isdigit()


class JobResource(resource.Resource):
    def __init__(self, job_scheduler):
        resource.Resource.__init__(self)
        self.job_scheduler = job_scheduler
        self.controller = controller.JobController(job_scheduler)

    def get_run_from_identifier(self, run_id):
        job_runs = self.job_scheduler.get_job_runs()
        if run_id.upper() == 'HEAD':
            return job_runs.get_newest()
        if run_id.isdigit():
            return job_runs.get_run_by_num(int(run_id))
        if is_negative_int(run_id):
            return job_runs.get_run_by_index(int(run_id))

    def getChild(self, run_id, _):
        if not run_id:
            return self

        run_id = maybe_decode(run_id)
        run = self.get_run_from_identifier(run_id)
        if run:
            return JobRunResource(run, self.job_scheduler)

        job = self.job_scheduler.get_job()
        if run_id in job.action_graph.names():
            action_runs = job.runs.get_action_runs(run_id)
            return ActionRunHistoryResource(action_runs)

        return ErrorResource(f"Cannot find job run '{run_id}' for '{job}'")

    @AsyncResource.bounded
    def render_GET(self, request):
        include_action_runs = requestargs.get_bool(
            request,
            'include_action_runs',
        )
        include_graph = requestargs.get_bool(request, 'include_action_graph')
        num_runs = requestargs.get_integer(request, 'num_runs')
        job_adapter = adapter.JobAdapter(
            self.job_scheduler.get_job(),
            include_job_runs=True,
            include_action_runs=include_action_runs,
            include_action_graph=include_graph,
            num_runs=num_runs,
        )
        return respond(request=request, response=job_adapter.get_repr())

    @AsyncResource.exclusive
    def render_POST(self, request):
        run_time = requestargs.get_datetime(request, 'run_time')
        return handle_command(
            request,
            self.controller,
            self.job_scheduler,
            run_time=run_time,
        )


class ActionRunHistoryResource(resource.Resource):

    isLeaf = True

    def __init__(self, action_runs):
        resource.Resource.__init__(self)
        self.action_runs = action_runs

    @AsyncResource.bounded
    def render_GET(self, request):
        return respond(
            request=request,
            response=adapter.adapt_many(adapter.ActionRunAdapter, self.action_runs),
        )


class JobCollectionResource(resource.Resource):
    def __init__(self, job_collection):
        self.job_collection = job_collection
        self.controller = controller.JobCollectionController(job_collection)
        resource.Resource.__init__(self)

    def getChild(self, name, request):
        if not name:
            return self

        name = maybe_decode(name)
        return resource_from_collection(self.job_collection, name, JobResource)

    def get_data(
        self,
        include_job_run=False,
        include_action_runs=False,
        include_action_graph=True,
        include_node_pool=True,
    ):
        return adapter.adapt_many(
            adapter.JobAdapter,
            self.job_collection.get_jobs(),
            include_job_run,
            include_action_runs,
            include_action_graph,
            include_node_pool,
            num_runs=5,
        )

    def get_job_index(self):
        jobs = adapter.adapt_many(
            adapter.JobIndexAdapter,
            self.job_collection.get_jobs(),
        )
        return {job['name']: job['actions'] for job in jobs}

    @AsyncResource.bounded
    def render_GET(self, request):
        include_job_runs = requestargs.get_bool(
            request,
            'include_job_runs',
            default=False,
        )
        include_action_runs = requestargs.get_bool(
            request,
            'include_action_runs',
            default=False,
        )
        include_action_graph = requestargs.get_bool(
            request,
            'include_action_graph',
            default=True,
        )
        include_node_pool = requestargs.get_bool(
            request,
            'include_node_pool',
            default=True,
        )
        response = dict(
            jobs=self.get_data(
                include_job_runs,
                include_action_runs,
                include_action_graph,
                include_node_pool,
            ),
        )
        return respond(request=request, response=response)

    @AsyncResource.exclusive
    def render_POST(self, request):
        old_name = requestargs.get_string(request, 'old_name')
        new_name = requestargs.get_string(request, 'new_name')
        return handle_command(
            request=request,
            api_controller=self.controller,
            obj=self.job_collection,
            old_name=old_name,
            new_name=new_name,
        )


class ConfigResource(resource.Resource):
    """Resource for configuration changes"""

    isLeaf = True

    def __init__(self, master_control):
        self.controller = controller.ConfigController(master_control)
        resource.Resource.__init__(self)

    def get_config_index(self):
        return self.controller.get_namespaces()

    @AsyncResource.bounded
    def render_GET(self, request):
        config_name = requestargs.get_string(request, 'name')
        if not config_name:
            return respond(
                request=request,
                response={'error': "'name' for config is required."},
                code=http.BAD_REQUEST,
            )
        response = self.controller.read_config(config_name)
        return respond(request=request, response=response)

    @AsyncResource.exclusive
    def render_POST(self, request):
        config_content = requestargs.get_string(request, 'config')
        name = requestargs.get_string(request, 'name')
        config_hash = requestargs.get_string(request, 'hash')
        check = requestargs.get_bool(request, 'check')

        if not name:
            return respond(
                request=request,
                response={'error': "'name' for config is required."},
                code=http.BAD_REQUEST,
            )

        response = {'status': "Active"}

        if check:
            fn = self.controller.check_config
            req = "configure check"
        elif config_content == "":
            fn = self.controller.delete_config
            req = "configuration delete"
        else:
            fn = self.controller.update_config
            req = "reconfigure"

        log.info("Handling %s request: %s, %s" % (req, name, config_hash))
        error = fn(name, config_content, config_hash)

        if error:
            response['error'] = error
        return respond(request=request, response=response)


class StatusResource(resource.Resource):

    isLeaf = True

    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)

    @AsyncResource.bounded
    def render_GET(self, request):
        return respond(
            request=request, response={
                'status': "I'm alive.",
                'version': __version__,
                'boot_time': int(self._master_control.boot_time),
            }
        )


class MetricsResource(resource.Resource):

    isLeaf = True

    def __init__(self):
        resource.Resource.__init__(self)

    @AsyncResource.exclusive
    def render_GET(self, request):
        return respond(request=request, response=view_all_metrics())


class EventsResource(resource.Resource):
    isLeaf = True

    def __init__(self):
        super().__init__()
        self.controller = controller.EventsController()

    @AsyncResource.exclusive
    def render_GET(self, request):
        response = self.controller.info()
        return respond(request=request, response=response)

    @AsyncResource.bounded
    def render_POST(self, request):
        command = requestargs.get_string(request, 'command')
        if command not in self.controller.COMMANDS:
            return respond(
                request=request,
                response=dict(error=f'Unknown command: {command}'),
                code=http.BAD_REQUEST,
            )
        event = requestargs.get_string(request, 'event')
        fn = getattr(self.controller, command)
        response = fn(event)
        return respond(request=request, response=response)


class ApiRootResource(resource.Resource):
    def __init__(self, mcp):
        self._master_control = mcp
        resource.Resource.__init__(self)

        # Setup children
        self.putChild(
            b'jobs',
            JobCollectionResource(mcp.get_job_collection()),
        )

        self.putChild(b'config', ConfigResource(mcp))
        self.putChild(b'status', StatusResource(mcp))
        self.putChild(b'events', EventsResource())
        self.putChild(b'metrics', MetricsResource())
        self.putChild(b'', self)

    @AsyncResource.bounded
    def render_GET(self, request):
        """Return an index of urls for resources."""
        response = {
            'jobs': self.children[b'jobs'].get_job_index(),
            'namespaces': self.children[b'config'].get_config_index(),
        }
        return respond(request=request, response=response)


class RootResource(resource.Resource):
    def __init__(self, mcp, web_path):
        resource.Resource.__init__(self)
        self.web_path = web_path
        self.mcp = mcp
        self.putChild(b'api', ApiRootResource(self.mcp))
        self.putChild(b'web', static.File(web_path))
        self.putChild(b'', self)

    def render_GET(self, request):
        request.redirect(b'/web')
        request.finish()
        return server.NOT_DONE_YET

    def __str__(self):
        return "%s(%s, %s)" % (type(self).__name__, self.mcp, self.web_path)


class LogAdapter(object):
    def __init__(self, logger):
        self.logger = logger

    def write(self, line):
        self.logger.info(line.rstrip(b'\n'))

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

    def log(self, request):
        super().log(request)
        if 200 <= request.code < 300:
            meter('tron.site.2xx')
        if 300 <= request.code < 400:
            meter('tron.site.3xx')
        if 400 <= request.code < 500:
            meter('tron.site.4xx')
        if 500 <= request.code < 600:
            meter('tron.site.5xx')

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.resource)
