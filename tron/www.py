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

from tron import action
from tron import job
from tron import service
from tron.utils import timeutils


log = logging.getLogger("tron.www")


class JSONEncoder(json.JSONEncoder):
    """Custom JSON for certain objects"""

    def default(self, o):
        # This method is implemented by all of our core objects (Job, Node, etc)
        if hasattr(o, 'repr_data') and callable(o.repr_data):
            return o.repr_data()

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

    def __init__(self, act_run):
        self._act_run = act_run
        resource.Resource.__init__(self)

    def get_data(self, num_lines=10):
        act_run = self._act_run
        duration = str(
            timeutils.duration(act_run.start_time, act_run.end_time) or ''
        )

        data = act_run.repr_data(num_lines)
        data['duration'] = duration
        return data

    def render_GET(self, request):
        num_lines = None
        if request.args and request.args['num_lines'][0].isdigit():
            num_lines = int(request.args['num_lines'][0])

        return respond(request, self.get_data(num_lines))

    def render_POST(self, request):
        cmd = request.args['command'][0]
        log.info("Handling '%s' request for action run %s",
                 cmd, self._act_run.id)

        if cmd not in ('start', 'succeed', 'cancel', 'fail', 'skip'):
            log.warning("Unknown request command %s", request.args['command'])
            return respond(request, None, code=http.NOT_IMPLEMENTED)

        try:
            resp = getattr(self._act_run, '%s' % cmd)()
        except action.Error:
            resp = None
        if not resp:
            log.info("Failed to %s action run %r." % (cmd, self._act_run))
            return respond(request, {
                'result': "Failed to %s. Action in state: %s" % (
                    cmd,
                    self._act_run.state.short_name)
                })

        return respond(request, {'result': "Action run now in state %s" %
                                 self._act_run.state.short_name})


class JobRunResource(resource.Resource):

    isLeaf = False

    def __init__(self, run):
        self._run = run
        resource.Resource.__init__(self)

    def getChild(self, act_name, request):
        if act_name == '':
            return self
        elif act_name == '_events':
            return EventResource(self._run)

        for act_run in self._run.action_runs_with_cleanup:
            if act_name == act_run.action.name:
                return ActionRunResource(act_run)

        return resource.NoResource("Cannot find action '%s' for job run '%s'" %
                                   (act_name, self._run.id))

    def get_data(self, include_action_runs=False):
        run = self._run
        data = run.repr_data()
        if include_action_runs:
            data['runs'] = [
                ActionRunResource(action_run).get_data()
                for action_run in run.action_runs_with_cleanup
            ]

        duration = str(timeutils.duration(run.start_time, run.end_time) or '')
        data['duration'] = duration
        data['href'] = '/jobs/%s/%s' % (run.job.name, run.run_num)
        return data

    def render_GET(self, request):
        return respond(request, self.get_data(include_action_runs=True))

    def render_POST(self, request):
        cmd = request.args['command'][0]
        log.info("Handling '%s' request for job run %s", cmd, self._run.id)

        if cmd in ['start', 'restart', 'succeed', 'fail', 'cancel']:
            getattr(self, '_%s' % cmd)(request)
        else:
            log.warning("Unknown request command %s", request.args['command'])
            return respond(request, None, code=http.NOT_IMPLEMENTED)

        return respond(request, {'result': "Job run now in state %s" %
                                 self._run.state.short_name})

    def _restart(self, request):
        log.info("Resetting all action runs to scheduled state")
        self._run.schedule()
        self._start(request)

    def _start(self, request):
        try:
            log.info("Starting job run %s", self._run.id)
            self._run.start()
        except job.Error, e:
            log.warning("Failed to start job run %r", e)

    def _succeed(self, request):
        if not self._run.is_running and not self._run.is_success:
            log.info("Marking job run %s for success", self._run.id)
            self._run.succeed()
        else:
            log.warning("Request to mark job run %s succeed when it has"
                        " already", self._run.id)

    def _cancel(self, request):
        if self._run.is_scheduled or self._run.is_queued:
            log.info("Cancelling job %s", self._run.id)
            self._run.cancel()
        else:
            log.warning("Request to cancel job run %s when it's already"
                        " cancelled", self._run.id)

    def _fail(self, request):
        if (not self._run.is_running and
            not self._run.is_success and
            not self._run.is_failure):
            log.info("Marking job run %s as failed", self._run.id)
            self._run.fail()
        else:
            log.warning("Request to fail job run %s when it's already running"
                        " or done", self._run.id)


class JobResource(resource.Resource):
    """A resource that describes a particular job"""

    isLeaf = False

    def __init__(self, job, master_control):
        self._job = job
        self._master_control = master_control
        resource.Resource.__init__(self)

    def getChild(self, run_num, request):
        if run_num == '':
            return self
        elif run_num == '_events':
            return EventResource(self._job)

        run = None
        run_num = run_num.upper()

        if run_num == 'HEAD':
            run = self._job.newest()

        if not run:
            # May be none if run_num is not a state.short_name
            run = self._job.newest_run_by_state(run_num)

        if run_num.isdigit():
            run = self._job.get_run_by_num(int(run_num))

        if run:
            return JobRunResource(run)
        return resource.NoResource("Cannot run number '%s' for job '%s'" %
                                   (run_num, self._job.name))

    def get_data(self, include_job_run=False, include_action_runs=False):
        data = self._job.repr_data()
        data['href'] = '/jobs/%s' % urllib.quote(self._job.name)

        if include_job_run:
            data['runs'] = [
                JobRunResource(job_run).get_data(include_action_runs)
                for job_run in self._job.runs
            ]
        return data

    def render_GET(self, request):
        include_action_runs = False
        if request.args:
            if 'include_action_runs' in request.args:
                include_action_runs = True
        return respond(request, self.get_data(True, include_action_runs))

    def render_POST(self, request):
        cmd = request.args['command'][0]

        log.info("Handling '%s' request for job run %s", cmd, self._job.name)

        if cmd == 'enable':
            self._master_control.enable_job(self._job)
            return respond(request, {'result': "Job %s is enabled" %
                                     self._job.name})

        if cmd == 'disable':
            self._master_control.disable_job(self._job)
            return respond(request, {'result': "Job %s is disabled" %
                                     self._job.name})

        if cmd == 'start':
            if 'run_time' in request.args:
                run_time_str = request.args['run_time'][0]
                run_time = datetime.datetime.strptime(run_time_str,
                                                      "%Y-%m-%d %H:%M:%S")
            else:
                run_time = timeutils.current_time()

            runs = self._job.manual_start(run_time=run_time)
            return respond(request, {'result': "New Job Runs %s created" %
                                     [r.id for r in runs]})

        log.warning("Unknown request job command %s", request.args['command'])
        return respond(request, None, code=http.NOT_IMPLEMENTED)


class JobsResource(resource.Resource):
    """Resource for all our daemon's jobs"""

    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)

    def getChild(self, name, request):
        if name == '':
            return self

        found = self._master_control.jobs.get(name)
        if found is None:
            return resource.NoResource("Cannot find job '%s'" % name)

        return JobResource(found, self._master_control)

    def get_data(self, include_job_run=False, include_action_runs=False):
        mcp = self._master_control
        return [
            JobResource(job, mcp).get_data(include_job_run, include_action_runs)
            for job in self._master_control.jobs.itervalues()
        ]

    def render_GET(self, request):
        include_job_runs = include_action_runs = False
        if request.args:
            if 'include_job_runs' in request.args:
                include_job_runs = True
            if 'include_action_runs' in request.args:
                include_action_runs = True

        output = {
            'jobs': self.get_data(include_job_runs, include_action_runs),
        }
        return respond(request, output)

    def render_POST(self, request):
        cmd = request.args['command'][0]
        log.info("Handling '%s' request on all jobs", cmd)

        if cmd == 'disableall':
            self._master_control.disable_all()
            return respond(request, {'result': "All jobs are now disabled"})

        if cmd == 'enableall':
            self._master_control.enable_all()
            return respond(request, {'result': "All jobs are now enabled"})

        log.warning("Unknown request command %s for all jobs",
                    request.args['command'])
        return respond(request, None, code=http.NOT_IMPLEMENTED)


class ServiceInstanceResource(resource.Resource):

    isLeaf = True

    def __init__(self, service_instance, master_control):
        self._service_instance = service_instance
        self._master_control = master_control
        resource.Resource.__init__(self)

    def render_POST(self, request):
        cmd = request.args['command'][0]
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

        log.warning("Unknown request command %s for service %s",
                    request.args['command'],
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
        elif name == '_events':
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
        cmd = request.args['command'][0]
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
                    request.args['command'], self._service.name)
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

        service_list = self.get_data()

        output = {
            'services': service_list,
        }
        return respond(request, output)


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
        new_config = request.args['config'][0]
        self._master_control.rewrite_config(new_config)

        # TODO: This should be a more informative response
        response = {'status': "I'm alive biatch"}
        try:
            self._master_control.live_reconfig()
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
        assert hasattr(recordable, 'event_recorder')
        self._recordable = recordable

    def render_GET(self, request):
        response = {'data': []}

        for evt in self._recordable.event_recorder.list():
            entity_desc = "UNKNOWN"
            if evt.entity:
                entity_desc = str(evt.entity)
            response['data'].append({
                'level': evt.level,
                'name': evt.name,
                'entity': entity_desc,
                'time': evt.time.strftime("%Y-%m-%d %H:%M:%S")
            })

        return respond(request, response)


class RootResource(resource.Resource):
    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)

        # Setup children
        self.putChild('jobs', JobsResource(master_control))
        self.putChild('services', ServicesResource(master_control))
        self.putChild('config', ConfigResource(master_control))
        self.putChild('status', StatusResource(master_control))
        self.putChild('events', EventResource(master_control))

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

        response = dict()
        response['jobs'] = jobs_resource.get_data()
        response['jobs_href'] = request.uri + request.childLink('jobs')

        response['services'] = services_resource.get_data()
        response['services_href'] = request.uri + request.childLink('services')

        response['config_href'] = request.uri + request.childLink('config')
        response['status_href'] = request.uri + request.childLink('status')

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
