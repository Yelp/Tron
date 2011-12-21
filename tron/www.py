"""Management Web Services Interface
"""

import datetime
import logging
import urllib

try:
    import json as simplejson
except ImportError:
    import simplejson


from twisted.cred import checkers
from twisted.internet import reactor
from twisted.web import http, resource, server


from tron import action
from tron import config
from tron import job
from tron import service
from tron.utils import timeutils


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
    return ""


def job_run_state(job_run):
    if job_run.is_success:
        return "SUCC"
    if job_run.is_cancelled:
        return "CANC"
    if job_run.is_running:
        return "RUNN"
    if job_run.is_failure:
        return "FAIL"
    if job_run.is_scheduled:
        return "SCHE"
    if job_run.is_queued:
        return "QUE"

    return "UNKWN"


class ActionRunResource(resource.Resource):

    isLeaf = True

    def __init__(self, act_run):
        self._act_run = act_run
        resource.Resource.__init__(self)

    def render_GET(self, request):
        output = {
            'id': self._act_run.id,
            'state': job_run_state(self._act_run),
            'node': self._act_run.node.hostname,
            'command': self._act_run.command,
            'raw_command': self._act_run.action.command,
            'requirements': [req.name
                             for req in self._act_run.action.required_actions],
        }

        if request.args and request.args['num_lines'][0].isdigit():
            num_lines = int(request.args['num_lines'][0])
            output['stdout'] = self._act_run.tail_stdout(num_lines)
            output['stderr'] = self._act_run.tail_stderr(num_lines)

        return respond(request, output)

    def render_POST(self, request):
        cmd = request.args['command'][0]
        log.info("Handling '%s' request for action run %s",
                 cmd, self._act_run.id)

        if cmd == 'start':
            self._start(request)
        elif cmd == 'succeed':
            self._succeed(request)
        elif cmd == 'cancel':
            self._cancel(request)
        elif cmd == 'fail':
            self._fail(request)
        else:
            log.warning("Unknown request command %s", request.args['command'])
            return respond(request, None, code=http.NOT_IMPLEMENTED)

        return respond(request, {'result': "Action run now in state %s" %
                                 job_run_state(self._act_run)})

    def _start(self, request):
        if not self._act_run.is_success and not self._act_run.is_running:
            log.info("Starting job run %s", self._act_run.id)
            try:
                self._act_run.start()
            except action.Error, e:
                log.info("Failed to start action run %r", e)
        else:
            log.warning("Request to start job run %s when it's already done",
                        self._act_run.id)

    def _succeed(self, request):
        if not self._act_run.is_running and not self._act_run.is_success:
            log.info("Marking job run %s for success", self._act_run.id)
            self._act_run.succeed()
        else:
            log.warning("Request to mark job run %s succeeded when it's"
                        " running or already succeeded", self._act_run.id)

    def _cancel(self, request):
        if self._act_run.is_scheduled or self._act_run.is_queued:
            log.info("Cancelling job %s", self._act_run.id)
            self._act_run.cancel()
        else:
            log.warning("Request to cancel job run %s when it's not possible",
                        self._act_run.id)

    def _fail(self, request):
        if (not self._act_run.is_running and
            not self._act_run.is_success and
            not self._act_run.is_failure):
            log.info("Marking job run %s as failed", self._act_run.id)
            self._act_run.fail(0)
        else:
            log.warning("Request to fail job run %s when it's already running"
                        " or done", self._act_run.id)


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

    def render_GET(self, request):
        run_output = []
        state = job_run_state(self._run)

        def action_output(action_run):
            action_state = job_run_state(action_run)

            last_time = (action_run.end_time
                         if action_run.end_time
                         else timeutils.current_time())
            duration = (str(last_time - action_run.start_time)
                        if action_run.start_time
                        else "")

            return {
                'id': action_run.id,
                'name': action_run.action.name,
                'run_time': action_run.run_time and str(action_run.run_time),
                'start_time': (action_run.start_time and
                               str(action_run.start_time)),
                'end_time': action_run.end_time and str(action_run.end_time),
                'exit_status': action_run.exit_status,
                'duration': duration,
                'state': action_state,
                'command': action_run.command,
            }

        run_output = [action_output(action_run)
                      for action_run in self._run.action_runs_with_cleanup]

        output = {
            'runs': run_output,
            'id': self._run.id,
            'state': state,
            'node': self._run.node.hostname,
        }

        return respond(request, output)

    def render_POST(self, request):
        cmd = request.args['command'][0]
        log.info("Handling '%s' request for job run %s", cmd, self._run.id)

        if cmd == "start":
            self._start(request)
        elif cmd == 'restart':
            self._restart(request)
        elif cmd == "succeed":
            self._succeed(request)
        elif cmd == "fail":
            self._fail(request)
        elif cmd == "cancel":
            self._cancel(request)
        else:
            log.warning("Unknown request command %s", request.args['command'])
            return respond(request, None, code=http.NOT_IMPLEMENTED)

        return respond(request, {'result': "Job run now in state %s" %
                                 job_run_state(self._run)})

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

        if run_num.upper() == 'HEAD':
            run = self._job.newest()
        if run_num.upper() in ['SUCC', 'CANC', 'RUNN', 'FAIL', 'SCHE', 'QUE',
                               'UNKWN']:
            run = self._job.newest_run_by_state(run_num.upper())
        if run_num.isdigit():
            run = self._job.get_run_by_num(int(run_num))

        if run:
            return JobRunResource(run)
        return resource.NoResource("Cannot run number '%s' for job '%s'" %
                                   (run_num, self._job.name))

    def get_run_data(self, request, run):
        state = job_run_state(run)
        last_time = run.end_time if run.end_time else timeutils.current_time()
        duration = str(last_time - run.start_time) if run.start_time else ""

        return {
                'id': run.id,
                'href': request.childLink(run.id),
                'node': run.node.hostname if run.node else None,
                'run_time': run.run_time and str(run.run_time),
                'start_time': run.start_time and str(run.start_time),
                'end_time': run.end_time and str(run.end_time),
                'duration': duration,
                'run_num': run.run_num,
                'state': state,
            }

    def render_GET(self, request):
        run_output = []
        for job_run in self._job.runs:
            run_output.append(self.get_run_data(request, job_run))

        resources_output = []

        output = {
            'name': self._job.name,
            'scheduler': str(self._job.scheduler),
            'runs': run_output,
            'action_names': map(lambda t: t.name, self._job.topo_actions),
            'node_pool': map(lambda n: n.hostname, self._job.node_pool.nodes),
        }
        return respond(request, output)

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

    def get_data(self, request):
        serv_list = []
        job_list = []
        for current_job in self._master_control.jobs.itervalues():
            last_success = None
            if current_job.last_success and current_job.last_success.end_time:
                fmt = "%Y-%m-%d %H:%M:%S"
                last_success = current_job.last_success.end_time.strftime(fmt)

            # We need to describe the current state of this job
            current_run = current_job.next_to_finish()
            status = "UNKNOWN"

            if current_run and current_run.is_running:
                status = "RUNNING"
            elif current_run and current_run.is_scheduled:
                status = "ENABLED"
            elif not current_run:
                status = "DISABLED"

            job_desc = {
                'name': current_job.name,
                'href': "/jobs/%s" % urllib.quote(current_job.name),
                'status': status,
                'scheduler': str(current_job.scheduler),
                'last_success': last_success,
            }
            job_list.append(job_desc)

        return job_list

    def render_GET(self, request):
        request.setHeader("content-type", "text/json")

        output = {
            'jobs': self.get_data(request),
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

        found = None
        for instance in self._service.instances:
            if str(instance.instance_number) == str(name):
                return ServiceInstanceResource(instance, self._master_control)
        else:
            return resource.NoResource("Cannot find service '%s'" % name)

    def get_instance_data(self, request, instance):
        return {
                'id': instance.id,
                'node': instance.node.hostname if instance.node else None,
                'state': instance.state.name,
            }

    def render_GET(self, request):
        instance_output = []
        for instance in self._service.instances:
            instance_output.append(self.get_instance_data(request, instance))

        resources_output = []

        output = {
            'name': self._service.name,
            'state': self._service.state.name.upper(),
            'count': self._service.count,
            'command': self._service.command,
            'instances': instance_output,
            'node_pool': map(lambda n: n.hostname,
                             self._service.node_pool.nodes),
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

    def get_data(self, request):
        service_list = []
        for current_service in self._master_control.services.itervalues():
            try:
                status = current_service.state.name.upper()
            except:
                status = "BROKEN"
            try:
                count = current_service.count
            except:
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

        service_list = self.get_data(request)

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

        # TODO: This should be a more informative reponse
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
        response = {}
        response['data'] = []

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
        response['jobs'] = jobs_resource.get_data(request)
        response['jobs_href'] = request.uri + request.childLink('jobs')

        response['services'] = services_resource.get_data(request)
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
