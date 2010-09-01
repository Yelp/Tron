"""Management Web Services Interface

Got to know what's going on ?
"""
import logging

from twisted.internet import reactor
from twisted.cred import checkers
from twisted.web import server, resource, http

import simplejson

from tron.utils import timeutils
from tron import config

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
    if job_run.is_failed:
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
            'requirements': [req.name for req in self._act_run.action.required_actions],
        }
        
        if request.args and request.args['num_lines'][0].isdigit():
            output['stdout'] = self._act_run.tail_stdout(int(request.args['num_lines'][0]))
            output['stderr'] = self._act_run.tail_stderr(int(request.args['num_lines'][0]))
            
        return respond(request, output)

    def render_POST(self, request):
        log.debug("Handling post request for action run %s", self._act_run.id)
        cmd = request.args['command'][0]
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

        return respond(request, {'result': "Action run now in result %s" % job_run_state(self._act_run)})
    
    def _start(self, request):
        if not self._act_run.is_success and not self._act_run.is_running:
            log.info("Starting job run %s", self._act_run.id)
            self._act_run.start()
        else:
            log.warning("Request to start job run %s when it's already done", self._act_run.id)

    def _succeed(self, request):
        if not self._act_run.is_running and not self._act_run.is_success:
            log.info("Marking job run %s for success", self._act_run.id)
            self._act_run.succeed()
        else:
            log.warning("Request to mark job run %s succeeded when it's running or already succeeded", self._act_run.id)

    def _cancel(self, request):
        if self._act_run.is_scheduled or self._act_run.is_queued:
            log.info("Cancelling job %s", self._act_run.id)
            self._act_run.cancel()
        else:
            log.warning("Request to cancel job run %s when it's not possible", self._act_run.id)

    def _fail(self, request):
        if not self._act_run.is_running and not self._act_run.is_success and not self._act_run.is_failed:
            log.info("Marking job run %s as failed", self._act_run.id)
            self._act_run.fail(0)
        else:
            log.warning("Request to fail job run %s when it's already running or done", self._act_run.id)

class JobRunResource(resource.Resource):
    isLeaf = False
    def __init__(self, run):
        self._run = run
        resource.Resource.__init__(self)

    def getChild(self, act_name, request):
        if act_name == '':
            return self
        
        for act_run in self._run.runs:
            if act_name == act_run.action.name:
                return ActionRunResource(act_run)

        return resource.NoResource("Cannot find action '%s' for job run '%s'" % (act_name, self._run.id)) 

    def render_GET(self, request):
        run_output = []
        state = job_run_state(self._run)
        
        for action_run in self._run.runs:
            action_state = job_run_state(action_run)
            
            last_time = action_run.end_time if action_run.end_time else timeutils.current_time()
            duration = str(last_time - action_run.start_time) if action_run.start_time else ""
           
            run_output.append({
                'id': action_run.id,
                'name': action_run.action.name,
                'run_time': action_run.run_time and str(action_run.run_time),
                'start_time': action_run.start_time and str(action_run.start_time),
                'end_time': action_run.end_time and str(action_run.end_time),
                'exit_status': action_run.exit_status,
                'duration': duration,
                'state': action_state,
            })

        output = {
            'runs': run_output, 
            'id': self._run.id, 
            'state': state,
            'node': self._run.node.hostname,
        }
        
        return respond(request, output)

    def render_POST(self, request):
        log.debug("Handling post request for run %s", self._run.id)
        cmd = request.args['command'][0]
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
        
        return respond(request, {'result': "Job run now in result %s" % job_run_state(self._run)})

    def _restart(self, request):
        log.info("Resetting all action runs to scheduled state")
        self._run.schedule()
        self._start(request)

    def _start(self, request):
        if not self._run.is_success and not self._run.is_running:
            log.info("Starting job run %s", self._run.id)
            self._run.start()
        else:
            log.warning("Request to start job run %s when it's already done", self._run.id)

    def _succeed(self, request):
        if not self._run.is_running and not self._run.is_success:
            log.info("Marking job run %s for success", self._run.id)
            self._run.succeed()
        else:
            log.warning("Request to mark job run %s succeed when it has already", self._run.id)

    def _cancel(self, request):
        if self._run.is_scheduled or self._run.is_queued:
            log.info("Cancelling job %s", self._run.id)
            self._run.cancel()
        else:
            log.warning("Request to cancel job run %s when it's already cancelled", self._run.id)

    def _fail(self, request):
        if not self._run.is_running and not self._run.is_success and not self._run.is_failed:
            log.info("Marking job run %s as failed", self._run.id)
            self._run.fail()
        else:
            log.warning("Request to fail job run %s when it's already running or done", self._run.id)

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
        
        if run_num.isdigit():
            run = self._job.get_run_by_num(int(run_num))
            if run:
                return JobRunResource(run)
        
        return resource.NoResource("Cannot run number '%s' for job '%s'" % (run_num, self._job.name))

    def get_run_data(self, request, run):
        state = job_run_state(run)
        last_time = run.end_time if run.end_time else timeutils.current_time()
        duration = str(last_time - run.start_time) if run.start_time else ""

        return {
                'id': run.id,
                'href': request.childLink(run.id),
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

        enable_run_output = []
        for e_run in self._job.enable_runs:
            enable_run_output.append(self.get_run_data(request, e_run))

        disable_run_output = []
        for d_run in self._job.disable_runs:
            disable_run_output.append(self.get_run_data(request, d_run))

        resources_output = []
        
        output = {
            'name': self._job.name,
            'scheduler': str(self._job.scheduler),
            'runs': run_output,
            'enable_runs': enable_run_output,
            'disable_runs': disable_run_output,
            'action_names': map(lambda t: t.name, self._job.topo_actions),
            'node_pool': map(lambda n: n.hostname, self._job.node_pool.nodes),
        }
        return respond(request, output)

    def render_POST(self, request):
        log.debug("Handling post request for %s", self._job.name)
        if request.args['command'][0] == 'enable':
            self._master_control.enable_job(self._job)
            return respond(request, {'result': "Job %s is enabled" % self._job.name})

        if request.args['command'][0] == 'disable':
            self._master_control.disable_job(self._job)
            return respond(request, {'result': "Job %s is disabled" % self._job.name})

        if request.args['command'][0] == 'start':
            run = self._job.manual_start()
            return respond(request, {'result': "New job %s created" % run.id})

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
        
    def render_GET(self, request):
        request.setHeader("content-type", "text/json")
        
        serv_list = []
        job_list = []
        for current_job in self._master_control.jobs.itervalues():
            last_success = str(current_job.last_success.end_time) if current_job.last_success else None
            
            # We need to describe the current state of this job
            is_service = current_job.enable_act or current_job.disable_act
            current_run = current_job.next_to_finish()
            status = "UNKNOWN"

            if current_run and current_run.is_running:
                status = "MONITORING" if is_service else "RUNNING"
            elif current_run and current_run.is_scheduled:
                status = "ENABLED"
            elif not current_run:
                status = "DISABLED"
                
            job_desc = {
                'name': current_job.name,
                'href': request.childLink(current_job.name),
                'status': status,
                'scheduler': str(current_job.scheduler),
                'last_success': last_success,
            }
            if is_service:
                serv_list.append(job_desc)
            else:
                job_list.append(job_desc)

        output = {
            'jobs': job_list,
            'services': serv_list,
        }
        return respond(request, output)
    
    def render_POST(self, request):
        log.debug("Handling post request on all jobs")
        if request.args['command'][0] == 'disableall':
            self._master_control.disable_all()
            return respond(request, {'result': "All jobs are now disabled"})
       
        if request.args['command'][0] == 'enableall':
            self._master_control.enable_all()
            return respond(request, {'result': "All jobs are now enabled"})

        log.warning("Unknown request command %s for all jobs", request.args['command'])
        return respond(request, None, code=http.NOT_IMPLEMENTED)


class ConfigResource(resource.Resource):
    """Resource for configuration changes"""
    isLeaf = True
    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)

    def render_GET(self, request):
        return respond(request, {'config':self._master_control.config_lines()})

    def render_POST(self, request):
        new_config = request.args['config'][0]
        self._master_control.rewrite_config(new_config)
        response = {'status': "I'm alive biatch"}
        try:
            self._master_control.live_reconfig()
        except Exception, e:
            response['error'] = str(e)
        
        return respond(request, response)
        

class RootResource(resource.Resource):
    def __init__(self, master_control):
        self._master_control = master_control
        resource.Resource.__init__(self)
        
        # Setup children
        self.putChild('jobs', JobsResource(master_control))
        self.putChild('config', ConfigResource(master_control))

    def getChild(self, name, request):
        if name == '':
            return self
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
    
    
