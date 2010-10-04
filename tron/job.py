import logging
import os
import shutil
from collections import deque

from tron import action, command_context
from tron.utils import timeutils

log = logging.getLogger('tron.job')

RUN_LIMIT = 50

class JobRun(object):
    def __init__(self, job, run_num=None):
        self.run_num = job.next_num() if run_num is None else run_num
        self.job = job
        self.state_callback = job.state_callback
        self.id = "%s.%s" % (job.name, self.run_num)
        self.output_dir = os.path.join(job.output_dir, self.id)
       
        self.run_time = None
        self.start_time = None
        self.end_time = None
        self.node = None
        self.runs = []
        self.context = command_context.CommandContext(self, job.context)
               
    def set_run_time(self, run_time):
        self.run_time = run_time

        for r in self.runs:
            if not r.required_runs:
                r.run_time = run_time

    def scheduled_start(self):
        self.attempt_start()

        if self.is_scheduled:
            if self.job.queueing:
                log.warning("A previous run for %s has not finished - placing in queue", self.job.name)
                self.queue()
            else:
                log.warning("A previous run for %s has not finished - cancelling", self.job.name)
                self.cancel()

    def start(self):
        log.info("Starting action job %s", self.job.name)
        self.start_time = timeutils.current_time()
        self.end_time = None

        for r in self.runs:
            r.attempt_start()
  
    def manual_start(self):
        self.queue()
        self.attempt_start()

    def attempt_start(self):
        if self.should_start:
            self.start()

    def last_success_check(self):
        if not self.job.last_success or self.run_num > self.job.last_success.run_num:
            self.job.last_success = self

    def create_action_run(self, act):
        act_run = act.build_run(self.context)

        act_run.id = "%s.%s" % (self.id, act.name)
        act_run.state_callback = self.state_callback        
        act_run.complete_callback = self.run_completed

        act_run.node = act_run.node_pool.next() if act_run.node_pool else self.node
        act_run.stdout_path = os.path.join(self.output_dir, act_run.name + '.stdout')
        act_run.stderr_path = os.path.join(self.output_dir, act_run.name + '.stderr')

        return act_run

    def run_completed(self):
        if self.is_success:
            self.last_success_check()
            
            if self.job.constant and self.job.enabled:
                self.job.build_run().start()

        if self.is_done:
            self.end_time = timeutils.current_time()
            
            next = self.job.next_to_finish()
            if next and next.is_queued:
                next.attempt_start()

    @property
    def data(self):
        return {'runs':[r.data for r in self.runs],
                'run_num': self.run_num,
                'run_time': self.run_time,
                'start_time': self.start_time,
                'end_time': self.end_time
        }

    def schedule(self):
        for r in self.runs:
            r.schedule()

    def queue(self):
        for r in self.runs:
            r.queue()
        
    def cancel(self):
        for r in self.runs:
            r.cancel()
    
    def succeed(self):
        for r in self.runs:
            r.mark_success()

    def fail(self):
        for r in self.runs:
            r.fail(0)

    @property
    def should_start(self):
        if not self.job.enabled or self.is_running:
            return False
        return self.job.next_to_finish(self.node if self.job.all_nodes else None) == self

    @property
    def is_failed(self):
        return any([r.is_failed for r in self.runs])

    @property
    def is_success(self):
        return all([r.is_success for r in self.runs])

    @property
    def is_done(self):
        return not any([r.is_running or r.is_queued or r.is_scheduled for r in self.runs])

    @property
    def is_queued(self):
        return all([r.is_queued for r in self.runs])
   
    @property
    def is_running(self):
        return any([r.is_running for r in self.runs])

    @property
    def is_scheduled(self):
        return any([r.is_scheduled for r in self.runs])

    @property
    def is_unknown(self):
        return any([r.is_unknown for r in self.runs])

    @property
    def is_cancelled(self):
        return all([r.is_cancelled for r in self.runs])

class Job(object):
    run_num = 0
    def next_num(self):
        self.run_num += 1
        return self.run_num - 1

    def __init__(self, name=None, action=None, context=None):
        self.name = name
        self.topo_actions = [action] if action else []
        self.scheduler = None
        self.runs = deque()
        
        self.queueing = True
        self.all_nodes = False
        self.enabled = True
        self.constant = False
        self.last_success = None
        
        self.run_limit = RUN_LIMIT
        self.node_pool = None
        self.output_dir = None
        self.state_callback = lambda:None
        self.context = command_context.CommandContext(self)

    def _register_action(self, action):
        """Prepare an action to be *owned* by this job"""
        if action in self.topo_actions:
            raise Error("Action %s already in jobs %s" % (action.name, job.name))

    def add_action(self, action):
        self._register_action(action)
        self.topo_actions.append(action)

    def __eq__(self, other):
        if not isinstance(other, Job) or self.name != other.name or self.queueing != other.queueing \
           or self.scheduler != other.scheduler or self.node_pool != other.node_pool \
           or self.all_nodes != other.all_nodes or len(self.topo_actions) != len(other.topo_actions) \
           or self.run_limit != other.run_limit:
            return False

        return all([me == you for (me, you) in zip(self.topo_actions, other.topo_actions)])

    def __ne__(self, other):
        return not self == other

    def set_context(self, context):
        self.context = command_context.CommandContext(self.context, context)

    def enable(self):
        self.enabled = True
        next = self.next_to_finish()

        if next and next.is_queued:
            next.start()
    
    def disable(self):
        self.enabled = False

        for r in self.runs:
            if r.is_scheduled or r.is_queued:
                r.cancel()
    
    def next_to_finish(self, node=None):
        """Returns the next run to finish(optional node requirement). Useful for 
        getting the currently running job run or next queued/schedule job run.
        """
        def choose(prev, next):
            return prev if (prev and prev.is_running) or (node and next.node != node) \
               or next.is_success or next.is_failed or next.is_cancelled or next.is_unknown else next

        return reduce(choose, self.runs, None)

    def get_run_by_num(self, num):
        def choose(chosen, next):
            return next if next.run_num == num else chosen

        return reduce(choose, self.runs, None)

    def remove_old_runs(self):
        """Remove old runs so the number left matches the run limit.
        However only removes runs up to the last success or up to the next to run
        """
        next = self.next_to_finish()
        next_num = next.run_num if next else self.runs[0].run_num
        succ_num = self.last_success.run_num if self.last_success else 0
        keep_num = min([next_num, succ_num])

        while len(self.runs) > self.run_limit and keep_num > self.runs[-1].run_num:
            old = self.runs.pop()
            if os.path.exists(old.output_dir):
                shutil.rmtree(old.output_dir)

    def next_runs(self):
        if not self.scheduler:
            return []
        
        return self.scheduler.next_runs(self)

    def build_action_dag(self, job_run, actions):
        """Build actions and setup requirements"""
        runs = {}
        for a in actions:
            run = job_run.create_action_run(a)
            runs[a.name] = run
            
            job_run.runs.append(run)

            for req in a.required_actions:
                runs[req.name].waiting_runs.append(run)
                run.required_runs.append(runs[req.name])
        
    def build_run(self, node=None, actions=None, run_num=None):
        job_run = JobRun(self, run_num=run_num)
        job_run.node = node or self.node_pool.next() 
 
        if os.path.exists(self.output_dir) and not os.path.exists(job_run.output_dir):
            os.mkdir(job_run.output_dir)

        # If the actions aren't specified, then we know this is a normal run
        if not actions:
            self.runs.appendleft(job_run)
            actions = self.topo_actions
            self.remove_old_runs()

        self.build_action_dag(job_run, actions)
        return job_run

    def build_runs(self):
        if self.all_nodes:
            return [self.build_run(node=node) for node in self.node_pool.nodes]
        return [self.build_run()]

    def manual_start(self):
        scheduled = deque()
        while self.runs and self.runs[0].is_scheduled:
            scheduled.appendleft(self.runs.popleft())
        
        man_runs = self.build_runs()
        self.runs.extendleft(scheduled)

        for r in man_runs:
            r.queue()
            r.attempt_start()

        return man_runs

    def absorb_old_job(self, old):
        self.runs = old.runs

        self.output_dir = old.output_dir
        self.last_success = old.last_success
        self.run_num = old.run_num
        self.enabled = old.enabled

    @property
    def data(self):
        return {'runs': [r.data for r in self.runs],
                'enabled': self.enabled
        }

    def restore_main_run(self, data):
        run = self.restore_run(data, self.topo_actions)
        self.runs.append(run)
        if run.is_success and not self.last_success:
            self.last_success = run
        return run

    def restore_run(self, data, actions):
        run = self.build_run(run_num=data['run_num'], actions=actions)
        self.run_num = max([run.run_num + 1, self.run_num])

        for r, state in zip(run.runs, data['runs']):
            r.restore_state(state)
            
        run.start_time = data['start_time']
        run.end_time = data['end_time']
        run.set_run_time(data['run_time'])

        return run

