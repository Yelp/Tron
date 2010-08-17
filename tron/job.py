import logging
import os
import shutil
from collections import deque

from tron import action
from tron.utils import timeutils

log = logging.getLogger('tron.job')

RUN_LIMIT = 50

class JobRun(object):
    def __init__(self, job):
        self.run_num = job.next_num()
        self.job = job
        self.id = "%s.%s" % (job.name, self.run_num)
        self.output_dir = os.path.join(job.output_dir, self.id)
       
        self.run_time = None
        self.start_time = None
        self.end_time = None
        self.node = None
        self.runs = []
               
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

    def run_completed(self):
        if self.is_success:
            self.last_success_check()
            
            if self.job.constant and self.job.running:
                self.job.next_run().start()

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
        return self.job.running and not self.is_running and self.job.next_to_finish() == self

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

    def __init__(self, name=None, action=None):
        self.name = name
        self.topo_actions = [action] if action else []
        self.scheduler = None
        self.runs = deque()
        
        self.queueing = True
        self.running = True
        self.constant = False
        self.last_success = None
        
        self.run_limit = RUN_LIMIT
        self.node_pool = None
        self.output_dir = None

    def enable(self):
        self.running = True
        next = self.next_to_finish()
        if next and next.is_queued:
            next.start()
    
    def disable(self):
        self.running = False
        for r in self.runs:
            if r.is_scheduled or r.is_queued:
                r.cancel()
        
    def next_to_finish(self):
        """Returns the next run to finish. Useful for getting the currently 
        running job run or next queued/schedule job run.
        """
        def choose(prev, next):
            return next if not (prev and prev.is_running) and \
               (next.is_queued or next.is_scheduled or next.is_running) else prev

        return reduce(choose, self.runs, None)

    def get_run_by_num(self, num):
        for r in self.runs:
            if r.run_num == num:
                return r
        return None

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

    def next_run(self):
        if not self.scheduler:
            return None
        
        job_run = self.scheduler.next_run(self)
        if job_run:
            self.runs.appendleft(job_run)
            self.remove_old_runs()
        
            if os.path.exists(self.output_dir) and not os.path.exists(job_run.output_dir):
                os.mkdir(job_run.output_dir)
   
        return job_run

    def build_run(self):
        job_run = JobRun(self)
        if self.node_pool:
            job_run.node = self.node_pool.next() 
        
        #Build actions and setup requirements
        runs = {}
        for a in self.topo_actions:
            run = a.build_run(job_run)
            runs[a.name] = run
            
            job_run.runs.append(run)

            for req in a.required_actions:
                runs[req.name].waiting_runs.append(run)
                run.required_runs.append(runs[req.name])

        return job_run

    def manual_start(self):
        run = self.build_run()
        if self.runs[0].is_scheduled:
            top = self.runs.popleft()
            self.runs.appendleft(run)
            self.runs.appendleft(top)
        else:
            self.runs.appendleft(run)

        run.queue()

        if self.next_to_finish() == run:
            run.start()

        return run

    def absorb_old_job(self, old):
        self.runs = old.runs
        self.last_success = old.last_success
        self.run_num = old.run_num

    @property
    def data(self):
        return {'runs': [r.data for r in self.runs],
                'running': self.running
        }

    def restore_run(self, data):
        run = self.build_run()
        for r, state in zip(run.runs, data['runs']):
            r.restore_state(state)
            
        run.run_num = data['run_num']
        run.start_time = data['start_time']
        run.end_time = data['end_time']
        run.set_run_time(data['run_time'])
       
        self.runs.appendleft(run)

        if run.is_success:
            self.last_success = run

        return run

