import logging

from tron import task
from tron.utils import timeutils

log = logging.getLogger('tron.job')

class JobRun(object):
    def __init__(self, job, prev=None):
        self.run_num = len(job.runs)
        self.id = "%s.%s" % (job.name, self.run_num)
        self.job = job
        self.prev = prev
        self.next = None
        
        self.run_time = None
        self.start_time = None
        self.end_time = None
        
        self.runs = []
        self.data = {'runs':[], 'run_time':None}

    def set_run_time(self, run_time):
        self.run_time = run_time
        self.data['run_time'] = run_time

        for r in self.runs:
            if not r.required_runs:
                r.run_time = run_time

    def scheduled_start(self):
        if self.should_start:
            self.start()
        elif self.job.queueing:
            log.warning("Previous job, %s, not finished - placing in queue", self.job.name)
            self.queue()
        else:
            log.warning("Previous job, %s, not finished - cancelling instance", self.job.name)
            self.cancel()

    def start(self):
        log.info("Starting task job %s", self.job.name)
        self.start_time = timeutils.current_time()
        [r.attempt_start() for r in self.runs]
        
    def run_completed(self):
        if self.is_success:
            self.end_time = timeutils.current_time()
            if self.next and self.next.is_queued:
                self.next.start()
            if self.job.constant:
                self.job.build_run(self).start()

    def queue(self):
        [r.queue() for r in self.runs]
        
    def cancel(self):
        [r.cancel() for r in self.runs]

    @property
    def is_failed(self):
        return any([r.is_failed for r in self.runs])

    @property
    def is_success(self):
        return all([r.is_success for r in self.runs])

    @property
    def is_queued(self):
        return all([r.is_queued for r in self.runs])
    
    @property
    def is_scheduled(self):
        return any([r.is_scheduled for r in self.runs])

    @property
    def is_unknown(self):
        return any([r.is_unknown for r in self.runs])

    @property
    def is_cancelled(self):
        return all([r.is_cancelled for r in self.runs])

    @property
    def should_start(self):
        prev_job = self.prev
        while prev_job and prev_job.is_cancelled:
            prev_job = prev_job.prev

        return not prev_job or prev_job.is_success

class Job(object):
    num_runs = 0
    def __init__(self, name, task=None):
        self.name = name
        self.topo_tasks = [task] if task else []
        self.scheduler = None
        self.queueing = False
        self.runs = []
        self.constant = False
        
        self.state_callback = None
        self.data = []
    
    def next_run(self):
        if not self.scheduler:
            return None
        return self.scheduler.next_run(self)

    def build_run(self, prev=None):
        job_run = JobRun(self, prev)
        if prev:
            prev.next = job_run

        runs = {}
        for t in self.topo_tasks:
            run = t.build_run()
            
            run.job_run = job_run
            runs[t.name] = run
            
            job_run.runs.append(run)
            job_run.data['runs'].append(run.data)

            for req in t.required_tasks:
                runs[req.name].waiting_runs.append(run)
                run.required_runs.append(runs[req.name])
      
        self.runs.append(job_run)
        self.data.append(job_run.data)
        return job_run

    def restore_run(self, data, prev=None):
        run = self.build_run(prev)
        for r, state in zip(run.runs, data['runs']):
            r.restore_state(state)
        
        run.set_run_time(data['run_time'])
        return run

