import logging

from tron import job

log = logging.getLogger('tron.job_flow')

class JobFlowRun(object):
    def __init__(self, flow, prev=None):
        self.flow = flow
        self.runs = []
        self.prev = prev
        self.next = None
        self.run_time = None

    def scheduled_start(self):
        if self.should_start:
            self.start()
        elif self.flow.queueing:
            log.warning("Previous job flow, %s, not finished - placing in queue", self.flow.name)
            self.queue()
        else:
            log.warning("Previous job flow, %s, not finished - cancelling instance", self.flow.name)
            self.cancel()

    def start(self):
        log.info("Starting job flow %s", self.flow.name)
        for r in self.runs:
            if not r.required_runs:
                r.start()
        
    def run_completed(self):
        if self.is_success:
            if self.next and self.next.is_queued:
                self.next.start()
            if self.flow.constant:
                self.flow.build_run(self).start()

    def queue(self):
        for r in self.runs:
            r.state = job.JOB_RUN_QUEUED 
        
    def cancel(self):
        [r.cancel() for r in self.runs]

    @property
    def is_success(self):
        return all([r.is_success for r in self.runs])

    @property
    def is_queued(self):
        return all([r.is_queued for r in self.runs])

    @property
    def is_cancelled(self):
        return any([r.is_cancelled for r in self.runs])

    @property
    def should_start(self):
        prev_flow = self.prev
        while prev_flow and prev_flow.is_cancelled:
            prev_flow = prev_flow.prev

        return not prev_flow or prev_flow.is_success

class JobFlow(object):
    def __init__(self, name, job=None):
        self.name = name
        self.topo_jobs = [job] if job else []
        self.scheduler = None
        self.queueing = False
        self.last = None
        self.constant = False

    def next_run(self):
        if not self.scheduler:
            return None
        return self.scheduler.next_run(self)

    def build_run(self, prev=None):
        flow_run = JobFlowRun(self, prev)
        if prev:
            prev.next = flow_run

        runs = {}
        for j in self.topo_jobs:
            run = j.build_run()
            
            if j.required_jobs:
                run.state = job.JOB_RUN_QUEUED

            run.flow_run = flow_run
            flow_run.runs.append(run)
            runs[j.name] = run

            for req in j.required_jobs:
                runs[req.name].waiting_runs.append(run)
                run.required_runs.append(runs[req.name])
      
        self.last = flow_run
        return flow_run

