import datetime
import logging

from tron.utils import timeutils

log = logging.getLogger('tron.scheduler')

class ConstantScheduler(object):
    """The constant scheduler only schedules the first one.  The job run starts then next when finished"""
    def next_run(self, job):
        if job.runs and (job.runs[0].is_running or job.runs[0].is_scheduled):
            return
        
        job.constant = True
        job_run = job.build_run()
        job_run.set_run_time(timeutils.current_time())
        return job_run

    def __str__(self):
        return "CONSTANT"

class DailyScheduler(object):
    """The daily scheduler schedules one run per day"""
    def __init__(self, start_time=None):
        # What time of day does this thing start ? Default to 1 second after midnight
        self.start_time = start_time or datetime.time(hour=0, minute=0, second=1)
        
    def next_run(self, job):
        # For a daily scheduler, always assume the next job run is tomorrow
        run_time = (timeutils.current_time() + datetime.timedelta(days=1)).replace(
                                                                            hour=self.start_time.hour, 
                                                                            minute=self.start_time.minute, 
                                                                            second=self.start_time.second)

        job_run = job.build_run()
        job_run.set_run_time(run_time)
        return job_run

    def __str__(self):
        return "DAILY"

class IntervalScheduler(object):
    """The interval scheduler runs a job (to success) based on a configured interval
    """
    def __init__(self, interval=None):
        self.interval = interval
    
    def next_run(self, job):
        # Find the last success to pick the next time to run
        if job.runs and job.running:
            run_time = job.runs[0].run_time + self.interval
        else:
            log.debug("Found no past runs for job %s, next run is now", job.name)
            run_time = timeutils.current_time() + self.interval
        
        job_run = job.build_run()
        job_run.set_run_time(run_time)
        return job_run

    def __str__(self):
        return "INTERVAL:%s" % self.interval
        
