import datetime
import logging

from tron.utils import timeutils

log = logging.getLogger('tron.scheduler')

class ConstantScheduler(object):
    """The constant scheduler always schedules a run because the job should be running constantly"""
    def next_run(self, job):
        run = job.build_run()
        run.run_time = timeutils.current_time()
        return run

    def __str__(self):
        return "CONSTANT"


class DailyScheduler(object):
    """The daily scheduler schedules one run per day"""
    def next_run(self, job):
        run = job.build_run()

        # For a daily scheduler, always assume the next job run is tomorrow
        run_time = (timeutils.current_time() + datetime.timedelta(days=1)).replace(hour=0, minute=1, second=1)

        run.run_time = run_time
        return run

    def __str__(self):
        return "DAILY"


class IntervalScheduler(object):
    """The interval scheduler runs a job (to success) based on a configured interval
    """
    def __init__(self, interval=None):
        self.interval = interval
    
    def next_run(self, job):
        run = job.build_run()

        # Find the last success to pick the next time to run
        for past_run in reversed(job.runs):
            if past_run.is_success or past_run.is_running or past_run.is_waiting:
                run.run_time = past_run.run_time + self.interval
                break
        else:
            log.debug("Found no past runs for job %s, next run is now", run)
            run.run_time = timeutils.current_time()
        
        return run

    def __str__(self):
        return "INTERVAL:%s" % self.interval
        
    
