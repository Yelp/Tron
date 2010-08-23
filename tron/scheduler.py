import datetime
import logging

from tron.utils import timeutils

log = logging.getLogger('tron.scheduler')

class ConstantScheduler(object):
    """The constant scheduler only schedules the first one.  The job run starts then next when finished"""
    def next_run(self, job):
        if job.runs and (job.runs[0].is_running or job.runs[0].is_scheduled):
            return None
        
        job.constant = True
        job_run = job.build_run()
        job_run.set_run_time(timeutils.current_time())
        return job_run

    def job_setup(self, job):
        job.constant = True
        job.queueing = False

    def __str__(self):
        return "CONSTANT"

    def __eq__(self, other):
        return isinstance(other, ConstantScheduler)

    def __ne__(self, other):
        return not self == other

class DailyScheduler(object):
    """The daily scheduler schedules one run per day"""
    def __init__(self, start_time=None, days=1, week=None):
        # What time of day does this thing start ? Default to 1 second after midnight
        self.start_time = start_time or datetime.time(hour=0, minute=0, second=1)
        self.days = days
        self.week = week
        
    def next_run(self, job):
        # Find the next time to run
        if job.runs:
            days = self.week[job.runs[0].run_time.weekday()] if self.week else self.days
        else:
            days = self.week[timeutils.current_time().weekday()] if self.week else self.days
        
        run_time = (timeutils.current_time() + datetime.timedelta(seconds=20))
        
        #run_time = (timeutils.current_time() + datetime.timedelta(days=days)).replace(
        #                                                                    hour=self.start_time.hour, 
        #                                                                    minute=self.start_time.minute, 
        #                                                                    second=self.start_time.second)

        job_run = job.build_run()
        job_run.set_run_time(run_time)
        return job_run
    
    def job_setup(self, job):
        job.queueing = True
    
    def __str__(self):
        return "DAILY"
    
    def __eq__(self, other):
        return isinstance(other, DailyScheduler) and self.days == other.days and \
           self.week == other.week and self.start_time == other.start_time

    def __ne__(self, other):
        return not self == other


class IntervalScheduler(object):
    """The interval scheduler runs a job (to success) based on a configured interval
    """
    def __init__(self, interval=None):
        self.interval = interval
    
    def next_run(self, job):
        run_time = timeutils.current_time() + self.interval
        
        job_run = job.build_run()
        job_run.set_run_time(run_time)
        return job_run

    def job_setup(self, job):
        job.queueing = False
    
    def __str__(self):
        return "INTERVAL:%s" % self.interval
        
    def __eq__(self, other):
        return isinstance(other, IntervalScheduler) and self.interval == other.interval
    
    def __ne__(self, other):
        return not self == other


