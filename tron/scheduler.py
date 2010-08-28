import datetime
import logging

from collections import deque
from tron.utils import timeutils

log = logging.getLogger('tron.scheduler')

WEEK = 'mtwrfsu'
CONVERT = {
    'mo': 'm', 'tu': 't', 'we': 'w', 'th': 'r', 'fr': 'f', 'sa': 's', 'su': 'u',
    'm' : 'm', 't' : 't', 'w' : 'w', 'r' : 'r', 'f' : 'f', 's' : 's', 'u' : 'u',
}

class ConstantScheduler(object):
    """The constant scheduler only schedules the first one.  The job run starts then next when finished"""
    def next_runs(self, job):
        if job.next_to_finish():
            return []
        
        job_runs = job.build_runs()
        for job_run in job_runs:
            job_run.set_run_time(timeutils.current_time())
        
        return job_runs

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
    def __init__(self, start_time=None, days=1):
        # What time of day does this thing start ? Default to 1 second after midnight
        self.start_time = start_time or datetime.time(hour=0, minute=0, second=1)
        self.wait_days = self.get_daily_waits(days)
        
    def get_daily_waits(self, days):
        """Computes how many days to wait till the next run from any day.
        e.g. If the next run is on Thursday and today is Monday.  Monday's entry is 3
        """
        if isinstance(days, int):
            return [days for i in range(7)]

        week = [False for i in range(7)]
        for day in days:
            week[WEEK.index(CONVERT[day[0:2].lower()])] = True

        count = week.index(True) + 1
        waits = deque()

        for val in reversed(week):
            waits.appendleft(count)
            count = 1 if val else count + 1
        return waits
    
    def next_runs(self, job):
        # Find the next time to run
        if job.runs:
            days = self.wait_days[job.runs[0].run_time.weekday()]
        else:
            days = self.wait_days[timeutils.current_time().weekday()]

        run_time = (timeutils.current_time() + datetime.timedelta(days=days)).replace(
                                                                            hour=self.start_time.hour, 
                                                                            minute=self.start_time.minute, 
                                                                            second=self.start_time.second)

        job_runs = job.build_runs()
        for job_run in job_runs:
            job_run.set_run_time(run_time)
 
        return job_runs
    
    def job_setup(self, job):
        job.queueing = True
    
    def __str__(self):
        return "DAILY"
    
    def __eq__(self, other):
        return isinstance(other, DailyScheduler) and \
           self.wait_days == other.wait_days and self.start_time == other.start_time

    def __ne__(self, other):
        return not self == other


class IntervalScheduler(object):
    """The interval scheduler runs a job (to success) based on a configured interval
    """
    def __init__(self, interval=None):
        self.interval = interval
    
    def next_runs(self, job):
        run_time = timeutils.current_time() + self.interval
        
        job_runs = job.build_runs()
        for job_run in job_runs:
            job_run.set_run_time(run_time)
        
        return job_runs

    def job_setup(self, job):
        job.queueing = False
    
    def __str__(self):
        return "INTERVAL:%s" % self.interval
        
    def __eq__(self, other):
        return isinstance(other, IntervalScheduler) and self.interval == other.interval
    
    def __ne__(self, other):
        return not self == other


