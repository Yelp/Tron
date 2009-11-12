import datetime

from tron.utils import time

class ConstantScheduler(object):
    """The constant scheduler always schedules a run because the job should be running constantly"""
    def next_run(self, job):
        run = job.build_run()
        run.run_time = time.current_time()
        return run

class DailyScheduler(object):
    """The daily scheduler schedules one run per day"""
    def next_run(self, job):
        run = job.build_run()

        # For a daily scheduler, always assume the next job run is tomorrow
        run_time = (time.current_time() + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0)

        run.run_time = run_time
        return run
        
    
    