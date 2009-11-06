import datetime

from tron import utils
import utils.time

class BaseScheduler(object):
    """Common base class for schedulers.
    
    Of course it isn't required to implement a scheduler, but provides the bare minimum functionality required.
    """
    def __init__(self):
        super(BaseScheduler, self).__init__()
        

    def next_run(self, job):
        run = job.build_run()
        run.start_time = datetime.datetime.now()
        return run

class ConstantScheduler(BaseScheduler):
    """The constant scheduler always schedules a run because the job should be running constantly"""
    pass

class DailyScheduler(BaseScheduler):
    """The daily scheduler schedules one run per day"""
    def next_run(self, job):
        run = super(DailyScheduler, self).next_run(job)

        # For a daily scheduler, always assume the next job run is tomorrow
        run_time = (utils.time.current_time() + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0)

        run.start_time = run_time

        return run
        
    
    