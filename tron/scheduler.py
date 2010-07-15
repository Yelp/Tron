import datetime
import logging

from tron.utils import timeutils

log = logging.getLogger('tron.scheduler')

class ConstantScheduler(object):
    """The constant scheduler only schedules the first one but sets itself as a dependant so always runs"""
    def next_run(self, flow):
        if flow.prev:
            return None
        flow.run_time = timeutils.current_time()
        return flow

    def __str__(self):
        return "CONSTANT"

    def set_flow_queueing(self, flow):
        pass

class DailyScheduler(object):
    """The daily scheduler schedules one run per day"""
    def next_run(self, flow):
        # For a daily scheduler, always assume the next job run is tomorrow
        run_time = (timeutils.current_time() + datetime.timedelta(days=1)).replace(hour=0, minute=1, second=1)

        flow.run_time = run_time
        return flow

    def __str__(self):
        return "DAILY"

    def set_flow_queueing(self, flow):
        flow.queueing = True

class IntervalScheduler(object):
    """The interval scheduler runs a flow (to success) based on a configured interval
    """
    def __init__(self, interval=None):
        self.interval = interval
    
    def next_run(self, flow):
        # Find the last success to pick the next time to run
        if flow.prev:
            run_time = flow.prev.run_time + self.interval
        else:
            log.debug("Found no past runs for flow %s, next run is now", flow.flow.name)
            run_time = timeutils.current_time()
        
        flow.run_time = run_time
        return flow

    def __str__(self):
        return "INTERVAL:%s" % self.interval
        
    def set_flow_queueing(self, flow):
        flow.queueing = False

