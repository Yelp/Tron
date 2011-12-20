""" Resources - Things that must be available for a job run to be executed

Resources are how we managed our dependencies before starting a job.  The only
required interface for a resource is that it has a 'is_ready' property.
"""

from tron.utils import timeutils


class JobResource(object):
    """Resource that indicates a job must have previously run

    TODO: This first resource has support for interval checking, which is goign
    to be useful for other resources, but maybe not so much for jobs.
    """

    def __init__(self, job, last_succeed_interval=None):
        self.job = job
        self.last_succeed_interval = last_succeed_interval

        self.last_check = None
        self.check_interval = None
        self._is_ready = False

    def _check_job_runs(self):
        if timeutils.current_time() < self.next_check_time:
            return

        min_success_time = None
        if self.last_succeed_interval:
            min_success_time = (timeutils.current_time() -
                                self.last_succeed_interval)

        for run in reversed(self.job.runs):
            if (run.is_success and
                (min_success_time is None or
                 run.end_time >= min_success_time)):
                self._is_ready = True
                break
        else:
            self._is_ready = False

        self.last_check = timeutils.current_time()

    @property
    def next_check_time(self):
        if self.last_check is None:
            return timeutils.current_time()
        else:
            return self.last_check + self.check_interval

    @property
    def is_ready(self):
        """Property indicating the resources is ready and available"""
        self._check_job_runs()
        return (self._is_ready and
                (self.check_interval is None or
                 self.last_check >
                    timeutils.current_time() - self.check_interval))
