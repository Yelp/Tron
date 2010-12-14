import datetime

from testify import *
from testify.utils import turtle

from tron import resource
from tron.utils import timeutils

class BasicTestCase(TestCase):
    """Check that basic resource checking works"""
    @setup
    def build_job(self):
        self.job = turtle.Turtle()
        self.job.runs = []

        self.resource = resource.JobResource(self.job)

        # Freeze the current time to prevent race conditions
        timeutils.override_current_time(datetime.datetime.now())

    @teardown
    def restore_time(self):
        timeutils.override_current_time(None)

    def test_check_no_runs(self):
        assert not self.resource.is_ready

    def test_check_with_fail_run(self):
        job_run = turtle.Turtle()
        job_run.is_success = False
        self.job.runs.append(job_run)

        assert not self.resource.is_ready

    def test_check_with_success_run(self):
        job_run = turtle.Turtle()
        job_run.is_success = True
        self.job.runs.append(job_run)

        assert self.resource.is_ready


class CheckIntervalTestCase(TestCase):
    """Verify that check intervals are respected"""
    @setup
    def build_job(self):
        self.job = turtle.Turtle()
        self.job.runs = []

        start_time = datetime.datetime.now()
        timeutils.override_current_time(start_time)

        self.resource = resource.JobResource(self.job)
        self.resource.check_interval = datetime.timedelta(minutes=1)

    @teardown
    def restore_time(self):
        timeutils.override_current_time(None)

    def test_interval_check(self):
        job_run = turtle.Turtle()
        job_run.is_success = False
        self.job.runs.append(job_run)

        assert not self.resource.is_ready

        job_run.is_success = True

        # Should still fail since we don't need to check again
        assert not self.resource.is_ready

        # Now push time ahead so we'll re-check
        next_check = self.resource.next_check_time
        timeutils.override_current_time(next_check)
        assert self.resource.is_ready


class LastSuccessTestCase(TestCase):
    """Check that we respect last success settings"""
    @setup
    def build_job(self):
        self.job = turtle.Turtle()
        self.job.runs = []

        start_time = datetime.datetime.now()
        timeutils.override_current_time(start_time)

        self.resource = resource.JobResource(self.job)
        self.resource.last_succeed_interval = datetime.timedelta(minutes=1)

    def test_success_too_old(self):
        job_run = turtle.Turtle()
        job_run.is_success = True
        job_run.end_time = timeutils.current_time() - datetime.timedelta(days=1)
        self.job.runs.append(job_run)

        assert not self.resource.is_ready

    def test_success(self):
        job_run = turtle.Turtle()
        job_run.is_success = True
        job_run.end_time = timeutils.current_time() - datetime.timedelta(seconds=30)
        self.job.runs.append(job_run)

        assert self.resource.is_ready

    def test_success_but_recent_failure(self):
        job_run = turtle.Turtle()
        job_run.is_success = True
        job_run.end_time = timeutils.current_time() - datetime.timedelta(seconds=30)
        self.job.runs.append(job_run)

        bad_job_run = turtle.Turtle()
        bad_job_run.is_success = False
        bad_job_run.end_time = timeutils.current_time()
        self.job.runs.append(bad_job_run)

        assert self.resource.is_ready

if __name__ == '__main__':
    run()
