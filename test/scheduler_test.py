import datetime

from testify import *
from testify.utils import turtle

from tron import scheduler, job

class ConstantSchedulerTest(TestCase):
    @setup
    def build_scheduler(self):
        self.scheduler = scheduler.ConstantScheduler()
        self.job = job.Job("Test Job")
        self.job.scheduler = self.scheduler
    
    def test_next_run(self):
        next_run = self.scheduler.next_run(self.job)
        assert_gte(datetime.datetime.now(), next_run.run_time)
   
        self.job.runs.append(next_run)
        next_run2 = self.scheduler.next_run(self.job)
        assert_equal(next_run2, None)

    def test_set_job_queueing(self):
        self.scheduler.set_job_queueing(self.job)
        assert_equal(len(self.job.dependants), 1)
        assert_equal(self.job.dependants[0], self.job)

    def test__str__(self):
        assert_equal(str(self.scheduler), "CONSTANT")

class DailySchedulerTest(TestCase):
    @setup
    def build_scheduler(self):
        self.scheduler = scheduler.DailyScheduler()
        self.job = job.Job("Test Job")
        self.job.scheduler = self.scheduler

    def test_next_run(self):
        next_run = self.scheduler.next_run(self.job)

        next_run_date = next_run.run_time.date()
        today = datetime.date.today()
        
        assert_gt(next_run_date, today)
        assert_equal(next_run_date - today, datetime.timedelta(days=1))

    def test_set_job_queueing(self):
        self.scheduler.set_job_queueing(self.job)
        assert self.job.queueing

    def test__str__(self):
        assert_equal(str(self.scheduler), "DAILY")

class IntervalSchedulerTest(TestCase):
    @setup
    def build_scheduler(self):
        self.interval = datetime.timedelta(seconds=1)
        self.scheduler = scheduler.IntervalScheduler(self.interval)
        self.job = job.Job("Test Job")
        self.job.scheduler = self.scheduler

    def test_next_run(self):
        next_run = self.scheduler.next_run(self.job)
        assert_gte(datetime.datetime.now(), next_run.run_time)
        
        self.job.runs.append(next_run)
        next_run2 = self.scheduler.next_run(self.job)
        assert_equal(next_run2.run_time - next_run.run_time, self.interval)

    def test_set_job_queueing(self):
        self.scheduler.set_job_queueing(self.job)
        assert not self.job.queueing

    def test__str__(self):
        assert_equal(str(self.scheduler), "INTERVAL:%s" % self.interval)

if __name__ == '__main__':
    run()
