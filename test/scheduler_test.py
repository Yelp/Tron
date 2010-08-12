import datetime

from testify import *
from testify.utils import turtle

from tron import scheduler, action, job

class ConstantSchedulerTest(TestCase):
    @setup
    def build_scheduler(self):
        self.scheduler = scheduler.ConstantScheduler()
        self.action = action.Action("Test Action")
        self.action.command = "Test Command"
        self.job = job.Job("Test Job", self.action)
        self.job.scheduler = self.scheduler
        self.action.job = self.job
    
    def test_next_run(self):
        next_run = self.job.next_run()
        assert_gte(datetime.datetime.now(), next_run.run_time)
   
        next_run2 = self.scheduler.next_run(self.job)
        assert_equal(next_run2, None)

    def test__str__(self):
        assert_equal(str(self.scheduler), "CONSTANT")

class DailySchedulerTest(TestCase):
    @setup
    def build_scheduler(self):
        self.scheduler = scheduler.DailyScheduler()
        self.action = action.Action("Test Action")
        self.job = job.Job("Test Job", self.action)
        self.job.scheduler = self.scheduler
        self.action.job = self.job

    def test_next_run(self):
        next_run = self.scheduler.next_run(self.job)

        next_run_date = next_run.run_time.date()
        today = datetime.date.today()
        
        assert_gt(next_run_date, today)
        assert_equal(next_run_date - today, datetime.timedelta(days=1))

    def test__str__(self):
        assert_equal(str(self.scheduler), "DAILY")


class DailySchedulerTimeTest(TestCase):
    @setup
    def build_scheduler(self):
        self.scheduler = scheduler.DailyScheduler(start_time=datetime.time(hour=16, minute=30))
        self.action = action.Action("Test Action - Beer Time")
        self.job = job.Job("Test Job", self.action)
        self.job.scheduler = self.scheduler
        self.action.job = self.job
    
    def test_next_run(self):
        next_run = self.scheduler.next_run(self.job)
        next_run_date = next_run.run_time.date()

        today = datetime.date.today()
        tomorrow = today + datetime.timedelta(days=1)
        
        assert_gt(next_run_date, today)
        assert_equal(next_run_date - today, datetime.timedelta(days=1))
        assert_lte(datetime.datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=12), next_run.run_time)


class IntervalSchedulerTest(TestCase):
    @setup
    def build_scheduler(self):
        self.interval = datetime.timedelta(seconds=1)
        self.scheduler = scheduler.IntervalScheduler(self.interval)
        self.action = action.Action("Test Action")
        self.job = job.Job("Test Job", self.action)
        self.job.scheduler = self.scheduler
        self.action.job = self.job

    def test_next_run(self):
        next_run = self.scheduler.next_run(self.job)
        assert_gte(datetime.datetime.now() + self.interval, next_run.run_time)
        
        next_run2 = self.scheduler.next_run(self.job)
        assert_equal(next_run2.run_time - next_run.run_time, self.interval)

    def test__str__(self):
        assert_equal(str(self.scheduler), "INTERVAL:%s" % self.interval)

if __name__ == '__main__':
    run()
