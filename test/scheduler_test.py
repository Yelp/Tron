import datetime

from testify import *
from testify.utils import turtle

from tron import scheduler

class ConstantSchedulerTest(TestCase):
    @setup
    def build_scheduler(self):
        self.scheduler = scheduler.ConstantScheduler()
        self.job = turtle.Turtle()
    
    def test_next_run(self):
        next_run = self.scheduler.next_run(self.job)
        assert_lte(datetime.datetime.now() - next_run.start_time, datetime.timedelta(seconds=2))

class DailySchedulerTest(TestCase):
    @setup
    def build_scheduler(self):
        self.scheduler = scheduler.DailyScheduler()
        self.job = turtle.Turtle()
        def build_run():
            return turtle.Turtle()
        self.job.build_run = build_run
    
    def test_next_run(self):
        next_run = self.scheduler.next_run(self.job)

        next_run_date = next_run.start_time.date()
        today = datetime.date.today()
        
        assert_gt(next_run_date, today)
        assert_equal(next_run_date - today, datetime.timedelta(days=1))

if __name__ == '__main__':
    run()