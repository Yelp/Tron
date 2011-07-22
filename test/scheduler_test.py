import calendar
import datetime
import tempfile
import shutil
import pytz

from testify import *
from testify.utils import turtle

from tron import scheduler, action, job, groctimespecification
from tron.utils import timeutils

class ConstantSchedulerTest(TestCase):
    @setup
    def build_scheduler(self):
        self.test_dir = tempfile.mkdtemp()
        self.scheduler = scheduler.ConstantScheduler()
        self.action = action.Action("Test Action")
        self.action.command = "Test Command"
        self.job = job.Job("Test Job", self.action)
        self.job.node_pool = turtle.Turtle()
        self.job.output_path = self.test_dir
        self.job.scheduler = self.scheduler
        self.action.job = self.job

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)
    
    def test_next_runs(self):
        next_run = self.job.next_runs()[0]
        assert_gte(datetime.datetime.now(), next_run.run_time)
   
        assert_equal(self.scheduler.next_runs(self.job), [])

    def test__str__(self):
        assert_equal(str(self.scheduler), "CONSTANT")


class DailySchedulerTest(TestCase):
    @setup
    def build_scheduler(self):
        self.test_dir = tempfile.mkdtemp()
        self.scheduler = scheduler.DailyScheduler()
        self.action = action.Action("Test Action")
        self.job = job.Job("Test Job", self.action)
        self.job.node_pool = turtle.Turtle()
        self.job.output_path = self.test_dir
        self.job.scheduler = self.scheduler
        self.action.job = self.job

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)
 
    def test_next_runs(self):
        next_run = self.scheduler.next_runs(self.job)[0]

        next_run_date = next_run.run_time.date()
        today = datetime.date.today()
        
        assert_gt(next_run_date, today)
        assert_equal(next_run_date - today, datetime.timedelta(days=1))

    def test__str__(self):
        assert_equal(str(self.scheduler), "DAILY")


class DailySchedulerTimeTest(TestCase):
    @setup
    def build_scheduler(self):
        self.test_dir = tempfile.mkdtemp()
        self.scheduler = scheduler.DailyScheduler(start_time=datetime.time(hour=16, minute=30))
        self.action = action.Action("Test Action - Beer Time")
        self.job = job.Job("Test Job", self.action)
        self.job.node_pool = turtle.Turtle()
        self.job.output_path = self.test_dir
        self.job.scheduler = self.scheduler
        self.action.job = self.job
    
    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)
 
    def test_next_runs(self):
        next_run = self.scheduler.next_runs(self.job)[0]
        next_run_date = next_run.run_time.date()

        today = datetime.date.today()
        tomorrow = today + datetime.timedelta(days=1)
        
        assert_gt(next_run_date, today)
        assert_equal(next_run_date - today, datetime.timedelta(days=1))
        assert_lte(datetime.datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=12), next_run.run_time)


class GrocSchedulerTest(TestCase):
    @setup
    def build_scheduler(self):
        self.test_dir = tempfile.mkdtemp()
        self.scheduler = scheduler.GrocScheduler()
        self.action = action.Action("Test Action")
        self.job = job.Job("Test Job", self.action)
        self.job.node_pool = turtle.Turtle()
        self.job.output_path = self.test_dir
        self.job.scheduler = self.scheduler
        self.action.job = self.job
        self.today = datetime.datetime(2011, 6, 1, tzinfo=pytz.utc)

    @teardown
    def remove_tmp(self):
        shutil.rmtree(self.test_dir)

    @setup
    def set_time(self):
        timeutils.override_current_time(self.today)

    @teardown
    def unset_time(self):
        timeutils.override_current_time(None)

    def test_parse_all(self):
        self.scheduler.parse('1st,2nd,3rd,4th monday,Tue of march,apr,September at 00:00')
        assert_equal(self.scheduler.ordinals, set((1, 2, 3, 4)))
        assert_equal(self.scheduler.monthdays, None)
        assert_equal(self.scheduler.weekdays, set((0, 1)))
        assert_equal(self.scheduler.months, set((3, 4, 9)))
        assert_equal(self.scheduler.timestr, '00:00')

    def test_parse_no_weekday(self):
        self.scheduler.parse('1st,2nd,3rd,10th day of march,apr,September at 00:00')
        assert_equal(self.scheduler.ordinals, None)
        assert_equal(self.scheduler.monthdays, set((1,2,3,10)))
        assert_equal(self.scheduler.weekdays, None)
        assert_equal(self.scheduler.months, set((3, 4, 9)))
        assert_equal(self.scheduler.timestr, '00:00')

    def test_parse_no_month(self):
        self.scheduler.parse('1st,2nd,3rd,10th day at 00:00')
        assert_equal(self.scheduler.ordinals, None)
        assert_equal(self.scheduler.monthdays, set((1,2,3,10)))
        assert_equal(self.scheduler.weekdays, None)
        assert_equal(self.scheduler.months, None)
        assert_equal(self.scheduler.timestr, '00:00')

    def test_parse_monthly(self):
        for test_str in ('1st day', '1st day of month'):
            self.scheduler.parse(test_str)
            assert_equal(self.scheduler.ordinals, None)
            assert_equal(self.scheduler.monthdays, set([1]))
            assert_equal(self.scheduler.weekdays, None)
            assert_equal(self.scheduler.months, None)
            assert_equal(self.scheduler.timestr, '00:00')

    def test_wildcards(self):
        self.scheduler.parse('every day')
        assert_equal(self.scheduler.ordinals, None)
        assert_equal(self.scheduler.monthdays, None)
        assert_equal(self.scheduler.weekdays, None)
        assert_equal(self.scheduler.months, None)
        assert_equal(self.scheduler.timestr, '00:00')

    def test_daily(self):
        self.scheduler.parse('every day')
        next_run = self.scheduler.next_runs(self.job)[0]

        next_run_date = next_run.run_time

        assert_gt(next_run_date, self.today)
        assert_equal(next_run_date.month, 6)
        assert_equal(next_run_date.day, 2)
        assert_equal(next_run_date.hour, 0)

    def test_daily_with_time(self):
        self.scheduler.parse('every day at 02:00')
        next_run = self.scheduler.next_runs(self.job)[0]

        next_run_date = next_run.run_time

        assert_gt(next_run_date, self.today)
        assert_equal(next_run_date.year, self.today.year)
        assert_equal(next_run_date.month, 6)
        assert_equal(next_run_date.day, 1)
        assert_equal(next_run_date.hour, 2)
        assert_equal(next_run_date.minute, 0)

    def test_weekly(self):
        self.scheduler.parse('every monday at 01:00')

        next_run = self.scheduler.next_runs(self.job)[0]

        next_run_date = next_run.run_time

        assert_gt(next_run_date, self.today)
        assert_equal(calendar.weekday(next_run_date.year, next_run_date.month, next_run_date.day), 6)

    def test_weekly_in_month(self):
        self.scheduler.parse('every monday in january at 00:01')

        next_run = self.scheduler.next_runs(self.job)[0]

        next_run_date = next_run.run_time

        assert_gt(next_run_date, self.today)
        assert_equal(next_run_date.year, self.today.year+1)
        assert_equal(next_run_date.month, 1)
        assert_equal(next_run_date.hour, 0)
        assert_equal(next_run_date.minute, 1)

    def test_monthly(self):
        self.scheduler.parse('1st day')

        next_run = self.scheduler.next_runs(self.job)[0]

        next_run_date = next_run.run_time

        assert_gt(next_run_date, self.today)
        assert_equal(next_run_date.month, 7)


class IntervalSchedulerTest(TestCase):
    @setup
    def build_scheduler(self):
        self.test_dir = tempfile.mkdtemp()
        self.interval = datetime.timedelta(seconds=1)
        self.scheduler = scheduler.IntervalScheduler(self.interval)
        self.action = action.Action("Test Action")
        self.job = job.Job("Test Job", self.action)
        self.job.node_pool = turtle.Turtle()
        self.job.output_path = self.test_dir
        self.job.scheduler = self.scheduler
        self.action.job = self.job

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)
 
    def test_next_runs(self):
        next_run = self.scheduler.next_runs(self.job)[0]
        assert_gte(datetime.datetime.now() + self.interval, next_run.run_time)
       
    def test__str__(self):
        assert_equal(str(self.scheduler), "INTERVAL:%s" % self.interval)

if __name__ == '__main__':
    run()
