import calendar
import datetime
import tempfile
import shutil

import pytz
from testify import *
from testify.utils import turtle

from tron import scheduler, action, job
from tron.utils import groctimespecification, timeutils

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


class DailySchedulerTimeTestBase(TestCase):
    @setup
    def build_scheduler(self):
        self.scheduler = scheduler.DailyScheduler(start_time=datetime.time(hour=14, minute=30))

    @setup
    def build_job(self):
        self.test_dir = tempfile.mkdtemp()
        self.action = action.Action("Test Action - Beer Time")
        self.job = job.Job("Test Job", self.action)
        self.job.node_pool = turtle.Turtle()
        self.job.output_path = self.test_dir
        self.job.scheduler = self.scheduler
        self.action.job = self.job

    @teardown
    def unset_time(self):
        timeutils.override_current_time(None)

    @teardown
    def cleanup(self):
        shutil.rmtree(self.test_dir)


class DailySchedulerTodayTest(DailySchedulerTimeTestBase):
    @setup
    def set_time(self):
        self.now = datetime.datetime.now().replace(hour=12, minute=0)
        timeutils.override_current_time(self.now)

    def test(self):
        # If we schedule a job for later today, it shoudl run today
        next_run = self.scheduler.next_runs(self.job)[0]
        next_run_date = next_run.run_time.date()

        assert_equal(next_run_date, self.now.date())
        assert_lte(datetime.datetime(year=self.now.year, month=self.now.month,
                                     day=self.now.day, hour=13),
                   next_run.run_time)


class DailySchedulerTomorrowTest(DailySchedulerTimeTestBase):

    @setup
    def set_time(self):
        self.now = datetime.datetime.now().replace(hour=15, minute=0)
        timeutils.override_current_time(self.now)

    def test(self):
        # If we schedule a job for later today, it shoudl run today
        next_run = self.scheduler.next_runs(self.job)[0]
        next_run_date = next_run.run_time.date()
        tomorrow = self.now.date() + datetime.timedelta(days=1)

        assert_equal(next_run_date, tomorrow)
        assert_lte(datetime.datetime(year=tomorrow.year, month=tomorrow.month,
                                     day=tomorrow.day, hour=13),
                   next_run.run_time)


class DailySchedulerDSTTest(TestCase):

    @setup
    def setup_tmp(self):
        self.tmp_dirs = []

    @teardown
    def unset_time(self):
        timeutils.override_current_time(None)

    @teardown
    def cleanup(self):
        for tmp_dir in self.tmp_dirs:
            shutil.rmtree(tmp_dir)

    def make_job(self, sch):
        tmp_dir = tempfile.mkdtemp()
        self.tmp_dirs.append(tmp_dir)
        a = action.Action("Test Action - Early Christmas Shopping")
        j = job.Job("Test Job", a)
        j.node_pool = turtle.Turtle()
        j.output_path = tmp_dir
        j.scheduler = sch
        a.job = j
        return j

    def test(self):
        """This test checks the behavior of the scheduler at the daylight
        savings time 'fall back' point, when the system clock sets itself
        back one hour.
        """
        self.scheduler = scheduler.DailyScheduler(
            start_time=datetime.time(hour=1, minute=0))

        job_1 = self.make_job(self.scheduler)
        job_2 = self.make_job(self.scheduler)

        # Exact crossover time:
        # datetime.datetime(2011, 11, 6, 9, 0, 0)
        # This test will use times on either side of it.

        # First schedule before, in PDT:
        now = datetime.datetime(2011, 11, 6, 8, 50, 0)
        timeutils.override_current_time(now)

        # We are at a time zone crossing, so the scheduler should add an extra
        # hour (but it currently does not and so this test fails)
        next_run = self.scheduler.next_runs(job_1)[0]
        pre_crossover_run_time = next_run.run_time

        # Then schedule for the same time, but after the crossover. The time
        # zone has changed to PST, so the scheduler should schedule normally.
        # The result should be one hour 'less' than the pre-crossover scheduled
        # time, which in absolute time is the same number of hours from "now"
        # as the local time zone is different.
        # The net result of this is that both job runs should now be scheduled
        # to run at the same time.
        # I'm going to go take some ibuprofen now.
        now = datetime.datetime(2011, 11, 6, 9, 10, 0)
        timeutils.override_current_time(now)

        next_run = self.scheduler.next_runs(job_2)[0]
        post_crossover_run_time = next_run.run_time

        print pre_crossover_run_timem, post_crossover_run_time

        assert_equal(pre_crossover_run_time,
                     post_crossover_run_time - datetime.timedelta(hours=1))

    def _demonstrate_pytz(self):
        fmt = '%Y-%m-%d %H:%M:%S %Z%z'
        pacific = pytz.timezone('US/Pacific')
        loc_dt = utc_dt.astimezone(pacific)

        before = pacific.normalize(loc_dt - datetime.timedelta(minutes=10))
        after = pacific.normalize(loc_dt + datetime.timedelta(minutes=10))
        print 'PDT->PST crossover:', loc_dt.strftime(fmt)
        print 'Before:', before.strftime(fmt)
        print 'After:', after.strftime(fmt)


class GrocSchedulerTest(TestCase):
    @setup
    def build_scheduler(self):
        self.test_dir = tempfile.mkdtemp()
        self.scheduler = scheduler.GrocScheduler()
        self.action = action.Action("Test Action")
        self.job = job.Job("Test Job", self.action)
        self.job.node_pool = turtle.Turtle()
        self.job.output_path = self.test_dir
        self.action.job = self.job
        self.today = datetime.datetime(2011, 6, 1)

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
        identical_scheduler = scheduler.GrocScheduler()
        identical_scheduler.parse('1st,2nd,3rd,4th mon,tue of mar,apr,sep')
        assert_equal(self.scheduler, identical_scheduler)

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

        assert_gte(next_run_date, self.today)
        assert_equal(next_run_date.month, 6)
        assert_equal(next_run_date.day, 2)
        assert_equal(next_run_date.hour, 0)

    def test_daily_with_time(self):
        self.scheduler.parse('every day at 02:00')
        next_run = self.scheduler.next_runs(self.job)[0]

        next_run_date = next_run.run_time

        assert_gte(next_run_date, self.today)
        assert_equal(next_run_date.year, self.today.year)
        assert_equal(next_run_date.month, 6)
        assert_equal(next_run_date.day, 1)
        assert_equal(next_run_date.hour, 2)
        assert_equal(next_run_date.minute, 0)

    def test_weekly(self):
        self.scheduler.parse('every monday at 01:00')

        next_run = self.scheduler.next_runs(self.job)[0]

        next_run_date = next_run.run_time

        assert_gte(next_run_date, self.today)
        assert_equal(calendar.weekday(next_run_date.year,
                                      next_run_date.month,
                                      next_run_date.day), 0)

    def test_weekly_in_month(self):
        self.scheduler.parse('every monday of january at 00:01')

        next_run = self.scheduler.next_runs(self.job)[0]

        next_run_date = next_run.run_time

        assert_gte(next_run_date, self.today)
        assert_equal(next_run_date.year, self.today.year+1)
        assert_equal(next_run_date.month, 1)
        assert_equal(next_run_date.hour, 0)
        assert_equal(next_run_date.minute, 1)
        assert_equal(calendar.weekday(next_run_date.year,
                                      next_run_date.month,
                                      next_run_date.day), 0)

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
