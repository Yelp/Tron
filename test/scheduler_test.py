import calendar
import datetime
import tempfile
import shutil
import time

import pytz
from testify import *
from testify.utils import turtle

from tron import action
from tron import job
from tron import mcp
from tron import scheduler
from tron.utils import groctimespecification
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
        # If we schedule a job for later today, it should run today
        next_run = self.scheduler.next_runs(self.job)[0]
        next_run_date = next_run.run_time.date()
        tomorrow = self.now.date() + datetime.timedelta(days=1)

        assert_equal(next_run_date, tomorrow)
        assert_lte(datetime.datetime(year=tomorrow.year, month=tomorrow.month,
                                     day=tomorrow.day, hour=13),
                   next_run.run_time)


class DailySchedulerLongJobRunTest(DailySchedulerTimeTestBase):

    @setup
    def set_time(self):
        self.now = datetime.datetime.now().replace(hour=12, minute=0)
        timeutils.override_current_time(self.now)

    def test_long_jobs_dont_wedge_scheduler(self):
        # Advance days twice as fast as they are scheduled, demonstrating
        # that the scheduler will put things in the past if that's where
        # they belong, and run them as fast as possible

        last_run = self.scheduler.next_runs(self.job)[0].run_time
        for i in range(10):
            next_run = self.scheduler.next_runs(self.job)[0].run_time
            assert_equal(next_run, last_run + datetime.timedelta(days=1))

            self.now += datetime.timedelta(days=2)
            timeutils.override_current_time(self.now)

            last_run = next_run


class DailySchedulerDSTTest(TestCase):

    @setup
    def setup_tmp(self):
        self.tmp_dirs = []

    @setup
    def setup_scheduler(self):
        self.fmt = '%Y-%m-%d %H:%M:%S %Z%z'

    @teardown
    def unset_time(self):
        timeutils.override_current_time(None)

    @teardown
    def cleanup(self):
        for tmp_dir in self.tmp_dirs:
            shutil.rmtree(tmp_dir)

    def make_job(self, sch):
        """Create a dummy job with the given scheduler that stores its data in
        a temp folder that will be deleted on teardown
        """
        tmp_dir = tempfile.mkdtemp()
        self.tmp_dirs.append(tmp_dir)
        a = action.Action("Test Action - Early Christmas Shopping")
        j = job.Job("Test Job", a)
        j.node_pool = turtle.Turtle()
        j.output_path = tmp_dir
        j.scheduler = sch
        a.job = j
        return j

    def hours_to_job_at_datetime(self, sch, *args, **kwargs):
        """Return the number of hours until the next *two* runs of a job with
        the given scheduler
        """
        # if you need to print a datetime with tz info, use this:
        #   fmt = '%Y-%m-%d %H:%M:%S %Z%z'
        #   my_datetime.strftime(fmt)

        j = self.make_job(sch)
        now = datetime.datetime(*args, **kwargs)
        timeutils.override_current_time(now)
        next_run = sch.next_runs(j)[0]
        t1 = round(next_run.seconds_until_run_time()/60/60, 1)
        next_run = sch.next_runs(j)[0]
        t2 = round(next_run.seconds_until_run_time()/60/60, 1)
        return t1, t2

    def _assert_range(self, x, lower, upper):
        assert_gt(x, lower)
        assert_lt(x, upper)

    def test_fall_back(self):
        """This test checks the behavior of the scheduler at the daylight
        savings time 'fall back' point, when the system time zone changes
        from (e.g.) PDT to PST.
        """

        sch = scheduler.DailyScheduler(
            start_time=datetime.time(hour=0, minute=0),
            time_zone=pytz.timezone('US/Pacific'))

        # Exact crossover time:
        # datetime.datetime(2011, 11, 6, 9, 0, 0, tzinfo=pytz.utc)
        # This test will use times on either side of it.

        # From the PDT vantage point, the run time is 24.2 hours away:
        s1a, s1b = self.hours_to_job_at_datetime(sch, 2011, 11, 6, 0, 50, 0)

        # From the PST vantage point, the run time is 22.8 hours away:
        # (this is measured from the point in absolute time 20 minutes after
        # the other measurement)
        s2a, s2b = self.hours_to_job_at_datetime(sch, 2011, 11, 6, 1, 10, 0)

        self._assert_range(s1b - s1a, 23.99, 24.11)
        self._assert_range(s2b - s2a, 23.99, 24.11)
        self._assert_range(s1a - s2a, 1.39, 1.41)

    def test_correct_time(self):
        sch = scheduler.DailyScheduler(
            start_time=datetime.time(hour=0, minute=0),
            time_zone=pytz.timezone('US/Pacific'))

        j = self.make_job(sch)
        now = datetime.datetime(2011, 11, 6, 1, 10, 0)
        timeutils.override_current_time(now)
        next_run_time = sch.next_runs(j)[0].run_time
        assert_equal(next_run_time.hour, 0)

    def test_spring_forward(self):
        """This test checks the behavior of the scheduler at the daylight
        savings time 'spring forward' point, when the system time zone changes
        from (e.g.) PST to PDT.
        """

        sch = scheduler.DailyScheduler(
            start_time=datetime.time(hour=0, minute=0),
            time_zone=pytz.timezone('US/Pacific'))

        # Exact crossover time:
        # datetime.datetime(2011, 3, 13, 2, 0, 0, tzinfo=pytz.utc)
        # This test will use times on either side of it.

        # From the PST vantage point, the run time is 20.2 hours away:
        s1a, s1b = self.hours_to_job_at_datetime(sch, 2011, 3, 13, 2, 50, 0)

        # From the PDT vantage point, the run time is 20.8 hours away:
        # (this is measured from the point in absolute time 20 minutes after
        # the other measurement)
        s2a, s2b = self.hours_to_job_at_datetime(sch, 2011, 3, 13, 3, 10, 0)

        self._assert_range(s1b - s1a, 23.99, 24.11)
        self._assert_range(s2b - s2a, 23.99, 24.11)
        self._assert_range(s1a - s2a, -0.61, -0.59)


class ComplexParserTest(TestCase):

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
