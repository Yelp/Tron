from __future__ import absolute_import
from __future__ import unicode_literals

import calendar
import datetime

import mock
import pytz

from testifycompat import assert_equal
from testifycompat import assert_gt
from testifycompat import assert_gte
from testifycompat import assert_lt
from testifycompat import assert_lte
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tests import testingutils
from tron import scheduler
from tron.config import config_utils
from tron.config import schedule_parse
from tron.config.config_utils import NullConfigContext
from tron.config.schedule_parse import parse_groc_expression
from tron.utils import timeutils


class TestSchedulerFromConfig(TestCase):
    def test_cron_scheduler(self):
        line = "cron */5 * * 7,8 *"
        config_context = mock.Mock(path='test')
        config = schedule_parse.valid_schedule(line, config_context)
        sched = scheduler.scheduler_from_config(config=config, time_zone=None)
        start_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        next_time = sched.next_run_time(start_time)
        assert_equal(next_time, datetime.datetime(2012, 7, 1, 0))
        assert_equal(str(sched), "cron */5 * * 7,8 *")

    def test_daily_scheduler(self):
        config_context = config_utils.NullConfigContext
        line = "daily 17:32 MWF"
        config = schedule_parse.valid_schedule(line, config_context)
        sched = scheduler.scheduler_from_config(config=config, time_zone=None)
        assert_equal(sched.time_spec.hours, [17])
        assert_equal(sched.time_spec.minutes, [32])
        start_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        for day in [14, 16, 19]:
            next_time = sched.next_run_time(start_time)
            assert_equal(next_time, datetime.datetime(2012, 3, day, 17, 32))
            start_time = next_time

        assert_equal(str(sched), "daily 17:32 MWF")


class GeneralSchedulerTestCase(testingutils.MockTimeTestCase):

    now = datetime.datetime.now().replace(hour=15, minute=0)

    def expected_time(self, date):
        return datetime.datetime.combine(date, datetime.time(14, 30))

    @setup
    def build_scheduler(self):
        self.scheduler = scheduler.GeneralScheduler(timestr='14:30')
        one_day = datetime.timedelta(days=1)
        self.today = self.now.date()
        self.yesterday = self.now - one_day
        self.tomorrow = self.now + one_day

    def test_next_run_time(self):
        next_run = self.scheduler.next_run_time(timeutils.current_time())
        assert_equal(self.expected_time(self.tomorrow), next_run)

        next_run = self.scheduler.next_run_time(self.yesterday)
        assert_equal(self.expected_time(self.today), next_run)

    @mock.patch('tron.scheduler.get_jitter', autospec=True)
    def test_next_run_time_with_jitter(self, mock_jitter):
        mock_jitter.return_value = delta = datetime.timedelta(seconds=-300)
        self.scheduler.jitter = datetime.timedelta(seconds=400)
        expected = self.expected_time(self.tomorrow) + delta
        next_run_time = self.scheduler.next_run_time(None)
        assert_equal(next_run_time, expected)

    def test__str__(self):
        assert_equal(str(self.scheduler), "daily ")

    def test__str__with_jitter(self):
        self.scheduler.jitter = datetime.timedelta(seconds=300)
        assert_equal(str(self.scheduler), "daily  (+/- 0:05:00)")


class GeneralSchedulerTimeTestBase(testingutils.MockTimeTestCase):

    now = datetime.datetime(2012, 3, 14, 15, 9, 26)

    @setup
    def build_scheduler(self):
        self.scheduler = scheduler.GeneralScheduler(timestr='14:30')


class GeneralSchedulerTodayTest(GeneralSchedulerTimeTestBase):

    now = datetime.datetime.now().replace(hour=12, minute=0)

    def test(self):
        # If we schedule a job for later today, it should run today
        run_time = self.scheduler.next_run_time(self.now)
        next_run_date = run_time.date()

        assert_equal(next_run_date, self.now.date())
        earlier_time = datetime.datetime(
            self.now.year,
            self.now.month,
            self.now.day,
            hour=13,
        )
        assert_lte(earlier_time, run_time)


class GeneralSchedulerTomorrowTest(GeneralSchedulerTimeTestBase):

    now = datetime.datetime.now().replace(hour=15, minute=0)

    def test(self):
        # If we schedule a job for later today, it should run today
        run_time = self.scheduler.next_run_time(self.now)
        next_run_date = run_time.date()
        tomorrow = self.now.date() + datetime.timedelta(days=1)

        assert_equal(next_run_date, tomorrow)
        earlier_time = datetime.datetime(
            year=tomorrow.year,
            month=tomorrow.month,
            day=tomorrow.day,
            hour=13,
        )
        assert_lte(earlier_time, run_time)


class GeneralSchedulerLongJobRunTest(GeneralSchedulerTimeTestBase):

    now = datetime.datetime.now().replace(hour=12, minute=0)

    def test_long_jobs_dont_wedge_scheduler(self):
        # Advance days twice as fast as they are scheduled, demonstrating
        # that the scheduler will put things in the past if that's where
        # they belong, and run them as fast as possible

        last_run = self.scheduler.next_run_time(None)
        for i in range(10):
            next_run = self.scheduler.next_run_time(last_run)
            assert_equal(next_run, last_run + datetime.timedelta(days=1))

            self.now += datetime.timedelta(days=2)
            last_run = next_run


class GeneralSchedulerDSTTest(testingutils.MockTimeTestCase):

    now = datetime.datetime(2011, 11, 6, 1, 10, 0)
    now_utc = timeutils.current_time(tz=pytz.timezone('UTC'))

    def hours_until_time(self, run_time, sch):
        tz = sch.time_zone
        now = timeutils.current_time()
        now = tz.localize(now) if tz else now
        seconds = timeutils.delta_total_seconds(run_time - now)
        return round(max(0, seconds) / 60 / 60, 1)

    def hours_diff_at_datetime(self, sch, *args, **kwargs):
        """Return the number of hours until the next *two* runs of a job with
        the given scheduler
        """
        self.now = datetime.datetime(*args, **kwargs)
        next_run = sch.next_run_time(self.now)
        t1 = self.hours_until_time(next_run, sch)
        next_run = sch.next_run_time(next_run.replace(tzinfo=None))
        t2 = self.hours_until_time(next_run, sch)
        return t1, t2

    def _assert_range(self, x, lower, upper):
        assert_gt(x, lower)
        assert_lt(x, upper)

    def test_fall_back(self):
        """This test checks the behavior of the scheduler at the daylight
        savings time 'fall back' point, when the system time zone changes
        from (e.g.) PDT to PST.
        """
        sch = scheduler.GeneralScheduler(time_zone=pytz.timezone('US/Pacific'))

        # Exact crossover time:
        # datetime.datetime(2011, 11, 6, 9, 0, 0, tzinfo=pytz.utc)
        # This test will use times on either side of it.

        # From the PDT vantage point, the run time is 24.2 hours away:
        s1a, s1b = self.hours_diff_at_datetime(sch, 2011, 11, 6, 0, 50, 0)

        # From the PST vantage point, the run time is 22.8 hours away:
        # (this is measured from the point in absolute time 20 minutes after
        # the other measurement)
        s2a, s2b = self.hours_diff_at_datetime(sch, 2011, 11, 6, 1, 10, 0)

        self._assert_range(s1b - s1a, 23.99, 24.11)
        self._assert_range(s2b - s2a, 23.99, 24.11)
        self._assert_range(s1a - s2a, 1.39, 1.41)

    def test_correct_time(self):
        sch = scheduler.GeneralScheduler(time_zone=pytz.timezone('US/Pacific'))
        next_run_time = sch.next_run_time(self.now)
        assert_equal(next_run_time.hour, 0)

    def test_spring_forward(self):
        """This test checks the behavior of the scheduler at the daylight
        savings time 'spring forward' point, when the system time zone changes
        from (e.g.) PST to PDT.
        """
        sch = scheduler.GeneralScheduler(time_zone=pytz.timezone('US/Pacific'))

        # Exact crossover time:
        # datetime.datetime(2011, 3, 13, 2, 0, 0, tzinfo=pytz.utc)
        # This test will use times on either side of it.

        # From the PST vantage point, the run time is 20.2 hours away:
        s1a, s1b = self.hours_diff_at_datetime(sch, 2011, 3, 13, 2, 50, 0)

        # From the PDT vantage point, the run time is 20.8 hours away:
        # (this is measured from the point in absolute time 20 minutes after
        # the other measurement)
        s2a, s2b = self.hours_diff_at_datetime(sch, 2011, 3, 13, 3, 10, 0)

        self._assert_range(s1b - s1a, 23.99, 24.11)
        self._assert_range(s2b - s2a, 23.99, 24.11)
        self._assert_range(s1a - s2a, -0.61, -0.59)

    def test_handles_tz_specific_jobs_with_tz_specific_start_time(self):
        sch = scheduler.GeneralScheduler(time_zone=pytz.timezone('UTC'))
        next_run_time = sch.next_run_time(self.now_utc)
        assert_equal(next_run_time.hour, 0)

    def test_handles_unsetting_the_time_zone(self):
        sch = scheduler.GeneralScheduler(time_zone=None)
        next_run_time = sch.next_run_time(self.now_utc)
        assert_equal(next_run_time.hour, 0)

    def test_handles_changing_the_time_zone(self):
        pacific_now = datetime.datetime.now(pytz.timezone('US/Pacific'))
        pacific_offset = pacific_now.utcoffset().total_seconds() / 60 / 60
        sch = scheduler.GeneralScheduler(time_zone=pytz.timezone('US/Pacific'))
        next_run_time = sch.next_run_time(self.now_utc)
        assert_equal(next_run_time.hour, -pacific_offset)


def parse_groc(config):
    config = schedule_parse.ConfigGenericSchedule('groc daily', config, None)
    return parse_groc_expression(config, NullConfigContext)


def scheduler_from_config(config):
    return scheduler.scheduler_from_config(parse_groc(config), None)


class ComplexParserTest(testingutils.MockTimeTestCase):

    now = datetime.datetime(2011, 6, 1)

    def test_parse_all(self):
        config_string = '1st,2nd,3rd,4th monday,Tue of march,apr,September at 00:00'
        cfg = parse_groc(config_string)
        assert_equal(cfg.ordinals, {1, 2, 3, 4})
        assert_equal(cfg.monthdays, None)
        assert_equal(cfg.weekdays, {1, 2})
        assert_equal(cfg.months, {3, 4, 9})
        assert_equal(cfg.timestr, '00:00')
        assert_equal(
            scheduler_from_config(config_string),
            scheduler_from_config(config_string),
        )

    def test_parse_no_weekday(self):
        cfg = parse_groc(
            '1st,2nd,3rd,10th day of march,apr,September at 00:00',
        )
        assert_equal(cfg.ordinals, None)
        assert_equal(cfg.monthdays, {1, 2, 3, 10})
        assert_equal(cfg.weekdays, None)
        assert_equal(cfg.months, {3, 4, 9})
        assert_equal(cfg.timestr, '00:00')

    def test_parse_no_month(self):
        cfg = parse_groc('1st,2nd,3rd,10th day at 00:00')
        assert_equal(cfg.ordinals, None)
        assert_equal(cfg.monthdays, {1, 2, 3, 10})
        assert_equal(cfg.weekdays, None)
        assert_equal(cfg.months, None)
        assert_equal(cfg.timestr, '00:00')

    def test_parse_monthly(self):
        for test_str in ('1st day', '1st day of month'):
            cfg = parse_groc(test_str)
            assert_equal(cfg.ordinals, None)
            assert_equal(cfg.monthdays, {1})
            assert_equal(cfg.weekdays, None)
            assert_equal(cfg.months, None)
            assert_equal(cfg.timestr, '00:00')

    def test_wildcards(self):
        cfg = parse_groc('every day')
        assert_equal(cfg.ordinals, None)
        assert_equal(cfg.monthdays, None)
        assert_equal(cfg.weekdays, None)
        assert_equal(cfg.months, None)
        assert_equal(cfg.timestr, '00:00')

    def test_daily(self):
        sch = scheduler_from_config('every day')
        next_run_date = sch.next_run_time(None)

        assert_gte(next_run_date, self.now)
        assert_equal(next_run_date.month, 6)
        assert_equal(next_run_date.day, 2)
        assert_equal(next_run_date.hour, 0)

    def test_daily_with_time(self):
        sch = scheduler_from_config('every day at 02:00')
        next_run_date = sch.next_run_time(None)

        assert_gte(next_run_date, self.now)
        assert_equal(next_run_date.year, self.now.year)
        assert_equal(next_run_date.month, 6)
        assert_equal(next_run_date.day, 1)
        assert_equal(next_run_date.hour, 2)
        assert_equal(next_run_date.minute, 0)

    def test_weekly(self):
        sch = scheduler_from_config('every monday at 01:00')
        next_run_date = sch.next_run_time(None)

        assert_gte(next_run_date, self.now)
        assert_equal(
            calendar.weekday(
                next_run_date.year,
                next_run_date.month,
                next_run_date.day,
            ),
            0,
        )

    def test_weekly_in_month(self):
        sch = scheduler_from_config('every monday of January at 00:01')
        next_run_date = sch.next_run_time(None)

        assert_gte(next_run_date, self.now)
        assert_equal(next_run_date.year, self.now.year + 1)
        assert_equal(next_run_date.month, 1)
        assert_equal(next_run_date.hour, 0)
        assert_equal(next_run_date.minute, 1)
        assert_equal(
            calendar.weekday(
                next_run_date.year,
                next_run_date.month,
                next_run_date.day,
            ),
            0,
        )

    def test_monthly(self):
        sch = scheduler_from_config('1st day')
        next_run_date = sch.next_run_time(None)

        assert_gt(next_run_date, self.now)
        assert_equal(next_run_date.month, 7)


if __name__ == '__main__':
    run()
