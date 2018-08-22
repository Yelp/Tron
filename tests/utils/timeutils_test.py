from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

import pytz

from testifycompat import assert_equal
from testifycompat import setup
from testifycompat import TestCase
from tests import testingutils
from tron.utils import timeutils
from tron.utils.timeutils import DateArithmetic
from tron.utils.timeutils import duration
from tron.utils.timeutils import macro_timedelta


class TestToTimestamp(TestCase):
    def test_normal_time_with_timezone(self):
        # 62 minutes after the epoch
        start_date = pytz.utc.localize(datetime.datetime(1970, 1, 1, 1, 2))
        assert_equal(timeutils.to_timestamp(start_date), 62 * 60)

    def test_ambiguous_times(self):
        pacific_tz = pytz.timezone("US/Pacific")
        before_fall_back = timeutils.to_timestamp(
            pacific_tz.localize(
                datetime.datetime(2017, 11, 5, 1, 23),
                is_dst=True,
            ),
        )
        after_fall_back = timeutils.to_timestamp(
            pacific_tz.localize(
                datetime.datetime(2017, 11, 5, 1, 23),
                is_dst=False,
            ),
        )
        assert_equal(after_fall_back - before_fall_back, 60 * 60)


class TestTimeDelta(TestCase):
    @setup
    def make_dates(self):
        self.start_nonleap = datetime.datetime(year=2011, month=1, day=1)
        self.end_nonleap = datetime.datetime(year=2011, month=12, day=31)
        self.begin_feb_nonleap = datetime.datetime(year=2011, month=2, day=1)
        self.start_leap = datetime.datetime(year=2012, month=1, day=1)
        self.end_leap = datetime.datetime(year=2012, month=12, day=31)
        self.begin_feb_leap = datetime.datetime(year=2012, month=2, day=1)

    def check_delta(self, start, target, years=0, months=0, days=0):
        assert_equal(
            start + macro_timedelta(
                start,
                years=years,
                months=months,
                days=days,
            ),
            target,
        )

    def test_days(self):
        self.check_delta(
            self.start_nonleap,
            datetime.datetime(year=2011, month=1, day=11),
            days=10,
        )
        self.check_delta(
            self.end_nonleap,
            datetime.datetime(year=2012, month=1, day=10),
            days=10,
        )
        self.check_delta(
            self.start_leap,
            datetime.datetime(year=2012, month=1, day=11),
            days=10,
        )
        self.check_delta(
            self.end_leap,
            datetime.datetime(year=2013, month=1, day=10),
            days=10,
        )
        self.check_delta(
            self.begin_feb_nonleap,
            datetime.datetime(year=2011, month=3, day=1),
            days=28,
        )
        self.check_delta(
            self.begin_feb_leap,
            datetime.datetime(year=2012, month=3, day=1),
            days=29,
        )

    def test_months(self):
        self.check_delta(
            self.start_nonleap,
            datetime.datetime(year=2011, month=11, day=1),
            months=10,
        )
        self.check_delta(
            self.end_nonleap,
            datetime.datetime(year=2012, month=10, day=31),
            months=10,
        )
        self.check_delta(
            self.start_leap,
            datetime.datetime(year=2012, month=11, day=1),
            months=10,
        )
        self.check_delta(
            self.end_leap,
            datetime.datetime(year=2013, month=10, day=31),
            months=10,
        )
        self.check_delta(
            self.begin_feb_nonleap,
            datetime.datetime(year=2011, month=12, day=1),
            months=10,
        )
        self.check_delta(
            self.begin_feb_leap,
            datetime.datetime(year=2012, month=12, day=1),
            months=10,
        )

    def test_years(self):
        self.check_delta(
            self.start_nonleap,
            datetime.datetime(year=2015, month=1, day=1),
            years=4,
        )
        self.check_delta(
            self.end_nonleap,
            datetime.datetime(year=2015, month=12, day=31),
            years=4,
        )
        self.check_delta(
            self.start_leap,
            datetime.datetime(year=2016, month=1, day=1),
            years=4,
        )
        self.check_delta(
            self.end_leap,
            datetime.datetime(year=2016, month=12, day=31),
            years=4,
        )
        self.check_delta(
            self.begin_feb_nonleap,
            datetime.datetime(year=2015, month=2, day=1),
            years=4,
        )
        self.check_delta(
            self.begin_feb_leap,
            datetime.datetime(year=2016, month=2, day=1),
            years=4,
        )

    def test_start_date_with_timezone(self):
        pacific_tz = pytz.timezone("US/Pacific")
        start_date = pacific_tz.localize(
            datetime.datetime(year=2018, month=1, day=3, hour=13),
        )
        expected_end = pacific_tz.localize(
            datetime.datetime(year=2018, month=1, day=1, hour=13),
        )
        self.check_delta(
            start_date,
            expected_end,
            days=-2,
        )


class TestDuration(TestCase):
    @setup
    def setup_times(self):
        self.earliest = datetime.datetime(2012, 2, 1, 3, 0, 0)
        self.latest = datetime.datetime(2012, 2, 1, 3, 20, 0)

    def test_duration(self):
        assert_equal(
            duration(self.earliest, self.latest),
            datetime.timedelta(0, 60 * 20),
        )

    def test_duration_no_end(self):
        delta = duration(self.earliest)
        assert delta.days >= 40

    def test_duration_no_start(self):
        assert_equal(duration(None), None)


class TestDeltaTotalSeconds(TestCase):
    def test(self):
        expected = 86702.004002999995
        delta = datetime.timedelta(*range(1, 6))
        delta_seconds = timeutils.delta_total_seconds(delta)
        assert_equal(delta_seconds, expected)


class DateArithmeticTestCase(testingutils.MockTimeTestCase):

    # Set a date with days less then 28, otherwise some tests will fail
    # when run on days > 28.
    now = datetime.datetime(2012, 3, 20)

    def _cmp_date(self, item, dt):
        assert_equal(DateArithmetic.parse(item), dt.strftime("%Y-%m-%d"))

    def _cmp_day(self, item, dt):
        assert_equal(DateArithmetic.parse(item), dt.strftime("%d"))

    def _cmp_month(self, item, dt):
        assert_equal(DateArithmetic.parse(item), dt.strftime("%m"))

    def _cmp_year(self, item, dt):
        assert_equal(DateArithmetic.parse(item), dt.strftime("%Y"))

    def test_shortdate(self):
        self._cmp_date('shortdate', self.now)

    def test_shortdate_plus(self):
        for i in range(50):
            dt = self.now + datetime.timedelta(days=i)
            self._cmp_date('shortdate+%s' % i, dt)

    def test_shortdate_minus(self):
        for i in range(50):
            dt = self.now - datetime.timedelta(days=i)
            self._cmp_date('shortdate-%s' % i, dt)

    def test_day(self):
        self._cmp_day('day', self.now)

    def test_day_minus(self):
        for i in range(50):
            dt = self.now - datetime.timedelta(days=i)
            self._cmp_day('day-%s' % i, dt)

    def test_day_plus(self):
        for i in range(50):
            dt = self.now + datetime.timedelta(days=i)
            self._cmp_day('day+%s' % i, dt)

    def test_month(self):
        self._cmp_month('month', self.now)

    def test_month_plus(self):
        for i in range(50):
            dt = self.now + timeutils.macro_timedelta(self.now, months=i)
            self._cmp_month('month+%s' % i, dt)

    def test_month_minus(self):
        for i in range(50):
            dt = self.now - timeutils.macro_timedelta(self.now, months=i)
            self._cmp_month('month-%s' % i, dt)

    def test_year(self):
        self._cmp_year('year', self.now)

    def test_year_plus(self):
        for i in range(50):
            dt = self.now + timeutils.macro_timedelta(self.now, years=i)
            self._cmp_year('year+%s' % i, dt)

    def test_year_minus(self):
        for i in range(50):
            dt = self.now - timeutils.macro_timedelta(self.now, years=i)
            self._cmp_year('year-%s' % i, dt)

    def test_unixtime(self):
        timestamp = int(timeutils.to_timestamp(self.now))
        assert_equal(DateArithmetic.parse('unixtime'), timestamp)

    def test_unixtime_plus(self):
        timestamp = int(timeutils.to_timestamp(self.now)) + 100
        assert_equal(DateArithmetic.parse('unixtime+100'), timestamp)

    def test_unixtime_minus(self):
        timestamp = int(timeutils.to_timestamp(self.now)) - 99
        assert_equal(DateArithmetic.parse('unixtime-99'), timestamp)

    def test_daynumber(self):
        daynum = self.now.toordinal()
        assert_equal(DateArithmetic.parse('daynumber'), daynum)

    def test_daynumber_plus(self):
        daynum = self.now.toordinal() + 1
        assert_equal(DateArithmetic.parse('daynumber+1'), daynum)

    def test_daynumber_minus(self):
        daynum = self.now.toordinal() - 1
        assert_equal(DateArithmetic.parse('daynumber-1'), daynum)

    def test_hour(self):
        hour = self.now.strftime("%H")
        assert_equal(DateArithmetic.parse('hour'), hour)

    def test_hour_plus(self):
        hour = "%02d" % ((int(self.now.strftime("%H")) + 1) % 24)
        assert_equal(DateArithmetic.parse('hour+1'), hour)

    def test_hour_minus(self):
        hour = "%02d" % ((int(self.now.strftime("%H")) - 1) % 24)
        assert_equal(DateArithmetic.parse('hour-1'), hour)

    def test_bad_date_format(self):
        assert DateArithmetic.parse('~~') is None


class TestDateArithmeticWithTimezone(DateArithmeticTestCase):

    now = pytz.timezone("US/Pacific").localize(datetime.datetime(2012, 3, 20))
