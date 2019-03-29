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
        timestamp = int(self.now.timestamp())
        assert_equal(DateArithmetic.parse('unixtime'), timestamp)

    def test_unixtime_plus(self):
        timestamp = int(self.now.timestamp()) + 100
        assert_equal(DateArithmetic.parse('unixtime+100'), timestamp)

    def test_unixtime_minus(self):
        timestamp = int(self.now.timestamp()) - 99
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

    def test_round_day(self):
        start = datetime.datetime(2019, 3, 30)
        delta = timeutils.macro_timedelta(start, months=-1)
        assert (start + delta).day == 28


class DateArithmeticYMDHTest(TestCase):
    def test_ym_plus(self):
        def parse(*ym):
            return DateArithmetic.parse('ym+1', datetime.datetime(*ym))

        assert_equal(parse(2018, 1, 1), '2018-02')
        assert_equal(parse(2017, 12, 1), '2018-01')

    def test_ym_minus(self):
        def parse(*ym):
            return DateArithmetic.parse('ym-1', datetime.datetime(*ym))

        assert_equal(parse(2018, 1, 1), '2017-12')
        assert_equal(parse(2018, 2, 1), '2018-01')

    def test_ymd_plus(self):
        def parse(*ymd):
            return DateArithmetic.parse('ymd+1', datetime.datetime(*ymd))

        assert_equal(parse(2018, 1, 1), '2018-01-02')
        assert_equal(parse(2018, 1, 31), '2018-02-01')

    def test_ymd_minus(self):
        def parse(*ymd):
            return DateArithmetic.parse('ymd-1', datetime.datetime(*ymd))

        assert_equal(parse(2018, 1, 1), '2017-12-31')
        assert_equal(parse(2018, 1, 2), '2018-01-01')

    def test_ymdh_plus(self):
        def parse(*ymdh):
            return DateArithmetic.parse('ymdh+1', datetime.datetime(*ymdh))

        assert_equal(parse(2018, 1, 1, 1), '2018-01-01T02')
        assert_equal(parse(2018, 1, 31, 23), '2018-02-01T00')

    def test_ymdh_minus(self):
        def parse(*ymdh):
            return DateArithmetic.parse('ymdh-1', datetime.datetime(*ymdh))

        assert_equal(parse(2018, 1, 1, 1), '2018-01-01T00')
        assert_equal(parse(2018, 1, 1, 0), '2017-12-31T23')

    def test_ymdhm_plus(self):
        def parse(*ymdhm):
            return DateArithmetic.parse('ymdhm+1', datetime.datetime(*ymdhm))

        assert_equal(parse(2018, 1, 1, 1, 1), '2018-01-01T01:02')
        assert_equal(parse(2018, 1, 31, 23, 59), '2018-02-01T00:00')

    def test_ymdhm_minus(self):
        def parse(*ymdhm):
            return DateArithmetic.parse('ymdhm-1', datetime.datetime(*ymdhm))

        assert_equal(parse(2018, 1, 1, 1, 2), '2018-01-01T01:01')
        assert_equal(parse(2018, 1, 1, 0, 0), '2017-12-31T23:59')

    def test_ym_minus_round(self):
        dt = datetime.datetime(2019, 3, 30)
        s = timeutils.DateArithmetic.parse('ym-1', dt=dt)
        assert s == '2019-02'


class TestDateArithmeticWithTimezone(DateArithmeticTestCase):

    now = pytz.timezone("US/Pacific").localize(datetime.datetime(2012, 3, 20))
