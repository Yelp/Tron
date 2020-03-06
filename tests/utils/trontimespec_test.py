from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

import pytz

from testifycompat import assert_equal
from testifycompat import run
from testifycompat import TestCase
from tron.utils import trontimespec


class TestGetTime(TestCase):
    def test_get_time(self):
        assert_equal(datetime.time(4, 15), trontimespec.get_time("4:15"))
        assert_equal(datetime.time(22, 59), trontimespec.get_time("22:59"))

    def test_get_time_invalid_time(self):
        assert not trontimespec.get_time("25:00")
        assert not trontimespec.get_time("22:61")


class TestTimeSpecification(TestCase):
    def _cmp(self, start_time, expected):
        start_time = datetime.datetime(*start_time)
        expected = datetime.datetime(*expected)
        assert_equal(self.time_spec.get_match(start_time), expected)

    def test_get_match_months(self):
        self.time_spec = trontimespec.TimeSpecification(months=[1, 5])
        self._cmp((2012, 3, 14), (2012, 5, 1))
        self._cmp((2012, 5, 22), (2012, 5, 23))
        self._cmp((2012, 12, 22), (2013, 1, 1))

    def test_get_match_monthdays(self):
        self.time_spec = trontimespec.TimeSpecification(
            monthdays=[10, 3, 3, 10],
        )
        self._cmp((2012, 3, 14), (2012, 4, 3))
        self._cmp((2012, 3, 1), (2012, 3, 3))

    def test_get_match_weekdays(self):
        self.time_spec = trontimespec.TimeSpecification(weekdays=[2, 3])
        self._cmp((2012, 3, 14), (2012, 3, 20))
        self._cmp((2012, 3, 20), (2012, 3, 21))

    def test_next_month_generator(self):
        time_spec = trontimespec.TimeSpecification(months=[2, 5])
        gen = time_spec.next_month(datetime.datetime(2012, 3, 14))
        expected = [(5, 2012), (2, 2013), (5, 2013), (2, 2014)]
        assert_equal([next(gen) for _ in range(4)], expected)

    def test_next_day_monthdays(self):
        time_spec = trontimespec.TimeSpecification(monthdays=[5, 10, 15])
        gen = time_spec.next_day(14, 2012, 3)
        assert_equal(list(gen), [15])

        gen = time_spec.next_day(1, 2012, 3)
        assert_equal(list(gen), [5, 10, 15])

    def test_next_day_monthdays_with_last(self):
        time_spec = trontimespec.TimeSpecification(monthdays=[5, 'LAST'])
        gen = time_spec.next_day(14, 2012, 3)
        assert_equal(list(gen), [31])

    def test_next_day_weekdays(self):
        time_spec = trontimespec.TimeSpecification(weekdays=[1, 5])
        gen = time_spec.next_day(14, 2012, 3)
        assert_equal(list(gen), [16, 19, 23, 26, 30])

        gen = time_spec.next_day(1, 2012, 3)
        assert_equal(list(gen), [2, 5, 9, 12, 16, 19, 23, 26, 30])

    def test_next_day_weekdays_with_ordinals(self):
        time_spec = trontimespec.TimeSpecification(
            weekdays=[1, 5],
            ordinals=[1, 3],
        )
        gen = time_spec.next_day(14, 2012, 3)
        assert_equal(list(gen), [16, 19])

        gen = time_spec.next_day(1, 2012, 3)
        assert_equal(list(gen), [2, 5, 16, 19])

    def test_next_time_timestr(self):
        time_spec = trontimespec.TimeSpecification(timestr="13:13")
        start_date = datetime.datetime(2012, 3, 14, 0, 15)
        time = time_spec.next_time(start_date, True)
        assert_equal(time, datetime.time(13, 13))

        start_date = datetime.datetime(2012, 3, 14, 13, 13)
        assert time_spec.next_time(start_date, True) is None
        time = time_spec.next_time(start_date, False)
        assert_equal(time, datetime.time(13, 13))

    def test_next_time_hours(self):
        time_spec = trontimespec.TimeSpecification(hours=[4, 10])
        start_date = datetime.datetime(2012, 3, 14, 0, 15)
        time = time_spec.next_time(start_date, True)
        assert_equal(time, datetime.time(4, 0))

        start_date = datetime.datetime(2012, 3, 14, 13, 13)
        assert time_spec.next_time(start_date, True) is None
        time = time_spec.next_time(start_date, False)
        assert_equal(time, datetime.time(4, 0))

    def test_next_time_minutes(self):
        time_spec = trontimespec.TimeSpecification(
            minutes=[30, 20, 30],
            seconds=[0],
        )
        start_date = datetime.datetime(2012, 3, 14, 0, 25)
        time = time_spec.next_time(start_date, True)
        assert_equal(time, datetime.time(0, 30))

        start_date = datetime.datetime(2012, 3, 14, 23, 30)
        assert time_spec.next_time(start_date, True) is None
        time = time_spec.next_time(start_date, False)
        assert_equal(time, datetime.time(0, 20))

    def test_next_time_hours_and_minutes_and_seconds(self):
        time_spec = trontimespec.TimeSpecification(
            minutes=[20, 30],
            hours=[1, 5],
            seconds=[4, 5],
        )
        start_date = datetime.datetime(2012, 3, 14, 1, 25)
        time = time_spec.next_time(start_date, True)
        assert_equal(time, datetime.time(1, 30, 4))

        start_date = datetime.datetime(2012, 3, 14, 5, 30, 6)
        assert time_spec.next_time(start_date, True) is None
        time = time_spec.next_time(start_date, False)
        assert_equal(time, datetime.time(1, 20, 4))

    def test_get_match_dst_spring_forward(self):
        tz = pytz.timezone('US/Pacific')
        time_spec = trontimespec.TimeSpecification(
            hours=[0, 1, 2, 3, 4],
            minutes=[0],
            seconds=[0],
            timezone='US/Pacific',
        )
        start = trontimespec.naive_as_timezone(datetime.datetime(2020, 3, 8, 1), tz)
        # Springing forward, the next hour after 1AM should be 3AM
        next_time = time_spec.get_match(start)
        assert next_time.hour == 3

    def test_get_match_dst_fall_back(self):
        tz = pytz.timezone('US/Pacific')
        time_spec = trontimespec.TimeSpecification(
            hours=[0, 1, 2, 3, 4],
            minutes=[0],
            seconds=[0],
            timezone='US/Pacific',
        )
        start = trontimespec.naive_as_timezone(datetime.datetime(2020, 11, 1, 1), tz)
        # Falling back, the next hour after 1AM is 1AM again. But we only run on the first 1AM
        # Next run time should be 2AM
        next_time = time_spec.get_match(start)
        assert next_time.hour == 2


if __name__ == "__main__":
    run()
