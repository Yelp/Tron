"""Functions for working with dates and timestamps."""
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import datetime
import re
import time
from calendar import timegm


def current_time(tz=None):
    """Return the current datetime."""
    return datetime.datetime.now(tz=tz)


def current_timestamp():
    """Return the current time as a timestamp."""
    return to_timestamp(current_time())


def to_timestamp(time_val):
    """Generate a unix timestamp for the given datetime instance"""
    # TODO: replace with datetime.timestamp() after python3.6
    if time_val.tzinfo:
        return timegm(time_val.utctimetuple())
    return time.mktime(time_val.utctimetuple())


def delta_total_seconds(td):
    """Equivalent to timedelta.total_seconds() available in Python 2.7.
    """
    microseconds, seconds, days = td.microseconds, td.seconds, td.days
    return (microseconds + (seconds + days * 24 * 3600) * 10**6) / 10**6


def macro_timedelta(start_date, years=0, months=0, days=0, hours=0):
    """Since datetime doesn't provide timedeltas at the year or month level,
    this function generates timedeltas of the appropriate sizes.
    """
    delta = datetime.timedelta(days=days, hours=hours)

    new_month = start_date.month + months
    while new_month > 12:
        new_month -= 12
        years += 1
    while new_month < 1:
        new_month += 12
        years -= 1

    end_date = datetime.datetime(
        start_date.year + years,
        new_month,
        start_date.day,
        start_date.hour,
    )
    month_and_year_delta = end_date - start_date.replace(tzinfo=None)
    delta += month_and_year_delta

    return delta


def duration(start_time, end_time=None):
    """Get a timedelta between end_time and start_time, where end_time defaults
    to now().

    WARNING: mixing tz-aware and naive datetimes in start_time and end_time
    will cause an error.
    """
    if not start_time:
        return None
    last_time = end_time if end_time else current_time()
    return last_time - start_time


class DateArithmetic(object):
    """Parses a string which contains a date arithmetic pattern and returns
    a date with the delta added or subtracted.
    """

    DATE_TYPE_PATTERN = re.compile(r'(\w+)([+-]\d+)?')

    DATE_FORMATS = {
        'year': '%Y',
        'month': '%m',
        'day': '%d',
        'hour': '%H',
        'shortdate': '%Y-%m-%d',
    }

    @classmethod
    def parse(cls, date_str, dt=None):
        """Parse a date arithmetic pattern (Ex: 'shortdate-1'). Supports
        date strings: shortdate, year, month, day, unixtime, daynumber.
        Supports subtraction and addition operations of integers. Time unit is
        based on date format (Ex: seconds for unixtime, days for day).
        """
        dt = dt or current_time()

        match = cls.DATE_TYPE_PATTERN.match(date_str)
        if not match:
            return
        attr, value = match.groups()
        delta = int(value) if value else 0

        if attr in ('shortdate', 'year', 'month', 'day', 'hour'):
            if delta:
                kwargs = {'days' if attr == 'shortdate' else attr + 's': delta}
                dt += macro_timedelta(dt, **kwargs)
            return dt.strftime(cls.DATE_FORMATS[attr])

        if attr == 'unixtime':
            return int(to_timestamp(dt)) + delta

        if attr == 'daynumber':
            return dt.toordinal() + delta
