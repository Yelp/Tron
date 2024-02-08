"""Functions for working with dates and timestamps."""
import calendar
import datetime
import re


def current_time(tz=None):
    """Return the current datetime."""
    return datetime.datetime.now(tz=tz)


def current_timestamp():
    """Return the current time as a timestamp."""
    return current_time().timestamp()


def delta_total_seconds(td):
    """Equivalent to timedelta.total_seconds() available in Python 2.7."""
    microseconds, seconds, days = td.microseconds, td.seconds, td.days
    return (microseconds + (seconds + days * 24 * 3600) * 10**6) / 10**6


def macro_timedelta(start_date, years=0, months=0, days=0, hours=0, minutes=0):
    """Since datetime doesn't provide timedeltas at the year or month level,
    this function generates timedeltas of the appropriate sizes.
    """
    delta = datetime.timedelta(days=days, hours=hours, minutes=minutes)

    new_month = start_date.month + months
    while new_month > 12:
        new_month -= 12
        years += 1
    while new_month < 1:
        new_month += 12
        years -= 1
    new_year = start_date.year + years

    # TRON-1045: round day to the max days in a given month if the day doesn't
    # exist for that month. (e.g. Feb 30 rounds to Feb 28 in a non-leap year)
    _, days_in_month = calendar.monthrange(new_year, new_month)
    new_day = min(start_date.day, days_in_month)

    end_date = datetime.datetime(
        new_year,
        new_month,
        new_day,
        start_date.hour,
        start_date.minute,
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


class DateArithmetic:
    """Parses a string which contains a date arithmetic pattern and returns
    a date with the delta added or subtracted.
    """

    DATE_TYPE_PATTERN = re.compile(r"(\w+)([+-]\d+)?")

    DATE_FORMATS = {
        "year": "%Y",
        "month": "%m",
        "day": "%d",
        "hour": "%H",
        "shortdate": "%Y-%m-%d",
        "ym": "%Y-%m",
        "ymd": "%Y-%m-%d",
        "ymdh": "%Y-%m-%dT%H",
        "ymdhm": "%Y-%m-%dT%H:%M",
    }

    @classmethod
    def parse(cls, date_str, dt=None):
        """Parse a date arithmetic pattern (Ex: 'shortdate-1'). Supports
        date strings: shortdate, year, month, day, unixtime, daynumber.
        Supports subtraction and addition operations of integers. Time unit is
        based on date format (Ex: seconds for unixtime, days for day).
        """
        dt = dt or current_time()
        date_str = date_str.replace(" ", "")
        match = cls.DATE_TYPE_PATTERN.match(date_str)
        if not match:
            return
        attr, value = match.groups()
        delta = int(value) if value else 0

        if attr in ("shortdate", "year", "month", "day", "hour"):
            if delta:
                kwargs = {"days" if attr == "shortdate" else attr + "s": delta}
                dt += macro_timedelta(dt, **kwargs)
            return dt.strftime(cls.DATE_FORMATS[attr])

        if attr in ("ym", "ymd", "ymdh", "ymdhm"):
            args = [0] * len(attr)
            args[-1] = delta
            dt += macro_timedelta(dt, *args)
            return dt.strftime(cls.DATE_FORMATS[attr])

        if attr == "unixtime":
            return int(dt.timestamp()) + delta

        if attr == "daynumber":
            return dt.toordinal() + delta
