"""Time utilites"""
import datetime
import time


# Global time override
_current_time_override = None


def override_current_time(current_time):
    """Interface for overriding the current time

    This is a test hook for forcing the app to think it's a specific time.
    """
    global _current_time_override
    _current_time_override = current_time


def current_time():
    """Standard interface for generating the current time."""
    if _current_time_override is not None:
        return _current_time_override
    else:
        return datetime.datetime.now()


def current_timestamp():
    return to_timestamp(current_time())


def to_timestamp(time_val):
    """Generate a unix timestamp for the given datetime instance"""
    return time.mktime(time_val.timetuple())


def macro_timedelta(start_date, years=0, months=0, days=0):
    """Since datetime doesn't provide timedeltas at the year or month level,
    this function generates timedeltas of the appropriate sizes.
    """
    delta = datetime.timedelta(days=days)

    new_month = start_date.month + months
    while new_month > 12:
        new_month -= 12
        years += 1
    while new_month < 1:
        new_month += 12
        years -= 1

    end_date = datetime.datetime(start_date.year + years,
                                 new_month, start_date.day)
    delta += end_date - start_date

    return delta
