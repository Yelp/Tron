"""
Parse and validate scheduler configuration and return immutable structures.
"""
import calendar
from collections import namedtuple
import datetime
import re

from tron.config import ConfigError


ConfigDailyScheduler = namedtuple(
    'ConfigDailyScheduler',
    ['ordinals', 'weekdays', 'monthdays', 'months', 'timestr']
)


ConfigConstantScheduler = namedtuple('ConfigConstantScheduler', [])


ConfigIntervalScheduler = namedtuple(
    'ConfigIntervalScheduler', [
        'timedelta',    # datetime.timedelta
    ])

class ScheduleParseError(Exception):
    pass



def valid_schedule(path, schedule):
    if isinstance(schedule, basestring):
        schedule = schedule.strip()
        scheduler_args = schedule.split()
        scheduler_name = scheduler_args.pop(0).lower()

        if schedule == 'constant':
            return ConfigConstantScheduler()
        elif scheduler_name == 'daily':
            return valid_daily_scheduler(*scheduler_args)
        elif scheduler_name == 'interval':
            return valid_interval_scheduler(' '.join(scheduler_args))
        else:
            return parse_daily_expression(schedule)

    if 'interval' in schedule:
        return valid_interval_scheduler(**schedule)
    elif 'start_time' in schedule or 'days' in schedule:
        return valid_daily_scheduler(**schedule)
    else:
        raise ConfigError("Unknown scheduler at %s: %s" % (path, schedule))


def valid_daily_scheduler(start_time=None, days=None):
    """Old style, will be converted to DailyScheduler with a compatibility
    function

    schedule:
        start_time: "07:00:00"
        days: "MWF"
    """

    err_msg = ("Start time must be in string format HH:MM[:SS]. Seconds"
               " are ignored but parsed so as to be backward-compatible."
               " You said: %s")

    if start_time is None:
        hms = ['00', '00']
    else:
        if not isinstance(start_time, basestring):
            raise ConfigError(err_msg % start_time)

        # make sure at least hours and minutes are specified
        hms = start_time.strip().split(':')

        if len(hms) < 2:
            raise ConfigError(err_msg % start_time)

    weekdays = set(CONVERT_DAYS_INT[d] for d in days or 'MTWRFSU')
    if weekdays == set([0, 1, 2, 3, 4, 5, 6]):
        days_str = 'day'
    else:
        # incoming string is MTWRF, we want M,T,W,R,F for the parser
        days_str = ','.join(days)

    return parse_daily_expression(
        'every %s of month at %s:%s' % (days_str, hms[0], hms[1])
    )


def valid_interval_scheduler(interval):
    # remove spaces
    interval = ''.join(interval.split())

    # Shortcut values for intervals
    TIME_INTERVAL_SHORTCUTS = {
        'hourly': dict(hours=1),
    }

    # Translations from possible configuration units to the argument to
    # datetime.timedelta
    TIME_INTERVAL_UNITS = {
        'months': ['mo', 'month', 'months'],
        'days': ['d', 'day', 'days'],
        'hours': ['h', 'hr', 'hrs', 'hour', 'hours'],
        'minutes': ['m', 'min', 'mins', 'minute', 'minutes'],
        'seconds': ['s', 'sec', 'secs', 'second', 'seconds']
    }

    if interval in TIME_INTERVAL_SHORTCUTS:
        kwargs = TIME_INTERVAL_SHORTCUTS[interval]
    else:
        # Split digits and characters into tokens
        interval_re = re.compile(r"\d+|[a-zA-Z]+")
        interval_tokens = interval_re.findall(interval)
        if len(interval_tokens) != 2:
            raise ConfigError("Invalid interval specification: %s", interval)

        value, units = interval_tokens

        kwargs = {}
        for key, unit_set in TIME_INTERVAL_UNITS.iteritems():
            if units in unit_set:
                kwargs[key] = int(value)
                break
        else:
            raise ConfigError("Invalid interval specification: %s", interval)

    return ConfigIntervalScheduler(timedelta=datetime.timedelta(**kwargs))


def day_canonicalization_map():
    """Build a map of weekday synonym to int index 0-6 inclusive."""
    canon_map = dict()

    # 7-element lists with weekday names in order
    weekday_lists = (calendar.day_name,
                     calendar.day_abbr,
                     ('m', 't', 'w', 'r', 'f', 's', 'u'),
                     ('mo', 'tu', 'we', 'th', 'fr', 'sa', 'su'))
    for day_list in weekday_lists:
        for day_name_synonym, day_index in zip(day_list, range(7)):
            canon_map[day_name_synonym] = day_index
            canon_map[day_name_synonym.lower()] = day_index
            canon_map[day_name_synonym.upper()] = day_index
    return canon_map

# Canonicalize weekday names to integer indices
CONVERT_DAYS_INT = day_canonicalization_map()   # day name/abbrev => {0123456}


def month_canonicalization_map():
    """Build a map of month synonym to int index 0-11 inclusive."""
    canon_map = dict()

    # calendar stores month data with a useless element in front. cut it off.
    monthname_lists = (calendar.month_name[1:], calendar.month_abbr[1:])
    for month_list in monthname_lists:
        for key, value in zip(month_list, range(1, 13)):
            canon_map[key] = value
            canon_map[key.lower()] = value
    return canon_map


# Canonicalize month names to integer indices
# month name/abbrev => {0 <= k <= 11}
CONVERT_MONTHS = month_canonicalization_map()


def daily_schedule_parser_re():
    """Build a regular expression that matches this:

        ("every"|ordinal) (day) ["of|in" (monthspec)] (["at"] HH:MM)

    ordinal   - comma-separated list of "1st" and so forth
    days      - comma-separated list of days of the week (for example,
                "mon", "tuesday", with both short and long forms being
                accepted); "every day" is equivalent to
                "every mon,tue,wed,thu,fri,sat,sun"
    monthspec - comma-separated list of month names (for example,
                "jan", "march", "sep"). If omitted, implies every month.
                You can also say "month" to mean every month, as in
                "1,8th,15,22nd of month 09:00".
    HH:MM     - time of day in 24 hour time.

    This is a slightly more permissive version of Google App Engine's schedule
    parser, documented here:
    http://code.google.com/appengine/docs/python/config/cron.html#The_Schedule_Format
    """

    # m|mon|monday|...|day
    DAY_VALUES = '|'.join(CONVERT_DAYS_INT.keys() + ['day'])

    # jan|january|...|month
    MONTH_VALUES = '|'.join(CONVERT_MONTHS.keys() + ['month'])

    DATE_SUFFIXES = 'st|nd|rd|th'

    # every|1st|2nd|3rd (also would accept 3nd, 1rd, 4st)
    MONTH_DAYS_EXPR = '(?P<month_days>every|((\d+(%s),?)+))?' % DATE_SUFFIXES
    DAYS_EXPR = r'((?P<days>((%s),?)+))?' % DAY_VALUES
    MONTHS_EXPR = r'((in|of)\s+(?P<months>((%s),?)+))?' % MONTH_VALUES

    # [at] 00:00
    TIME_EXPR = r'((at\s+)?(?P<time>\d\d:\d\d))?'

    DAILY_SCHEDULE_EXPR = ''.join([
        r'^',
        MONTH_DAYS_EXPR, r'\s*',
        DAYS_EXPR, r'\s*',
        MONTHS_EXPR, r'\s*',
         TIME_EXPR, r'\s*',
        r'$'
    ])
    return re.compile(DAILY_SCHEDULE_EXPR)


# Matches expressions of the form
# ``("every"|ordinal) (days) ["of|in" (monthspec)] (["at"] HH:MM)``.
# See :py:func:`daily_schedule_parser_re` for details.
DAILY_SCHEDULE_RE = daily_schedule_parser_re()


def _parse_number(day):
    return int(''.join(c for c in day if c.isdigit()))

def parse_daily_expression(expression):
    """Given an expression of the form in the docstring of
    daily_schedule_parser_re(), return the parsed values in a
    ConfigDailyScheduler
    """
    m = DAILY_SCHEDULE_RE.match(expression.lower())
    if not m:
        raise ScheduleParseError('Expression %r is not a valid scheduler'
                                 ' expression.' % expression)

    timestr = m.group('time')
    if timestr is None:
        timestr = '00:00'

    if m.group('days') in (None, 'day'):
        weekdays = None
    else:
        weekdays = set(CONVERT_DAYS_INT[d] for d in m.group('days').split(','))

    monthdays = None
    ordinals = None

    if m.group('month_days') != 'every':
        values = set(_parse_number(n)
                     for n in m.group('month_days').split(','))
        if weekdays is None:
            monthdays = values
        else:
            ordinals = values

    if m.group('months') in (None, 'month'):
        months = None
    else:
        months = set(CONVERT_MONTHS[mo] for mo in m.group('months').split(','))

    return ConfigDailyScheduler(
        ordinals=ordinals,
        weekdays=weekdays,
        monthdays=monthdays,
        months=months,
        timestr=timestr,
    )