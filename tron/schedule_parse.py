import calendar
from collections import namedtuple
import re


ConfigGrocDailyScheduler = namedtuple(
    'GrocDailySchedule',
    ['ordinals', 'weekdays', 'monthdays', 'months', 'timestr']
)


class ScheduleParseError(Exception):
    pass


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


def groc_daily_schedule_parser_re():
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

    GROC_SCHEDULE_EXPR = ''.join([
        r'^',
        MONTH_DAYS_EXPR, r'\s*',
        DAYS_EXPR, r'\s*',
        MONTHS_EXPR, r'\s*',
         TIME_EXPR, r'\s*',
        r'$'
    ])
    return re.compile(GROC_SCHEDULE_EXPR)


# Matches expressions of the form
# ``("every"|ordinal) (days) ["of|in" (monthspec)] (["at"] HH:MM)``.
# See :py:func:`groc_schedule_parser_re` for details.
GROC_DAILY_SCHEDULE_RE = groc_daily_schedule_parser_re()


def _parse_number(day):
    return int(''.join(c for c in day if c.isdigit()))

def parse_groc_daily_expression(expression):
    """Given an expression of the form in the docstring of
    groc_daily_schedule_parser_re(), return the parsed values in a
    ConfigGrocDailyScheduler
    """
    m = GROC_DAILY_SCHEDULE_RE.match(expression.lower())
    if not m:
        raise ScheduleParseError('Expression %r is not a valid scheduler expression.')

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

    return ConfigGrocDailyScheduler(
        ordinals=ordinals,
        weekdays=weekdays,
        monthdays=monthdays,
        months=months,
        timestr=timestr,
    )
