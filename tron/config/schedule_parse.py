"""
Parse and validate scheduler configuration and return immutable structures.
"""
import calendar
from collections import namedtuple
import datetime
import re

from tron.config import ConfigError, config_utils
from tron.utils import crontab, dicts


ConfigGrocScheduler = namedtuple('ConfigGrocScheduler',
    'original ordinals weekdays monthdays months timestr')

ConfigCronScheduler = namedtuple('ConfigCronScheduler',
    'original minutes hours monthdays months weekdays ordinals')

ConfigDailyScheduler    = namedtuple('ConfigDailyScheduler',
    'original hour minute second days')

ConfigConstantScheduler = namedtuple('ConfigConstantScheduler', [])
ConfigIntervalScheduler = namedtuple('ConfigIntervalScheduler', 'timedelta')


class ScheduleParseError(ConfigError):
    pass


def pad_sequence(seq, size, padding=None):
    """Force a sequence to size. Pad with None if too short, and ignore
    extra pieces if too long."""
    return (list(seq) + [padding] * size)[:size]


def valid_schedule(schedule, config_context):
    if isinstance(schedule, basestring):
        schedule = schedule.strip()
        scheduler_args = schedule.split()
        scheduler_name = scheduler_args.pop(0).lower()

        if schedule == 'constant':
            return ConfigConstantScheduler()
        elif scheduler_name == 'daily':
            start_time, days = pad_sequence(scheduler_args, 2)
            return valid_daily_scheduler(start_time, days, config_context)
        elif scheduler_name == 'interval':
            scheduler_config = ' '.join(scheduler_args)
            return valid_interval_scheduler(scheduler_config, config_context)
        elif scheduler_name == 'cron':
            return valid_cron_scheduler(scheduler_args, config_context)
        else:
            return parse_groc_expression(schedule, config_context)

    if 'interval' in schedule:
        return valid_interval_scheduler(schedule['interval'], config_context)
    elif 'start_time' in schedule or 'days' in schedule:
        start_time, days = schedule.get('start_time'), schedule.get('days')
        return valid_daily_scheduler(start_time, days, config_context)
    else:
        path = config_context.path
        raise ConfigError("Unknown scheduler at %s: %s" % (path, schedule))


def valid_daily_scheduler(time_string, days, config_context):
    """Daily scheduler, accepts a time of day and an optional list of days."""
    time_string = time_string or '00:00:00'
    time_spec   = config_utils.valid_time(time_string, config_context)
    days        = config_utils.valid_string(days or "", config_context)

    def valid_day(day):
        if day not in CONVERT_DAYS_INT:
            raise ConfigError("Unknown day %s at %s" % (day, config_context.path))
        return CONVERT_DAYS_INT[day]

    original = "%s %s" % (time_string, days)
    weekdays = set(valid_day(day) for day in days or ())
    return ConfigDailyScheduler(original,
        time_spec.hour, time_spec.minute, time_spec.second, weekdays)


# Shortcut values for intervals
TIME_INTERVAL_SHORTCUTS = {
    'hourly': dict(hours=1),
}

# Translations from possible configuration units to the argument to
# datetime.timedelta
TIME_INTERVAL_UNITS = dicts.invert_dict_list({
    'months':   ['mo', 'month', 'months'],
    'days':     ['d', 'day', 'days'],
    'hours':    ['h', 'hr', 'hrs', 'hour', 'hours'],
    'minutes':  ['m', 'min', 'mins', 'minute', 'minutes'],
    'seconds':  ['s', 'sec', 'secs', 'second', 'seconds']
})

# Split digits and characters into tokens
TIME_INTERVAL_RE = re.compile(r"\d+|[a-zA-Z]+")


def valid_interval_scheduler(interval,  config_context):
    interval    = ''.join(interval.split())
    error_msg   = 'Invalid interval specification at %s: %s'

    def build_config(spec):
        return ConfigIntervalScheduler(timedelta=datetime.timedelta(**spec))

    if interval in TIME_INTERVAL_SHORTCUTS:
        return build_config(TIME_INTERVAL_SHORTCUTS[interval])

    interval_tokens = TIME_INTERVAL_RE.findall(interval)
    if len(interval_tokens) != 2:
        raise ConfigError(error_msg % (config_context.path, interval))

    value, units = interval_tokens
    if units not in TIME_INTERVAL_UNITS:
        raise ConfigError(error_msg % (config_context.path, interval))

    return build_config({TIME_INTERVAL_UNITS[units]: int(value)})


def normalize_weekdays(seq):
    return seq[6:7] + seq[:6]

def day_canonicalization_map():
    """Build a map of weekday synonym to int index 0-6 inclusive."""
    canon_map = dict()

    # 7-element lists with weekday names in order
    weekday_lists = [
        normalize_weekdays(calendar.day_name),
        normalize_weekdays(calendar.day_abbr),
        ('u', 'm', 't', 'w', 'r', 'f', 's',),
        ('su', 'mo', 'tu', 'we', 'th', 'fr', 'sa',)]
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


def build_groc_schedule_parser_re():
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
DAILY_SCHEDULE_RE = build_groc_schedule_parser_re()


def _parse_number(day):
    return int(''.join(c for c in day if c.isdigit()))

def parse_groc_expression(expression, config_context):
    """Given an expression of the form in the docstring of
    daily_schedule_parser_re(), return the parsed values in a
    ConfigGrocScheduler
    """
    m = DAILY_SCHEDULE_RE.match(expression.lower())
    if not m:
        msg = 'Schedule at %s is not a valid expression: %s'
        raise ScheduleParseError(msg % (config_context.path, expression))

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

    return ConfigGrocScheduler(
        original=expression,
        ordinals=ordinals,
        weekdays=weekdays,
        monthdays=monthdays,
        months=months,
        timestr=timestr)


def valid_cron_scheduler(scheduler_args, config_context):
    """Parse a cron schedule."""
    try:
        crontab_kwargs = crontab.parse_crontab(' '.join(scheduler_args))
        return ConfigCronScheduler(original=scheduler_args, **crontab_kwargs)
    except ValueError, e:
        msg = "Invalid cron scheduler %s: %s"
        raise ConfigError(msg % (config_context.path, e))
