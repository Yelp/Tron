"""
Parse and validate scheduler configuration and return immutable structures.
"""
import calendar
from collections import namedtuple
import datetime
import re

from tron.config import ConfigError, config_utils, schema
from tron.utils import crontab, dicts


ConfigGenericSchedule = schema.config_object_factory(
    'ConfigGenericSchedule',
    ['type', 'value'],
    ['jitter']
)

ConfigGrocScheduler = namedtuple('ConfigGrocScheduler',
    'original ordinals weekdays monthdays months timestr jitter')

ConfigCronScheduler = namedtuple('ConfigCronScheduler',
    'original minutes hours monthdays months weekdays ordinals jitter')

ConfigDailyScheduler    = namedtuple('ConfigDailyScheduler',
    'original hour minute second days jitter')

ConfigConstantScheduler = namedtuple('ConfigConstantScheduler', [])

ConfigIntervalScheduler = namedtuple('ConfigIntervalScheduler',
    'timedelta jitter')


class ScheduleParseError(ConfigError):
    pass


def pad_sequence(seq, size, padding=None):
    """Force a sequence to size. Pad with padding if too short, and ignore
    extra pieces if too long."""
    return (list(seq) + [padding for _ in xrange(size)])[:size]


def schedule_config_from_string(schedule, config_context):
    """Return a scheduler config object from a string."""
    schedule = schedule.strip()
    name, schedule_config = pad_sequence(schedule.split(None, 1), 2, padding='')
    if name not in schedulers:
        config = ConfigGenericSchedule('groc daily', schedule, jitter=None)
        return parse_groc_expression(config, config_context)

    config = ConfigGenericSchedule(name, schedule_config, jitter=None)
    return validate_generic_schedule_config(config, config_context)


def validate_generic_schedule_config(config, config_context):
    return schedulers[config.type](config, config_context)


# TODO: remove in 0.7
def schedule_config_from_legacy_dict(schedule, config_context):
    """Support old style schedules as dicts."""
    if 'interval' in schedule:
        config = ConfigGenericSchedule('interval', schedule['interval'], None)
        return valid_interval_scheduler(config, config_context)

    if 'start_time' in schedule or 'days' in schedule:
        start_time = schedule.get('start_time', '00:00:00')
        days = schedule.get('days', '')
        scheduler_config = '%s %s' % (start_time, days)
        config = ConfigGenericSchedule('daily', scheduler_config, None)
        return valid_daily_scheduler(config, config_context)

    path = config_context.path
    raise ConfigError("Unknown scheduler at %s: %s" % (path, schedule))


def valid_schedule(schedule, config_context):
    if isinstance(schedule, basestring):
        return schedule_config_from_string(schedule, config_context)

    if 'type' not in schedule:
        return schedule_config_from_legacy_dict(schedule, config_context)

    schedule = ScheduleValidator().validate(schedule, config_context)
    return validate_generic_schedule_config(schedule, config_context)


def valid_constant_scheduler(_config, _context):
    """Adapter for validation interface and constant scheduler."""
    return ConfigConstantScheduler()


def valid_daily_scheduler(config, config_context):
    """Daily scheduler, accepts a time of day and an optional list of days."""
    schedule_config = config.value
    time_string, days = pad_sequence(schedule_config.split(), 2)
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
        time_spec.hour, time_spec.minute, time_spec.second, weekdays,
        jitter=config.jitter)


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
TIME_INTERVAL_RE = re.compile(r"^(?P<value>\d+)(?P<units>[a-zA-Z]+)$")

TimeInterval = namedtuple('TimeInterval', 'value units')


def valid_time_interval(interval, error_msg, config_context):
    interval = ''.join(interval.split())
    matches = TIME_INTERVAL_RE.match(interval)
    if not matches:
        raise ConfigError(error_msg % (config_context.path, interval))

    units = matches.group('units')
    if units not in TIME_INTERVAL_UNITS:
        raise ConfigError(error_msg % (config_context.path, interval))

    return TimeInterval(int(matches.group('value')), TIME_INTERVAL_UNITS[units])


def timedelta_from_time_interval(time_interval):
    return datetime.timedelta(**{time_interval.units: time_interval.value})


def valid_jitter(jitter, config_context):
    """Return jitter in seconds."""
    if not jitter:
        return

    error_msg   = 'Invalid jitter specification at %s: %s'
    time_interval = valid_time_interval(jitter, error_msg, config_context)
    if time_interval.units not in ('seconds', 'minutes'):
        msg = "Invalid time unit for jitter at %s: %s"
        raise ConfigError(msg % (config_context.path, time_interval.units))
    return timedelta_from_time_interval(time_interval)


# Shortcut values for intervals
TIME_INTERVAL_SHORTCUTS = {
    'hourly': TimeInterval(value=1, units='hours')
}

def valid_interval_scheduler(config,  config_context):
    interval  = config.value
    error_msg = 'Invalid interval specification at %s: %s'

    def build_config(time_interval):
        delta = timedelta_from_time_interval(time_interval)
        return ConfigIntervalScheduler(timedelta=delta, jitter=config.jitter)

    interval_key = interval.strip()
    if interval_key in TIME_INTERVAL_SHORTCUTS:
        return build_config(TIME_INTERVAL_SHORTCUTS[interval_key])

    time_interval = valid_time_interval(interval, error_msg, config_context)
    return build_config(time_interval)


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

def parse_groc_expression(config, config_context):
    """Given an expression of the form in the docstring of
    daily_schedule_parser_re(), return the parsed values in a
    ConfigGrocScheduler
    """
    expression = config.value
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
        timestr=timestr,
        jitter=config.jitter)


def valid_cron_scheduler(config, config_context):
    """Parse a cron schedule."""
    try:
        crontab_kwargs = crontab.parse_crontab(config.value)
        return ConfigCronScheduler(
            original=config.value, jitter=config.jitter, **crontab_kwargs)
    except ValueError, e:
        msg = "Invalid cron scheduler %s: %s"
        raise ConfigError(msg % (config_context.path, e))


schedulers = {
    'constant':     valid_constant_scheduler,
    'daily':        valid_daily_scheduler,
    'interval':     valid_interval_scheduler,
    'cron':         valid_cron_scheduler,
    'groc daily':   parse_groc_expression,
}


class ScheduleValidator(config_utils.Validator):
    """Validate the structure of a scheduler config."""
    config_class = ConfigGenericSchedule
    defaults = {
        'jitter':       None,
        }
    validators = {
        'type':         config_utils.build_enum_validator(schedulers.keys()),
        'jitter':       valid_jitter
    }
