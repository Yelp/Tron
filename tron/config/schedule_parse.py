"""
Parse and validate scheduler configuration and return immutable structures.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import calendar
import datetime
import re

from pyrsistent import field

from tron.config import config_utils
from tron.config import ConfigRecord
from tron.utils import crontab


class ConfigGenericSchedule(ConfigRecord):
    scheduler = field(type=str, mandatory=True)
    original = field(type=str, mandatory=True)
    jitter = field(
        type=(datetime.timedelta, type(None)),
        initial=None,
        factory=config_utils.valid_time_delta
    )

    @staticmethod
    def from_config(config):
        if isinstance(config, ConfigGenericSchedule) and \
                type(config) is not ConfigGenericSchedule:
            return config

        if isinstance(config, str):
            config = config.strip()
            name, schedule_config = pad_sequence(
                config.split(None, 1),
                2,
                padding='',
            )
            config = dict(scheduler=name, original=schedule_config)
        elif isinstance(config, dict):
            if 'interval' in config:
                config = dict(
                    scheduler='interval',
                    original=config['interval'],
                    jitter=None
                )
            elif 'start_time' in config or 'days' in config:
                start_time = config.get('start_time', '00:00:00')
                days = config.get('days', '')
                scheduler_config = f'{start_time} {days}'
                config = dict(scheduler='daily', original=scheduler_config)

        if isinstance(config, dict):
            config.setdefault('scheduler', config.pop('type', None))
            config.setdefault('original', config.pop('value', None))
            if config['scheduler'] not in schedulers:
                config['original'
                       ] = f"{config['scheduler']} {config['original']}"
                config['scheduler'] = 'groc daily'

        config = ConfigGenericSchedule(**config)
        return schedulers[config.scheduler].from_config(config)


class ConfigGrocScheduler(ConfigGenericSchedule):
    ordinals = field(initial=None)
    weekdays = field(initial=None)
    monthdays = field(initial=None)
    months = field(initial=None)
    timestr = field(initial=None)

    @staticmethod
    def from_config(config):
        """Given an expression of the form in the docstring of
        daily_schedule_parser_re(), return the parsed values in a
        ConfigGrocScheduler
        """

        expression = config.original
        m = DAILY_SCHEDULE_RE.match(expression.lower())
        if not m:
            raise ValueError(
                f'Schedule is not a valid expression: {expression}'
            )

        timestr = m.group('time')
        if timestr is None:
            timestr = '00:00'

        if m.group('days') in (None, 'day'):
            weekdays = None
        else:
            weekdays = {
                CONVERT_DAYS_INT[d]
                for d in m.group('days').split(',')
            }

        monthdays = None
        ordinals = None

        if m.group('month_days') != 'every':
            values = {
                _parse_number(n)
                for n in m.group('month_days').split(',')
            }
            if weekdays is None:
                monthdays = values
            else:
                ordinals = values

        if m.group('months') in (None, 'month'):
            months = None
        else:
            months = {
                CONVERT_MONTHS[mo]
                for mo in m.group('months').split(',')
            }

        return ConfigGrocScheduler(
            scheduler=config.scheduler,
            original=config.original,
            jitter=config.jitter,
            ordinals=ordinals,
            weekdays=weekdays,
            monthdays=monthdays,
            months=months,
            timestr=timestr,
        )


class ConfigCronScheduler(ConfigGenericSchedule):
    minutes = field(initial=None)
    hours = field(initial=None)
    monthdays = field(initial=None)
    months = field(initial=None)
    weekdays = field(initial=None)
    ordinals = field(initial=None)

    @staticmethod
    def from_config(config):
        try:
            crontab_kwargs = crontab.parse_crontab(config.original)
            return ConfigCronScheduler(
                scheduler=config.scheduler,
                original=config.original,
                jitter=config.jitter,
                **crontab_kwargs
            )
        except ValueError as e:
            raise ValueError("Invalid cron scheduler: %s" % e)


class ConfigDailyScheduler(ConfigGenericSchedule):
    hour = field()
    minute = field()
    second = field()
    days = field()

    @staticmethod
    def from_config(config):
        schedule_config = config.original
        time_string, days = pad_sequence(schedule_config.split(), 2)
        time_string = time_string or '00:00:00'
        time_spec = config_utils.valid_time(time_string)

        def valid_day(day):
            if day not in CONVERT_DAYS_INT:
                raise ValueError("Unknown day %s" % day)
            return CONVERT_DAYS_INT[day]

        weekdays = {valid_day(day) for day in days or ()}
        return ConfigDailyScheduler(
            scheduler=config.scheduler,
            original=config.original,
            jitter=config.jitter,
            hour=time_spec.hour,
            minute=time_spec.minute,
            second=time_spec.second,
            days=weekdays,
        )


class ConfigConstantScheduler(ConfigGenericSchedule):
    @staticmethod
    def from_config(config):
        return ConfigConstantScheduler(
            scheduler=config.scheduler,
            original=config.original,
            jitter=config.jitter
        )


class ConfigIntervalScheduler(ConfigGenericSchedule):
    timedelta = field()

    @staticmethod
    def from_config(config):
        delta = config.original.strip()
        if delta in TIME_INTERVAL_SHORTCUTS:
            delta = TIME_INTERVAL_SHORTCUTS[delta]
        else:
            delta = config_utils.valid_time_delta(delta)

        return ConfigIntervalScheduler(
            scheduler=config.scheduler,
            original=config.original,
            jitter=config.jitter,
            timedelta=delta,
        )


def pad_sequence(seq, size, padding=None):
    """Force a sequence to size. Pad with padding if too short, and ignore
    extra pieces if too long."""
    return (list(seq) + [padding for _ in range(size)])[:size]


# Shortcut values for intervals
TIME_INTERVAL_SHORTCUTS = {
    'hourly': datetime.timedelta(hours=1),
}


def normalize_weekdays(seq):
    return seq[6:7] + seq[:6]


def day_canonicalization_map():
    """Build a map of weekday synonym to int index 0-6 inclusive."""
    canon_map = dict()

    # 7-element lists with weekday names in order
    weekday_lists = [
        normalize_weekdays(calendar.day_name),
        normalize_weekdays(calendar.day_abbr),
        (
            'u',
            'm',
            't',
            'w',
            'r',
            'f',
            's',
        ),
        (
            'su',
            'mo',
            'tu',
            'we',
            'th',
            'fr',
            'sa',
        ),
    ]
    for day_list in weekday_lists:
        for day_name_synonym, day_index in zip(day_list, range(7)):
            canon_map[day_name_synonym] = day_index
            canon_map[day_name_synonym.lower()] = day_index
            canon_map[day_name_synonym.upper()] = day_index

    return canon_map


# Canonicalize weekday names to integer indices
CONVERT_DAYS_INT = day_canonicalization_map()  # day name/abbrev => {0123456}


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
    DAY_VALUES = '|'.join(list(CONVERT_DAYS_INT.keys()) + ['day'])

    # jan|january|...|month
    MONTH_VALUES = '|'.join(list(CONVERT_MONTHS.keys()) + ['month'])

    DATE_SUFFIXES = 'st|nd|rd|th'

    # every|1st|2nd|3rd (also would accept 3nd, 1rd, 4st)
    MONTH_DAYS_EXPR = '(?P<month_days>every|((\d+(%s),?)+))?' % DATE_SUFFIXES
    DAYS_EXPR = r'((?P<days>((%s),?)+))?' % DAY_VALUES
    MONTHS_EXPR = r'((in|of)\s+(?P<months>((%s),?)+))?' % MONTH_VALUES

    # [at] 00:00
    TIME_EXPR = r'((at\s+)?(?P<time>\d\d:\d\d))?'

    DAILY_SCHEDULE_EXPR = ''.join([
        r'^',
        MONTH_DAYS_EXPR,
        r'\s*',
        DAYS_EXPR,
        r'\s*',
        MONTHS_EXPR,
        r'\s*',
        TIME_EXPR,
        r'\s*',
        r'$',
    ])
    return re.compile(DAILY_SCHEDULE_EXPR)


# Matches expressions of the form
# ``("every"|ordinal) (days) ["of|in" (monthspec)] (["at"] HH:MM)``.
# See :py:func:`daily_schedule_parser_re` for details.
DAILY_SCHEDULE_RE = build_groc_schedule_parser_re()


def _parse_number(day):
    return int(''.join(c for c in day if c.isdigit()))


schedulers = {
    'constant': ConfigConstantScheduler,
    'daily': ConfigDailyScheduler,
    'interval': ConfigIntervalScheduler,
    'cron': ConfigCronScheduler,
    'groc daily': ConfigGrocScheduler,
}
