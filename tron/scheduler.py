"""
Tron schedulers

 A scheduler has a simple interface.

 class Scheduler(object):

    schedule_on_complete = <bool>

    def next_run_time(self, last_run_time):
        <returns datetime>


 next_run_time() should return a datetime which is the time the next job run
 will be run.

 schedule_on_complete is a bool that identifies if this scheduler should have
 jobs scheduled with the start_time of the previous run (False), or the
 end time of the previous run (False).
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import logging
import random

from pytz import AmbiguousTimeError
from pytz import NonExistentTimeError

from tron.config import schedule_parse
from tron.utils import timeutils
from tron.utils import trontimespec

log = logging.getLogger(__name__)


def scheduler_from_config(config, time_zone):
    """A factory for creating a scheduler from a configuration object."""
    if isinstance(config, schedule_parse.ConfigConstantScheduler):
        return ConstantScheduler()

    if isinstance(config, schedule_parse.ConfigIntervalScheduler):
        return IntervalScheduler(
            interval=config.timedelta,
            jitter=config.jitter,
            time_zone=time_zone,
        )

    if isinstance(config, schedule_parse.ConfigGrocScheduler):
        return GeneralScheduler(
            time_zone=time_zone,
            timestr=config.timestr or '00:00',
            ordinals=config.ordinals,
            monthdays=config.monthdays,
            months=config.months,
            weekdays=config.weekdays,
            name='groc',
            original=config.original,
            jitter=config.jitter,
        )

    if isinstance(config, schedule_parse.ConfigCronScheduler):
        return GeneralScheduler(
            time_zone=time_zone,
            minutes=config.minutes,
            hours=config.hours,
            monthdays=config.monthdays,
            months=config.months,
            weekdays=config.weekdays,
            ordinals=config.ordinals,
            seconds=[0],
            name='cron',
            original=config.original,
            jitter=config.jitter,
        )

    if isinstance(config, schedule_parse.ConfigDailyScheduler):
        return GeneralScheduler(
            hours=[config.hour],
            time_zone=time_zone,
            minutes=[config.minute],
            seconds=[config.second],
            weekdays=config.days,
            name='daily',
            original=config.original,
            jitter=config.jitter,
        )


class ConstantScheduler(object):
    """The constant scheduler schedules a new job immediately."""
    schedule_on_complete = True

    def next_run_time(self, _):
        return timeutils.current_time()

    def __str__(self):
        return self.get_name()

    def __eq__(self, other):
        return isinstance(other, ConstantScheduler)

    def __ne__(self, other):
        return not self == other

    def get_jitter(self):
        pass

    def get_name(self):
        return 'constant'

    def get_value(self):
        return ''


def get_jitter(time_delta):
    if not time_delta:
        return datetime.timedelta()
    seconds = timeutils.delta_total_seconds(time_delta)
    return datetime.timedelta(seconds=random.randint(-seconds, seconds))


def get_jitter_str(time_delta):
    if not time_delta:
        return ''
    return ' (+/- %s)' % time_delta


class GeneralScheduler(object):
    """Scheduler which uses a TimeSpecification.
    """
    schedule_on_complete = False

    def __init__(
        self,
        ordinals=None,
        weekdays=None,
        months=None,
        monthdays=None,
        timestr=None,
        minutes=None,
        hours=None,
        seconds=None,
        time_zone=None,
        name=None,
        original=None,
        jitter=None,
    ):
        """Parameters:
          timestr     - the time of day to run, as 'HH:MM'
          ordinals    - first, second, third &c, as a set of integers in 1..5 to
                        be used with "1st <weekday>", etc.
          monthdays   - set of integers to be used with "<month> 3rd", etc.
          months      - the months that this should run, as a set of integers in
                        1..12
          weekdays    - the days of the week that this should run, as a set of
                        integers, 0=Sunday, 6=Saturday
          timezone    - the optional timezone as a string for this specification.
                        Defaults to UTC - valid entries are things like
                        Australia/Victoria or PST8PDT.
        """
        self.time_zone = time_zone
        self.jitter = jitter
        self.name = name or 'daily'
        self.original = original or ''
        self.time_spec = trontimespec.TimeSpecification(
            ordinals=ordinals,
            weekdays=weekdays,
            months=months,
            monthdays=monthdays,
            timestr=timestr,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            timezone=time_zone.zone if time_zone else None,
        )

    def next_run_time(self, start_time):
        """Find the next time to run."""
        if not start_time:
            start_time = timeutils.current_time(tz=self.time_zone)
        elif self.time_zone:
            if start_time.tzinfo is None or start_time.tzinfo.utcoffset(
                start_time,
            ) is None:
                # tz-naive start times need to be localized first to the requested
                # time zone.
                try:
                    start_time = self.time_zone.localize(
                        start_time,
                        is_dst=None,
                    )
                except AmbiguousTimeError:
                    # We are in the infamous 1 AM block which happens twice on
                    # fall-back. Pretend like it's the first time, every time.
                    start_time = self.time_zone.localize(
                        start_time,
                        is_dst=True,
                    )
                except NonExistentTimeError:
                    # We are in the infamous 2:xx AM block which does not
                    # exist. Pretend like it's the later time, every time.
                    start_time = self.time_zone.localize(
                        start_time,
                        is_dst=True,
                    )

        return self.time_spec.get_match(start_time) + get_jitter(self.jitter)

    def __str__(self):
        return '%s %s%s' % (
            self.name,
            self.original,
            get_jitter_str(self.jitter),
        )

    def __eq__(self, other):
        return hasattr(
            other,
            'time_spec',
        ) and self.time_spec == other.time_spec

    def __ne__(self, other):
        return not self == other

    def get_jitter(self):
        return self.jitter

    def get_name(self):
        return self.name

    def get_value(self):
        return self.original


class IntervalScheduler(object):
    """The interval scheduler runs a job (to success) based on a configured
    interval.
    """
    schedule_on_complete = False

    def __init__(self, interval, jitter, time_zone):
        self.interval = interval
        self.jitter = jitter
        self.time_zone = time_zone

    def next_run_time(self, last_run_time, time_zone=None):
        last_run_time = last_run_time or timeutils.current_time(tz=time_zone)
        return last_run_time + self.interval + get_jitter(self.jitter)

    def __str__(self):
        return "%s %s%s" % (
            self.get_name(),
            self.interval,
            get_jitter_str(self.jitter),
        )

    def __eq__(self, other):
        return (
            isinstance(other, IntervalScheduler) and
            self.interval == other.interval
        )

    def __ne__(self, other):
        return not self == other

    def get_jitter(self):
        return self.jitter

    def get_name(self):
        return "interval"

    def get_value(self):
        return str(self.interval)
