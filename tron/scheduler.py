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
import logging

from pytz import AmbiguousTimeError, NonExistentTimeError
from tron.config import schedule_parse

from tron.utils import trontimespec
from tron.utils import timeutils


log = logging.getLogger(__name__)


def scheduler_from_config(config, time_zone):
    """A factory for creating a scheduler from a configuration object."""
    if isinstance(config, schedule_parse.ConfigConstantScheduler):
        return ConstantScheduler()

    if isinstance(config, schedule_parse.ConfigIntervalScheduler):
        return IntervalScheduler(interval=config.timedelta)

    if isinstance(config, schedule_parse.ConfigGrocScheduler):
        return GeneralScheduler(
            time_zone=time_zone,
            timestr=config.timestr or '00:00',
            ordinals=config.ordinals,
            monthdays=config.monthdays,
            months=config.months,
            weekdays=config.weekdays,
            string_repr='GROC %s' % config.original)

    if isinstance(config, schedule_parse.ConfigCronScheduler):
        return GeneralScheduler(
            minutes=config.minutes,
            hours=config.hours,
            monthdays=config.monthdays,
            months=config.months,
            weekdays=config.weekdays,
            ordinals=config.ordinals,
            seconds=[0],
            string_repr='CRON %s' % config.original)

    if isinstance(config, schedule_parse.ConfigDailyScheduler):
        return GeneralScheduler(
            hours=[config.hour],
            minutes=[config.minute],
            seconds=[config.second],
            weekdays=config.days,
            string_repr='DAILY %s' % config.original)


class ConstantScheduler(object):
    """The constant scheduler schedules a new job immediately."""
    schedule_on_complete = True

    def next_run_time(self, _):
        return timeutils.current_time()

    def __str__(self):
        return "CONSTANT"

    def __eq__(self, other):
        return isinstance(other, ConstantScheduler)

    def __ne__(self, other):
        return not self == other


class GeneralScheduler(object):
    """Scheduler which uses a TimeSpecification.
    """
    schedule_on_complete = False

    def __init__(self,
            ordinals=None,
            weekdays=None,
            months=None,
            monthdays=None,
            timestr=None,
            minutes=None,
            hours=None,
            seconds=None,
            time_zone=None,
            string_repr=None):
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
          string_repr - Original string representation this was parsed from,
                        if applicable
        """
        self.time_zone      = time_zone
        self.string_repr    = string_repr or "DAILY"
        self.time_spec      = trontimespec.TimeSpecification(
            ordinals=ordinals,
            weekdays=weekdays,
            months=months,
            monthdays=monthdays,
            timestr=timestr,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            timezone=time_zone.zone if time_zone else None)

    def next_run_time(self, start_time):
        """Find the next time to run."""
        if not start_time:
            start_time = timeutils.current_time()
        elif self.time_zone:
            try:
                start_time = self.time_zone.localize(start_time, is_dst=None)
            except AmbiguousTimeError:
                # We are in the infamous 1 AM block which happens twice on
                # fall-back. Pretend like it's the first time, every time.
                start_time = self.time_zone.localize(start_time, is_dst=True)
            except NonExistentTimeError:
                # We are in the infamous 2:xx AM block which does not
                # exist. Pretend like it's the later time, every time.
                start_time = self.time_zone.localize(start_time, is_dst=True)

        return self.time_spec.get_match(start_time)

    def __str__(self):
        return self.string_repr

    def __eq__(self, other):
        return hasattr(other, 'time_spec') and self.time_spec == other.time_spec

    def __ne__(self, other):
        return not self == other


class IntervalScheduler(object):
    """The interval scheduler runs a job (to success) based on a configured
    interval.
    """
    schedule_on_complete = False

    def __init__(self, interval=None):
        self.interval = interval

    def next_run_time(self, last_run_time):
        last_run_time = last_run_time or timeutils.current_time()
        return last_run_time + self.interval

    def __str__(self):
        return "INTERVAL %s" % self.interval

    def __eq__(self, other):
        return (isinstance(other, IntervalScheduler) and
                self.interval == other.interval)

    def __ne__(self, other):
        return not self == other
