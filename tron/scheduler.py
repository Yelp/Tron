"""
Tron schedulers

 A scheduler has a simple interface.

 class Scheduler(object):

    queue_overlapping = <bool>

    def next_run_time(self, last_run_time):
        <returns datetime>



 next_run_time() should return a datetime which is the time the next job run
 will be run.

 queue_overlapping should be False if this scheduler should stop queueing
 runs once there is a queued or scheduled run already waiting.  It should
 be True if jobs should always be queued regardless of the current
 state of other jobs.

"""
import logging

from pytz import AmbiguousTimeError, NonExistentTimeError
from tron.config import schedule_parse

from tron.utils import groctimespecification
from tron.utils import timeutils


log = logging.getLogger('tron.scheduler')


def scheduler_from_config(config, time_zone):
    """A factory for creating a scheduler from a configuration object."""
    if isinstance(config, schedule_parse.ConfigConstantScheduler):
        return ConstantScheduler()

    if isinstance(config, schedule_parse.ConfigIntervalScheduler):
        return IntervalScheduler(interval=config.timedelta)

    if isinstance(config, schedule_parse.ConfigDailyScheduler):
        return DailyScheduler(
            time_zone=time_zone,
            timestr=config.timestr,
            ordinals=config.ordinals,
            monthdays=config.monthdays,
            months=config.months,
            weekdays=config.weekdays
        )


class ConstantScheduler(object):
    """The constant scheduler schedules a new job immediately."""

    queue_overlapping = False

    def next_run_time(self, _):
        return timeutils.current_time()

    def __str__(self):
        return "CONSTANT"

    def __eq__(self, other):
        return isinstance(other, ConstantScheduler)

    def __ne__(self, other):
        return not self == other


class DailyScheduler(object):
    """Wrapper around SpecificTimeSpecification in the Google App Engine cron
    library
    """

    queue_overlapping = True

    def __init__(self, ordinals=None, weekdays=None, months=None,
                 monthdays=None, timestr=None, time_zone=None,
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
        self.ordinals = ordinals
        self.weekdays = weekdays
        self.months = months
        self.monthdays = monthdays
        self.timestr = timestr or '00:00'
        self.time_zone = time_zone
        self.string_repr = string_repr

        self._time_spec = None

    @property
    def time_spec(self):
        if self._time_spec is None:
            # calendar module has 0=Monday
            # groc has 0=Sunday
            if self.weekdays:
                groc_weekdays = set((x + 1) % 7 for x in self.weekdays)
            else:
                groc_weekdays = None
            self._time_spec = groctimespecification.SpecificTimeSpecification(
                ordinals=self.ordinals,
                weekdays=groc_weekdays,
                months=self.months,
                monthdays=self.monthdays,
                timestr=self.timestr,
                timezone=self.time_zone.zone if self.time_zone else None,
            )
        return self._time_spec

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

        return self.time_spec.GetMatch(start_time)

    def __str__(self):
        # Backward compatible string representation which also happens to be
        # user-friendly
        if self.string_repr is None:
            return 'DAILY'
        else:
            return self.string_repr

    def __eq__(self, other):
        return isinstance(other, DailyScheduler) and \
           all(getattr(self, attr) == getattr(other, attr)
               for attr in ('ordinals',
                            'weekdays',
                            'months',
                            'monthdays',
                            'timestr',
                            'time_zone'))

    def __ne__(self, other):
        return not self == other


class IntervalScheduler(object):
    """The interval scheduler runs a job (to success) based on a configured
    interval.
    """

    queue_overlapping = False

    def __init__(self, interval=None):
        self.interval = interval

    def next_run_time(self, last_run_time):
        last_run_time = last_run_time or timeutils.current_time()
        return last_run_time + self.interval

    def __str__(self):
        return "INTERVAL:%s" % self.interval

    def __eq__(self, other):
        return (isinstance(other, IntervalScheduler) and
                self.interval == other.interval)

    def __ne__(self, other):
        return not self == other
