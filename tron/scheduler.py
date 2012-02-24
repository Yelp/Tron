import calendar
import datetime
import logging
import re

from pytz import AmbiguousTimeError, NonExistentTimeError

from tron.utils import groctimespecification
from tron.utils import timeutils


log = logging.getLogger('tron.scheduler')


class ConstantScheduler(object):
    """The constant scheduler schedules the first job run. The next job run
    is scheduled when this first run is finished.
    """

    def __init__(self, *args, **kwargs):
        super(ConstantScheduler, self).__init__(*args, **kwargs)
        self.time_zone = None

    def next_runs(self, job):
        if job.next_to_finish():
            return []

        job_runs = job.build_runs()
        for job_run in job_runs:
            job_run.set_run_time(timeutils.current_time())

        return job_runs

    def __str__(self):
        return "CONSTANT"

    def __eq__(self, other):
        return isinstance(other, ConstantScheduler)

    def __ne__(self, other):
        return not self == other


class GrocScheduler(object):
    """Wrapper around SpecificTimeSpecification in the Google App Engine cron
    library
    """

    def __init__(self, ordinals=None, weekdays=None, months=None,
                 monthdays=None, timestr=None, time_zone=None,
                 start_time=None):
        """Parameters:
          timestr   - the time of day to run, as 'HH:MM'
          ordinals  - first, second, third &c, as a set of integers in 1..5 to
                      be used with "1st <weekday>", etc.
          monthdays - set of integers to be used with "<month> 3rd", etc.
          months    - the months that this should run, as a set of integers in
                      1..12
          weekdays  - the days of the week that this should run, as a set of
                      integers, 0=Sunday, 6=Saturday
          timezone  - the optional timezone as a string for this specification.
                      Defaults to UTC - valid entries are things like
                      Australia/Victoria or PST8PDT.
          start_time - Backward-compatible parameter for DailyScheduler
        """
        self.ordinals = ordinals
        self.weekdays = weekdays
        self.months = months
        self.monthdays = monthdays

        self.timestr = None
        if timestr is None:
            if start_time:
                # This is a fancy property
                self.start_time = start_time
            else:
                self.timestr = "00:00"
        else:
            self.timestr = timestr

        self.time_zone = time_zone
        self.string_repr = 'every day of month'

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
                timezone=self.time_zone.zone if self.time_zone else None)
        return self._time_spec

    def parse(self, scheduler_str):
        """Parse a schedule string."""
        self.string_repr = scheduler_str

        def parse_number(day):
            return int(''.join(c for c in day if c.isdigit()))

        m = GROC_SCHEDULE_RE.match(scheduler_str.lower())

        if m.group('time') is not None:
            self.timestr = m.group('time')

        if m.group('days') in (None, 'day'):
            self.weekdays = None
        else:
            self.weekdays = set(CONVERT_DAYS_INT[d]
                                for d in m.group('days').split(','))

        self.monthdays = None
        self.ordinals = None
        if m.group('month_days') != 'every':
            values = set(parse_number(n)
                         for n in m.group('month_days').split(','))
            if self.weekdays is None:
                self.monthdays = values
            else:
                self.ordinals = values

        if m.group('months') in (None, 'month'):
            self.months = None
        else:
            self.months = set(CONVERT_MONTHS[mo]
                              for mo in m.group('months').split(','))

    def parse_legacy_days(self, days):
        """Parse a string that would have been passed to DailyScheduler"""
        self.weekdays = set(CONVERT_DAYS_INT[d] for d in days)
        if self.weekdays != set([0, 1, 2, 3, 4, 5, 6]):
            self.string_repr = 'every %s of month' % ','.join(days)

    def get_daily_waits(self, days):
        """Backwards compatibility with DailyScheduler"""
        self.parse_legacy_days(days)

    def _get_start_time(self):
        hms = [int(val) for val in self.timestr.strip().split(':')]
        while len(hms) < 3:
            hms.append(0)
        hour, minute, second = hms
        return datetime.time(hour=hour, minute=minute, second=second)

    def _set_start_time(self, start_time):
        self.timestr = "%.2d:%.2d" % (start_time.hour, start_time.minute)

    start_time = property(_get_start_time, _set_start_time)

    def next_runs(self, job):
        # Find the next time to run
        if job.runs:
            start_time = job.runs[0].run_time
        else:
            start_time = timeutils.current_time()
            if self.time_zone:
                try:
                    start_time = self.time_zone.localize(start_time,
                                                         is_dst=None)
                except AmbiguousTimeError:
                    # We are in the infamous 1 AM block which happens twice on
                    # fall-back. Pretend like it's the first time, every time.
                    start_time = self.time_zone.localize(start_time,
                                                         is_dst=True)
                except NonExistentTimeError:
                    # We are in the infamous 2:xx AM block which does not
                    # exist. Pretend like it's the later time, every time.
                    start_time = self.time_zone.localize(start_time,
                                                         is_dst=True)

        run_time = self.time_spec.GetMatch(start_time)

        job_runs = job.build_runs()
        for job_run in job_runs:
            job_run.set_run_time(run_time)

        return job_runs

    def __str__(self):
        # Backward compatible string representation which also happens to be
        # user-friendly
        if self.string_repr == 'every day of month':
            return 'DAILY'
        else:
            return self.string_repr

    def __eq__(self, other):
        return isinstance(other, GrocScheduler) and \
           all(getattr(self, attr) == getattr(other, attr)
               for attr in ('ordinals',
                            'weekdays',
                            'months',
                            'monthdays',
                            'timestr',
                            'time_zone'))

    def __ne__(self, other):
        return not self == other


# GrocScheduler can pretend to be a DailyScheduler in order to be backdward-
# compatible
DailyScheduler = GrocScheduler


class IntervalScheduler(object):
    """The interval scheduler runs a job (to success) based on a configured
    interval.
    """

    def __init__(self, interval=None):
        self.interval = interval
        self.time_zone = None

    def next_runs(self, job):
        run_time = timeutils.current_time() + self.interval

        job_runs = job.build_runs()
        for job_run in job_runs:
            job_run.set_run_time(run_time)

        return job_runs

    def __str__(self):
        return "INTERVAL:%s" % self.interval

    def __eq__(self, other):
        return (isinstance(other, IntervalScheduler) and
                self.interval == other.interval)

    def __ne__(self, other):
        return not self == other
