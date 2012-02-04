import calendar
from collections import deque
import datetime
import logging
import re

from pytz import AmbiguousTimeError, NonExistentTimeError

from tron.utils import groctimespecification
from tron.utils import timeutils


log = logging.getLogger('tron.scheduler')


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


def groc_schedule_parser_re():
    """Build a regular expression that matches this:

        ("every"|ordinal) (days) ["of|in" (monthspec)] (["at"] HH:MM)

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
GROC_SCHEDULE_RE = groc_schedule_parser_re()


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

    def job_setup(self, job):
        job.constant = True
        job.queueing = False

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

    def job_setup(self, job):
        job.queueing = True

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

    def job_setup(self, job):
        job.queueing = False

    def __str__(self):
        return "INTERVAL:%s" % self.interval

    def __eq__(self, other):
        return (isinstance(other, IntervalScheduler) and
                self.interval == other.interval)

    def __ne__(self, other):
        return not self == other
