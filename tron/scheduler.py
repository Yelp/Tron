import logging

from pytz import AmbiguousTimeError, NonExistentTimeError
from tron.config import config_parse, schedule_parse

from tron.utils import groctimespecification
from tron.utils import timeutils


log = logging.getLogger('tron.scheduler')


def scheduler_from_config(config, time_zone):
    """A factory for creating a scheduler from a configuration object."""
    if isinstance(config, config_parse.ConfigConstantScheduler):
        return ConstantScheduler()

    if isinstance(config, config_parse.ConfigIntervalScheduler):
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


class DailyScheduler(object):
    """Wrapper around SpecificTimeSpecification in the Google App Engine cron
    library
    """

    def __init__(self, ordinals=None, weekdays=None, months=None,
                 monthdays=None, timestr=None, time_zone=None,
                 start_time=None, string_repr=None):
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
          start_time  - Backward-compatible parameter for DailyScheduler
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
                timezone=self.time_zone.zone if self.time_zone else None)
        return self._time_spec

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
