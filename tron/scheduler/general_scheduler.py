from pytz import AmbiguousTimeError
from pytz import NonExistentTimeError

from .common import get_jitter
from .common import get_jitter_str
from tron.utils import timeutils
from tron.utils import trontimespec


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
