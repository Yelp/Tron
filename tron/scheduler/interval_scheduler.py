from .common import get_jitter
from .common import get_jitter_str
from tron.utils import timeutils


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
