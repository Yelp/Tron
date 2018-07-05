from tron.utils import timeutils


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
