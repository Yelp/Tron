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
from .constant_scheduler import ConstantScheduler
from .general_scheduler import GeneralScheduler
from .interval_scheduler import IntervalScheduler
from tron.config import schedule_parse


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
