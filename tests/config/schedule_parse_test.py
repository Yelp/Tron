# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

from testify import assert_equal
from testify import assert_raises
from testify import run
from testify import TestCase

from tron.config import ConfigError
from tron.config import schedule_parse


class PadSequenceTestCase(TestCase):
    def test_pad_sequence_short(self):
        expected = [0, 1, 2, 3, None, None]
        assert_equal(schedule_parse.pad_sequence(range(4), 6), expected)

    def test_pad_sequence_long(self):
        expected = [0, 1, 2, 3]
        assert_equal(schedule_parse.pad_sequence(range(6), 4), expected)

    def test_pad_sequence_exact(self):
        expected = [0, 1, 2, 3]
        assert_equal(schedule_parse.pad_sequence(range(4), 4), expected)

    def test_pad_sequence_empty(self):
        expected = ["a", "a"]
        assert_equal(schedule_parse.pad_sequence([], 2, "a"), expected)

    def test_pad_negative_size(self):
        assert_equal(schedule_parse.pad_sequence([], -2, "a"), [])


class ScheduleConfigFromStringTestCase(TestCase):
    def test_groc_config(self):
        schedule = 'every Mon,Wed at 12:00'
        expected = schedule_parse.ConfigGrocScheduler(
            scheduler='groc daily',
            original='every Mon,Wed at 12:00',
            weekdays={1, 3},
            timestr='12:00',
        )
        config = schedule_parse.ConfigGenericSchedule.from_config(schedule)
        assert_equal(expected, config)

    def test_constant_config(self):
        schedule = 'constant'
        config = schedule_parse.ConfigGenericSchedule.from_config(schedule)
        assert_equal(config, schedule_parse.ConfigConstantScheduler(scheduler='constant', original=''))


class ValidSchedulerTestCase(TestCase):
    def test_cron_from_dict(self):
        schedule = {'type': 'cron', 'value': '* * * * *'}
        expected = schedule_parse.ConfigCronScheduler(
            scheduler='cron',
            original=schedule['value'],
        )
        assert_equal(
            expected,
            schedule_parse.ConfigGenericSchedule.from_config(schedule)
        )

    def test_cron_from_dict_with_jitter(self):
        schedule = {'type': 'cron', 'value': '* * * * *', 'jitter': '5 min'}
        expected = schedule_parse.ConfigCronScheduler(
            scheduler='cron',
            original=schedule['value'],
            jitter=datetime.timedelta(minutes=5),
        )
        assert_equal(
            expected,
            schedule_parse.ConfigGenericSchedule.from_config(schedule)
        )


class ValidCronSchedulerTestCase(TestCase):
    _suites = ['integration']

    def validate(self, line):
        return schedule_parse.ConfigGenericSchedule.from_config(
            dict(scheduler='cron', original=line)
        )

    def test_valid_config(self):
        config = self.validate('5 0 L * *')
        assert_equal(config.minutes, [5])
        assert_equal(config.months, None)
        assert_equal(config.monthdays, ['LAST'])

    def test_invalid_config(self):
        assert_raises(ConfigError, self.validate, '* * *')


class ValidDailySchedulerTestCase(TestCase):
    def assert_parse(self, config, expected):
        parsed = schedule_parse.ConfigGenericSchedule.from_config(
            f"daily {config}"
        )
        expected = schedule_parse.ConfigDailyScheduler(
            scheduler='daily', original=config, jitter=None, **expected
        )
        assert_equal(expected, parsed)

    def test_valid_daily_scheduler_start_time(self):
        expected = dict(hour=14, minute=32, second=0, days=set())
        self.assert_parse('14:32', expected)

    def test_valid_daily_scheduler_just_days(self):
        expected = dict(hour=0, minute=0, second=0, days={1, 3, 6})
        self.assert_parse("00:00:00 MWS", expected)

    def test_valid_daily_scheduler_time_and_day(self):
        expected = dict(hour=17, minute=2, second=44, days={0, 6})
        self.assert_parse("17:02:44 SU", expected)

    def test_valid_daily_scheduler_invalid_start_time(self):
        assert_raises(ValueError, self.assert_parse, "5 MWF", {})
        assert_raises(ValueError, self.assert_parse, "05:30:45:45 MWF", {})
        assert_raises(ValueError, self.assert_parse, "25:30:45 MWF", {})

    def test_valid_daily_scheduler_invalid_days(self):
        assert_raises(ValueError, self.assert_parse, "SUG", {})
        assert_raises(ValueError, self.assert_parse, "3", {})


class ValidIntervalSchedulerTestCase(TestCase):
    def validate(self, config_value):
        return schedule_parse.ConfigGenericSchedule.from_config(
            dict(
                scheduler='interval',
                original=config_value,
                jitter=None,
            )
        )

    def test_valid_interval_scheduler_shortcut(self):
        config = self.validate("hourly")
        expected = datetime.timedelta(hours=1)
        assert_equal(config.timedelta, expected)

    def test_valid_interval_scheduler_minutes(self):
        config = self.validate("5 minutes")
        expected = datetime.timedelta(minutes=5)
        assert_equal(config.timedelta, expected)

    def test_valid_interval_scheduler_hours(self):
        for spec in [
            '6h',
            '6 hours',
            '6 h',
            '6 hrs',
            '6 hour',
            '6 hours',
            '6hour',
        ]:
            config = self.validate(spec)
            expected = datetime.timedelta(hours=6)
            assert_equal(config.timedelta, expected)

    def test_valid_interval_scheduler_invalid_tokens(self):
        assert_raises(ConfigError, self.validate, "6 hours 22 minutes")

    def test_valid_interval_scheduler_unknown_unit(self):
        assert_raises(ConfigError, self.validate, "22 beats")

    def test_valid_interval_scheduler_bogus(self):
        assert_raises(ConfigError, self.validate, "asdasd.asd")

    def test_valid_interval_scheduler_underscore(self):
        assert_raises(ConfigError, self.validate, "6_minute")

    def test_valid_interval_scheduler_unicode(self):
        assert_raises(ConfigError, self.validate, "6 \x0e\x96minute")

    def test_valid_interval_scheduler_alias(self):
        config = self.validate("  hourly  ")
        expected = datetime.timedelta(hours=1)
        assert_equal(config.timedelta, expected)


if __name__ == "__main__":
    run()
