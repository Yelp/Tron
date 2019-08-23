# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

import mock

from testifycompat import assert_equal
from testifycompat import assert_raises
from testifycompat import run
from testifycompat import TestCase
from tron.config import config_utils
from tron.config import ConfigError
from tron.config import schedule_parse


class TestPadSequence(TestCase):
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


class TestScheduleConfigFromString(TestCase):
    @mock.patch(
        'tron.config.schedule_parse.parse_groc_expression',
        autospec=True,
    )
    def test_groc_config(self, mock_parse_groc):
        schedule = 'every Mon,Wed at 12:00'
        context = config_utils.NullConfigContext
        config = schedule_parse.schedule_config_from_string(schedule, context)
        assert_equal(config, mock_parse_groc.return_value)
        generic_config = schedule_parse.ConfigGenericSchedule(
            'groc daily',
            schedule,
            None,
        )
        mock_parse_groc.assert_called_with(generic_config, context)


class TestValidScheduler(TestCase):
    @mock.patch('tron.config.schedule_parse.schedulers', autospec=True)
    def assert_validation(self, schedule, expected, mock_schedulers):
        context = config_utils.NullConfigContext
        config = schedule_parse.valid_schedule(schedule, context)
        mock_schedulers.__getitem__.assert_called_with('cron')
        func = mock_schedulers.__getitem__.return_value
        assert_equal(config, func.return_value)
        func.assert_called_with(expected, context)

    def test_cron_from_dict(self):
        schedule = {'type': 'cron', 'value': '* * * * *'}
        config = schedule_parse.ConfigGenericSchedule(
            'cron',
            schedule['value'],
            datetime.timedelta(),
        )
        self.assert_validation(schedule, config)

    def test_cron_from_dict_with_jitter(self):
        schedule = {'type': 'cron', 'value': '* * * * *', 'jitter': '5 min'}
        config = schedule_parse.ConfigGenericSchedule(
            'cron',
            schedule['value'],
            datetime.timedelta(minutes=5),
        )
        self.assert_validation(schedule, config)


class TestValidCronScheduler(TestCase):
    _suites = ['integration']

    def validate(self, line):
        config = schedule_parse.ConfigGenericSchedule('cron', line, None)
        context = config_utils.NullConfigContext
        return schedule_parse.valid_cron_scheduler(config, context)

    def test_valid_config(self):
        config = self.validate('5 0 L * *')
        assert_equal(config.minutes, [5])
        assert_equal(config.months, None)
        assert_equal(config.monthdays, ['LAST'])

    def test_invalid_config(self):
        assert_raises(ConfigError, self.validate, '* * *')


class TestValidDailyScheduler(TestCase):
    def validate(self, config):
        context = config_utils.NullConfigContext
        config = schedule_parse.ConfigGenericSchedule('daily', config, None)
        return schedule_parse.valid_daily_scheduler(config, context)

    def assert_parse(self, config, expected):
        config = self.validate(config)
        expected = schedule_parse.ConfigDailyScheduler(*expected, jitter=None)
        assert_equal(config, expected)

    def test_valid_daily_scheduler_start_time(self):
        expected = ('14:32 ', 14, 32, 0, set())
        self.assert_parse('14:32', expected)

    def test_valid_daily_scheduler_just_days(self):
        expected = ("00:00:00 MWS", 0, 0, 0, {1, 3, 6})
        self.assert_parse("00:00:00 MWS", expected)

    def test_valid_daily_scheduler_time_and_day(self):
        expected = ("17:02:44 SU", 17, 2, 44, {0, 6})
        self.assert_parse("17:02:44 SU", expected)

    def test_valid_daily_scheduler_invalid_start_time(self):
        assert_raises(ConfigError, self.validate, "5 MWF")
        assert_raises(ConfigError, self.validate, "05:30:45:45 MWF")
        assert_raises(ConfigError, self.validate, "25:30:45 MWF")

    def test_valid_daily_scheduler_invalid_days(self):
        assert_raises(ConfigError, self.validate, "SUG")
        assert_raises(ConfigError, self.validate, "3")


if __name__ == "__main__":
    run()
