# -*- coding: utf-8 -*-
import datetime
import mock
from testify import TestCase, run, assert_equal, assert_raises
from testify.test_case import setup

from tron.config import schedule_parse, ConfigError, config_utils


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

    @mock.patch('tron.config.schedule_parse.parse_groc_expression', autospec=True)
    def test_groc_config(self, mock_parse_groc):
        schedule = 'every Mon,Wed at 12:00'
        context = config_utils.NullConfigContext
        config = schedule_parse.schedule_config_from_string(schedule, context)
        assert_equal(config, mock_parse_groc.return_value)
        mock_parse_groc.assert_called_with(schedule, context)


class ValidSchedulerTestCase(TestCase):

    @mock.patch('tron.config.schedule_parse.schedulers', autospec=True)
    def test_cron_from_dict(self, mock_schedulers):
        schedule = {'type': 'cron', 'value': '* * * * *'}
        context = config_utils.NullConfigContext
        config = schedule_parse.valid_schedule(schedule, context)
        mock_schedulers.__getitem__.assert_called_with('cron')
        func = mock_schedulers.__getitem__.return_value
        assert_equal(config, func.return_value)
        func.assert_called_with(schedule['value'], context, jitter=None)

    @mock.patch('tron.config.schedule_parse.schedulers', autospec=True)
    def test_cron_from_dict_with_jitter(self, mock_schedulers):
        schedule = {'type': 'cron', 'value': '* * * * *', 'jitter': '5 min'}
        jitter = 5 * 60
        context = config_utils.NullConfigContext
        config = schedule_parse.valid_schedule(schedule, context)
        mock_schedulers.__getitem__.assert_called_with('cron')
        func = mock_schedulers.__getitem__.return_value
        assert_equal(config, func.return_value)
        func.assert_called_with(schedule['value'], context, jitter=jitter)


class ValidJitterTestCase(TestCase):

    def test_valid_jitter_none(self):
        context = config_utils.NullConfigContext
        assert_equal(None, schedule_parse.valid_jitter(None, context))

    def test_valid_jitter_seconds(self):
        context = config_utils.NullConfigContext
        for jitter in ['82s', '82 s', '82 sec', '82seconds']:
            assert_equal(82, schedule_parse.valid_jitter(jitter, context))

    def test_valid_jitter_minutes(self):
        context = config_utils.NullConfigContext
        for jitter in ['10m', '10 m', '10 min', '10minutes']:
            assert_equal(600, schedule_parse.valid_jitter(jitter, context))

    def test_valid_jitter_invalid_unit(self):
        context = config_utils.NullConfigContext
        for jitter in ['1d', '3 mo', '3 months']:
            assert_raises(ConfigError,
                schedule_parse.valid_jitter,jitter, context)


class ValidCronSchedulerTestCase(TestCase):
    _suites = ['integration']

    @setup
    def setup_context(self):
        self.context = config_utils.NullConfigContext

    def test_valid_config(self):
        line = '5 0 L * *'
        config = schedule_parse.valid_cron_scheduler(line, self.context)
        assert_equal(config.minutes, [5])
        assert_equal(config.months, None)
        assert_equal(config.monthdays, ['LAST'])

    def test_invalid_config(self):
        assert_raises(ConfigError,
            schedule_parse.valid_cron_scheduler, '* * *', self.context)


class ValidDailySchedulerTestCase(TestCase):

    @setup
    def setup_context(self):
        self.context = config_utils.NullConfigContext

    def assert_parse(self, config, expected):
        config = schedule_parse.valid_daily_scheduler(*config)
        expected = schedule_parse.ConfigDailyScheduler(*expected, jitter=None)
        assert_equal(config, expected)

    def test_valid_daily_scheduler_start_time(self):
        expected = ('14:32 ', 14, 32, 0, set())
        self.assert_parse(('14:32', self.context), expected)

    def test_valid_daily_scheduler_just_days(self):
        expected = ("00:00:00 MWS", 0, 0, 0, set([1, 3, 6]))
        self.assert_parse(("00:00:00 MWS", self.context), expected)

    def test_valid_daily_scheduler_time_and_day(self):
        expected = ("17:02:44 SU", 17, 2, 44, set([0, 6]))
        self.assert_parse(("17:02:44 SU", self.context), expected)

    def test_valid_daily_scheduler_invalid_start_time(self):
        assert_raises(ConfigError, schedule_parse.valid_daily_scheduler,
            "5 MWF", self.context)
        assert_raises(ConfigError, schedule_parse.valid_daily_scheduler,
            "05:30:45:45 MWF", self.context)
        assert_raises(ConfigError, schedule_parse.valid_daily_scheduler,
            "25:30:45 MWF", self.context)

    def test_valid_daily_scheduler_invalid_days(self):
        assert_raises(ConfigError, schedule_parse.valid_daily_scheduler,
            "SUG", self.context)
        assert_raises(ConfigError, schedule_parse.valid_daily_scheduler,
            "3", self.context)


class ValidIntervalSchedulerTestCase(TestCase):

    @setup
    def setup_context(self):
        self.context = config_utils.NullConfigContext

    def test_valid_interval_scheduler_shortcut(self):
        config = schedule_parse.valid_interval_scheduler("hourly", self.context)
        expected = datetime.timedelta(hours=1)
        assert_equal(config.timedelta, expected)

    def test_valid_interval_scheduler_minutes(self):
        config = schedule_parse.valid_interval_scheduler(
                "5 minutes", self.context)
        expected = datetime.timedelta(minutes=5)
        assert_equal(config.timedelta, expected)

    def test_valid_interval_scheduler_hours(self):
        for spec in ['6h', '6 hours', '6 h', '6 hrs', '6 hour', u'6 hours', u'6hour']:
            config = schedule_parse.valid_interval_scheduler(spec, self.context)
            expected = datetime.timedelta(hours=6)
            assert_equal(config.timedelta, expected)

    def test_valid_interval_scheduler_invalid_tokens(self):
        assert_raises(ConfigError, schedule_parse.valid_interval_scheduler,
            "6 hours 22 minutes", self.context)

    def test_valid_interval_scheduler_unknown_unit(self):
        assert_raises(ConfigError, schedule_parse.valid_interval_scheduler,
            "22 beats", self.context)

    def test_valid_interval_scheduler_bogus(self):
        assert_raises(ConfigError, schedule_parse.valid_interval_scheduler,
            "asdasd.asd", self.context)

    def test_valid_interval_scheduler_underscore(self):
        assert_raises(ConfigError, schedule_parse.valid_interval_scheduler,
            u"6_minute", self.context)

    def test_valid_interval_scheduler_unicode(self):
        assert_raises(ConfigError, schedule_parse.valid_interval_scheduler,
            u"6 ຖminute", self.context)


if __name__ == "__main__":
    run()
