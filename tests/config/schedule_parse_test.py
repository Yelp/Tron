from testify import TestCase, run, assert_equal, assert_raises

from tron.config import schedule_parse, ConfigError


class ValidCronSchedulerTestCase(TestCase):
    _suites = ['integration']

    def test_valid_config(self):
        line = '5 0 L * *'.split()
        config = schedule_parse.valid_cron_scheduler(line)
        assert_equal(config.minutes, [5])
        assert_equal(config.months, None)
        assert_equal(config.monthdays, ['LAST'])


    def test_invalid_config(self):
        line = '* * *'
        assert_raises(ConfigError, schedule_parse.valid_cron_scheduler, line)


if __name__ == "__main__":
    run()