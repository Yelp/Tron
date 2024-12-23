from unittest import mock

from testifycompat import assert_equal
from testifycompat import assert_raises
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tron.utils import crontab


class TestConvertPredefined(TestCase):
    def test_convert_predefined_valid(self):
        expected = crontab.PREDEFINED_SCHEDULE["@hourly"]
        assert_equal(crontab.convert_predefined("@hourly"), expected)

    def test_convert_predefined_invalid(self):
        assert_raises(ValueError, crontab.convert_predefined, "@bogus")

    def test_convert_predefined_none(self):
        line = "something else"
        assert_equal(crontab.convert_predefined(line), line)


class TestParseCrontab(TestCase):
    def test_parse_asterisk(self):
        line = "* * * * *"
        actual = crontab.parse_crontab(line)
        assert_equal(actual["minutes"], None)
        assert_equal(actual["hours"], None)
        assert_equal(actual["months"], None)

    @mock.patch("tron.utils.crontab.MinuteFieldParser.parse", autospec=True)
    @mock.patch("tron.utils.crontab.HourFieldParser.parse", autospec=True)
    @mock.patch("tron.utils.crontab.MonthdayFieldParser.parse", autospec=True)
    @mock.patch("tron.utils.crontab.MonthFieldParser.parse", autospec=True)
    @mock.patch("tron.utils.crontab.WeekdayFieldParser.parse", autospec=True)
    def test_parse(self, mock_dow, mock_month, mock_monthday, mock_hour, mock_min):
        line = "* * * * *"
        actual = crontab.parse_crontab(line)
        assert_equal(actual["minutes"], mock_min.return_value)
        assert_equal(actual["hours"], mock_hour.return_value)
        assert_equal(actual["monthdays"], mock_monthday.return_value)
        assert_equal(actual["months"], mock_month.return_value)
        assert_equal(actual["weekdays"], mock_dow.return_value)

    def test_full_crontab_line(self):
        line = "*/15 0 1,15 * 1-5"
        expected = {
            "minutes": [0, 15, 30, 45],
            "hours": [0],
            "monthdays": [1, 15],
            "months": None,
            "weekdays": [1, 2, 3, 4, 5],
            "ordinals": None,
        }
        assert_equal(crontab.parse_crontab(line), expected)

    def test_full_crontab_line_with_last(self):
        line = "0 0 L * *"
        expected = {
            "minutes": [0],
            "hours": [0],
            "monthdays": ["LAST"],
            "months": None,
            "weekdays": None,
            "ordinals": None,
        }
        assert_equal(crontab.parse_crontab(line), expected)


class TestMinuteFieldParser(TestCase):
    @setup
    def setup_parser(self):
        self.parser = crontab.MinuteFieldParser()

    def test_validate_bounds(self):
        assert_equal(self.parser.validate_bounds(0), 0)
        assert_equal(self.parser.validate_bounds(59), 59)
        assert_raises(ValueError, self.parser.validate_bounds, 60)

    def test_get_values_asterisk(self):
        assert_equal(self.parser.get_values("*"), list(range(0, 60)))

    def test_get_values_min_only(self):
        assert_equal(self.parser.get_values("4"), [4])
        assert_equal(self.parser.get_values("33"), [33])

    def test_get_values_with_step(self):
        assert_equal(self.parser.get_values("*/10"), [0, 10, 20, 30, 40, 50])

    def test_get_values_with_step_and_range(self):
        assert_equal(self.parser.get_values("10-30/10"), [10, 20, 30])

    def test_get_values_with_step_and_overflow_range(self):
        assert_equal(self.parser.get_values("30-0/10"), [30, 40, 50, 0])

    def test_parse_with_groups(self):
        assert_equal(self.parser.parse("5,1,7,8,5"), [1, 5, 7, 8])

    def test_parse_with_groups_and_ranges(self):
        expected = [0, 1, 11, 13, 15, 17, 19, 20, 21, 40]
        assert_equal(self.parser.parse("1,11-22/2,*/20"), expected)


class TestMonthFieldParser(TestCase):
    @setup
    def setup_parser(self):
        self.parser = crontab.MonthFieldParser()

    def test_parse(self):
        expected = [1, 2, 3, 7, 12]
        assert_equal(self.parser.parse("DEC, Jan-Feb, jul, MaR"), expected)


class TestWeekdayFieldParser(TestCase):
    @setup
    def setup_parser(self):
        self.parser = crontab.WeekdayFieldParser()

    def test_parser(self):
        expected = [0, 3, 5, 6]
        assert_equal(self.parser.parse("Sun, 3, FRI, SaT-Sun"), expected)


class TestMonthdayFieldParser(TestCase):
    @setup
    def setup_parser(self):
        self.parser = crontab.MonthdayFieldParser()

    def test_parse_last(self):
        expected = [5, 6, "LAST"]
        assert_equal(self.parser.parse("5, 6, L"), expected)


class TestComplexExpressions(TestCase):
    @setup
    def setup_parser(self):
        self.parser = crontab.MinuteFieldParser()

    def test_complex_expression(self):
        expected = [0, 10, 20, 30, 40, 50, 55]
        assert_equal(self.parser.parse("*/10,55"), expected)


class TestInvalidInputs(TestCase):
    @setup
    def setup_parser(self):
        self.parser = crontab.MinuteFieldParser()

    def test_invalid_expression(self):
        with assert_raises(ValueError):
            self.parser.parse("61")


class TestBoundaryValues(TestCase):
    @setup
    def setup_parser(self):
        self.parser = crontab.MinuteFieldParser()

    def test_boundary_values(self):
        assert_equal(self.parser.parse("0"), [0])
        assert_equal(self.parser.parse("59"), [59])


if __name__ == "__main__":
    run()
