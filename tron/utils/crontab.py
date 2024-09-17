"""Parse a crontab entry and return a dictionary."""
import calendar
import itertools
import re
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

PREDEFINED_SCHEDULE = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}


def convert_predefined(line: str) -> str:
    if not line.startswith("@"):
        return line

    if line not in PREDEFINED_SCHEDULE:
        raise ValueError("Unknown predefine: %s" % line)
    return PREDEFINED_SCHEDULE[line]


class FieldParser:
    """Parse and validate a field in a crontab entry."""

    name: str = ""
    bounds: Tuple[int, int] = (0, 0)
    range_pattern = re.compile(
        r"""
        (?P<min>\d+|\*)         # Initial value
        (?:-(?P<max>\d+))?      # Optional max upper bound
        (?:/(?P<step>\d+))?     # Optional step increment
        """,
        re.VERBOSE,
    )

    def normalize(self, source: str) -> str:
        return source.strip()

    def get_groups(self, source: str) -> List[str]:
        return source.split(",")

    def parse(self, source: str) -> Optional[Union[List[int], List[str]]]:
        if source == "*":
            return None

        groups: Set[Union[int, str]] = set(
            itertools.chain.from_iterable(self.get_values(group) for group in self.get_groups(source))
        )
        has_last = "LAST" in groups
        if has_last:
            groups.remove("LAST")
        sorted_groups = sorted(groups, key=lambda x: (isinstance(x, str), x))
        if has_last:
            sorted_groups.append("LAST")
        if not sorted_groups:
            return None
        if all(isinstance(x, int) for x in sorted_groups):
            return sorted_groups  # type: ignore
        return sorted_groups  # type: ignore

    def get_match_groups(self, source: str) -> dict:
        match = self.range_pattern.match(source)
        if not match:
            raise ValueError("Unknown expression: %s" % source)
        return match.groupdict()

    def get_values(self, source: str) -> List[Union[int, str]]:
        source = self.normalize(source)
        match_groups = self.get_match_groups(source)
        step = 1
        min_value, max_value = self.get_value_range(match_groups)

        if match_groups["step"]:
            step = self.validate_bounds(match_groups["step"])
        return self.get_range(min_value, max_value, step)

    def get_value_range(self, match_groups: dict) -> Tuple[int, int]:
        if match_groups["min"] == "*":
            return self.bounds

        min_value = self.validate_bounds(match_groups["min"])
        if match_groups["max"]:
            # Cron expressions are inclusive, range is exclusive on upper bound
            max_value = self.validate_bounds(match_groups["max"]) + 1
            return min_value, max_value

        return min_value, min_value + 1

    def get_range(self, min_value: int, max_value: int, step: int) -> List[Union[int, str]]:
        if min_value < max_value:
            return list(range(min_value, max_value, step))

        min_bound, max_bound = self.bounds
        diff = (max_bound - min_value) + (max_value - min_bound)
        return [(min_value + i) % max_bound for i in list(range(0, diff, step))]

    def validate_bounds(self, value: str) -> int:
        min_value, max_value = self.bounds
        int_value = int(value)
        if not min_value <= int_value < max_value:
            raise ValueError(f"{self.name} value out of range: {int_value}")
        return int_value


class MinuteFieldParser(FieldParser):
    name = "minutes"
    bounds = (0, 60)


class HourFieldParser(FieldParser):
    name = "hours"
    bounds = (0, 24)


class MonthdayFieldParser(FieldParser):
    name = "monthdays"
    bounds = (1, 32)

    def get_values(self, source: str) -> List[Union[int, str]]:
        # Handle special case for last day of month
        source = self.normalize(source)
        if source == "L":
            return ["LAST"]

        return super().get_values(source)


class MonthFieldParser(FieldParser):
    name = "months"
    bounds = (1, 13)
    month_names = calendar.month_abbr[1:]

    def normalize(self, month: str) -> str:
        month = super().normalize(month)
        month = month.lower()
        for month_num, month_name in enumerate(self.month_names, start=1):
            month = month.replace(month_name.lower(), str(month_num))
        return month


class WeekdayFieldParser(FieldParser):
    name = "weekdays"
    bounds = (0, 7)
    day_names = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]

    def normalize(self, day_of_week: str) -> str:
        day_of_week = super().normalize(day_of_week)
        day_of_week = day_of_week.lower()
        for dow_num, dow_name in enumerate(self.day_names):
            day_of_week = day_of_week.replace(dow_name, str(dow_num))
        return day_of_week.replace("7", "0").replace("?", "*")


minute_parser = MinuteFieldParser()
hour_parser = HourFieldParser()
monthday_parser = MonthdayFieldParser()
month_parser = MonthFieldParser()
weekday_parser = WeekdayFieldParser()


# TODO: support L (for dow), W, #
def parse_crontab(line: str) -> dict:
    line = convert_predefined(line)
    minutes, hours, dom, months, dow = line.split(None, 4)

    return {
        "minutes": minute_parser.parse(minutes),
        "hours": hour_parser.parse(hours),
        "monthdays": monthday_parser.parse(dom),
        "months": month_parser.parse(months),
        "weekdays": weekday_parser.parse(dow),
        "ordinals": None,
    }
