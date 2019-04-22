"""Parse a crontab entry and return a dictionary."""
from __future__ import absolute_import
from __future__ import unicode_literals

import calendar
import itertools
import re

PREDEFINED_SCHEDULE = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}


def convert_predefined(line):
    if not line.startswith('@'):
        return line

    if line not in PREDEFINED_SCHEDULE:
        raise ValueError("Unknown predefine: %s" % line)
    return PREDEFINED_SCHEDULE[line]


class FieldParser(object):
    """Parse and validate a field in a crontab entry."""

    name = None
    bounds = None
    range_pattern = re.compile(
        r'''
        (?P<min>\d+|\*)         # Initial value
        (?:-(?P<max>\d+))?      # Optional max upper bound
        (?:/(?P<step>\d+))?     # Optional step increment
        ''',
        re.VERBOSE,
    )

    def normalize(self, source):
        return source.strip()

    def get_groups(self, source):
        return source.split(',')

    def parse(self, source):
        if source == '*':
            return None

        groups = [self.get_values(group) for group in self.get_groups(source)]
        groups = set(itertools.chain.from_iterable(groups))
        has_last = False
        if 'LAST' in groups:
            has_last = True
            groups.remove('LAST')
        groups = sorted(groups)
        if has_last:
            groups.append('LAST')
        return groups

    def get_match_groups(self, source):
        match = self.range_pattern.match(source)
        if not match:
            raise ValueError("Unknown expression: %s" % source)
        return match.groupdict()

    def get_values(self, source):
        source = self.normalize(source)
        match_groups = self.get_match_groups(source)
        step = 1
        min_value, max_value = self.get_value_range(match_groups)

        if match_groups['step']:
            step = self.validate_bounds(match_groups['step'])
        return self.get_range(min_value, max_value, step)

    def get_value_range(self, match_groups):
        if match_groups['min'] == '*':
            return self.bounds

        min_value = self.validate_bounds(match_groups['min'])
        if match_groups['max']:
            # Cron expressions are inclusive, range is exclusive on upper bound
            max_value = self.validate_bounds(match_groups['max']) + 1
            return min_value, max_value

        return min_value, min_value + 1

    def get_range(self, min_value, max_value, step):
        if min_value < max_value:
            return list(range(min_value, max_value, step))

        min_bound, max_bound = self.bounds
        diff = (max_bound - min_value) + (max_value - min_bound)
        return [(min_value + i) % max_bound
                for i in list(range(0, diff, step))]

    def validate_bounds(self, value):
        min_value, max_value = self.bounds
        value = int(value)
        if not min_value <= value < max_value:
            raise ValueError("%s value out of range: %s" % (self.name, value))
        return value


class MinuteFieldParser(FieldParser):
    name = 'minutes'
    bounds = (0, 60)


class HourFieldParser(FieldParser):
    name = 'hours'
    bounds = (0, 24)


class MonthdayFieldParser(FieldParser):
    name = 'monthdays'
    bounds = (1, 32)

    def get_values(self, source):
        # Handle special case for last day of month
        source = self.normalize(source)
        if source == 'L':
            return ['LAST']

        return super(MonthdayFieldParser, self).get_values(source)


class MonthFieldParser(FieldParser):
    name = 'months'
    bounds = (1, 13)
    month_names = calendar.month_abbr[1:]

    def normalize(self, month):
        month = super(MonthFieldParser, self).normalize(month)
        month = month.lower()
        for month_num, month_name in enumerate(self.month_names, start=1):
            month = month.replace(month_name.lower(), str(month_num))
        return month


class WeekdayFieldParser(FieldParser):
    name = 'weekdays'
    bounds = (0, 7)
    day_names = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat']

    def normalize(self, day_of_week):
        day_of_week = super(WeekdayFieldParser, self).normalize(day_of_week)
        day_of_week = day_of_week.lower()
        for dow_num, dow_name in enumerate(self.day_names):
            day_of_week = day_of_week.replace(dow_name, str(dow_num))
        return day_of_week.replace('7', '0').replace('?', '*')


minute_parser = MinuteFieldParser()
hour_parser = HourFieldParser()
monthday_parser = MonthdayFieldParser()
month_parser = MonthFieldParser()
weekday_parser = WeekdayFieldParser()


# TODO: support L (for dow), W, #
def parse_crontab(line):
    line = convert_predefined(line)
    minutes, hours, dom, months, dow = line.split(None, 4)

    return {
        'minutes': minute_parser.parse(minutes),
        'hours': hour_parser.parse(hours),
        'monthdays': monthday_parser.parse(dom),
        'months': month_parser.parse(months),
        'weekdays': weekday_parser.parse(dow),
        'ordinals': None,
    }
