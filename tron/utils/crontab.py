"""Parse a crontab entry and return a dictionary."""

import datetime
import calendar
import itertools
import re


#L_FIELDS        = (DAYS_OF_WEEK, DAYS_OF_MONTH)
DEFAULT_EPOCH   = (1970, 1, 1, 0, 0, 0)

PREDEFINED_SCHEDULE = {
    "@yearly":  "0 0 1 1 *",
    "@anually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly":  "0 0 * * 0",
    "@daily":   "0 0 * * *",
    "@midnight":"0 0 * * *",
    "@hourly":  "0 * * * *"}


def convert_predefined(line):
    if not line.startswith('@'):
        return line

    if line not in PREDEFINED_SCHEDULE:
        raise ValueError("Unknown predefine: %s" % line)
    return PREDEFINED_SCHEDULE[line]


def expand_bounds(bounds):
    return bounds[0], bounds[1] + 1


# TODO: support L, W, #
def parse_crontab(line):
    line     = convert_predefined(line)
    fields   = line.split(None, 4)
    minutes, hours, dom, months, dow = fields

    return {
        'minutes':      minutes,
        'hours':        hours,
        'monthdays':    dom,
        'months':       months,
        'weekdays':     dow}


class CronExpression(object):
    def __init__(self, line, epoch=DEFAULT_EPOCH, epoch_utc_offset=0):
        """
        Instantiates a CronExpression object with an optionally defined epoch.
        If the epoch is defined, the UTC offset can be specified one of two
        ways: as the sixth element in 'epoch' or supplied in epoch_utc_offset.
        The epoch should be defined down to the minute sorted by
        descending significance.
        """
        for key, value in PREDEFINED_SCHEDULE.items():
            if line.startswith(key):
                line = line.replace(key, value)
                break

        fields = line.split(None, 5)
        if len(fields) == 5:
            fields.append('')

        minutes, hours, dom, months, dow, self.comment = fields

        dow = dow.replace('7', '0').replace('?', '*')
        dom = dom.replace('?', '*')

        for monthnum, monthstr in MONTH_NAMES:
            months = months.lower().replace(monthstr, str(monthnum))

        for downum, dowstr in DAY_NAMES:
            dow = dow.lower().replace(dowstr, str(downum))

        self.string_tab = [minutes, hours, dom.upper(), months, dow.upper()]
        self.compute_numtab()
        if len(epoch) == 5:
            y, mo, d, h, m = epoch
            self.epoch = (y, mo, d, h, m, epoch_utc_offset)
        else:
            self.epoch = epoch

    def __str__(self):
        base = self.__class__.__name__ + "(%s)"
        cron_line = self.string_tab + [str(self.comment)]
        if not self.comment:
            cron_line.pop()
        arguments = '"' + ' '.join(cron_line) + '"'
        if self.epoch != DEFAULT_EPOCH:
            return base % (arguments + ", epoch=" + repr(self.epoch))
        else:
            return base % arguments

    def __repr__(self):
        return str(self)

    def compute_numtab(self):
        """
        Recomputes the sets for the static ranges of the trigger time.

        This method should only be called by the user if the string_tab
        member is modified.
        """
        self.numerical_tab = []

        for field_str, span in zip(self.string_tab, FIELD_RANGES):
            split_field_str = field_str.split(',')
            if len(split_field_str) > 1 and "*" in split_field_str:
                raise ValueError("\"*\" must be alone in a field.")

            unified = set()
            for cron_atom in split_field_str:
                # parse_atom only handles static cases
                for special_char in ('%', '#', 'L', 'W'):
                    if special_char in cron_atom:
                        break
                else:
                    unified.update(FieldParser.parse(cron_atom, span))

            self.numerical_tab.append(unified)

        if self.string_tab[2] == "*" and self.string_tab[4] != "*":
            self.numerical_tab[2] = set()

    def check_trigger(self, date_tuple, utc_offset=0):
        """
        Returns boolean indicating if the trigger is active at the given time.
        The date tuple should be in the local time. Unless periodicities are
        used, utc_offset does not need to be specified. If periodicities are
        used, specifically in the hour and minutes fields, it is crucial that
        the utc_offset is specified.
        """
        year, month, day, hour, mins = date_tuple
        given_date = datetime.date(year, month, day)
        zeroday = datetime.date(*self.epoch[:3])
        last_dom = calendar.monthrange(year, month)[-1]
        dom_matched = True

        # In calendar and datetime.date.weekday, Monday = 0
        given_dow = (datetime.date.weekday(given_date) + 1) % 7
        first_dow = (given_dow + 1 - day) % 7

        # Figure out how much time has passed from the epoch to the given date
        utc_diff = utc_offset - self.epoch[5]
        mod_delta_yrs = year - self.epoch[0]
        mod_delta_mon = month - self.epoch[1] + mod_delta_yrs * 12
        mod_delta_day = (given_date - zeroday).days
        mod_delta_hrs = hour - self.epoch[3] + mod_delta_day * 24 + utc_diff
        mod_delta_min = mins - self.epoch[4] + mod_delta_hrs * 60

        # Makes iterating through like components easier.
        quintuple = zip(
            (mins, hour, day, month, given_dow),
            self.numerical_tab,
            self.string_tab,
            (mod_delta_min, mod_delta_hrs, mod_delta_day, mod_delta_mon,
             mod_delta_day),
            FIELD_RANGES)

        for value, valid_values, field_str, delta_t, field_type in quintuple:
            # All valid, static values for the fields are stored in sets
            if value in valid_values:
                continue

            # The following for loop implements the logic for context
            # sensitive and epoch sensitive constraints. break statements,
            # which are executed when a match is found, lead to a continue
            # in the outer loop. If there are no matches found, the given date
            # does not match expression constraints, so the function returns
            # False as seen at the end of this for...else... construct.
            for cron_atom in field_str.split(','):
                if cron_atom[0] == '%':
                    if not(delta_t % int(cron_atom[1:])):
                        break

                elif field_type == DAYS_OF_WEEK and '#' in cron_atom:
                    D, N = int(cron_atom[0]), int(cron_atom[2])
                    # Computes Nth occurence of D day of the week
                    if (((D - first_dow) % 7) + 1 + 7 * (N - 1)) == day:
                        break

                elif field_type == DAYS_OF_MONTH and cron_atom[-1] == 'W':
                    target = min(int(cron_atom[:-1]), last_dom)
                    lands_on = (first_dow + target - 1) % 7
                    if lands_on == 0:
                        # Shift from Sun. to Mon. unless Mon. is next month
                        target += 1 if target < last_dom else -2
                    elif lands_on == 6:
                        # Shift from Sat. to Fri. unless Fri. in prior month
                        target += -1 if target > 1 else 2

                    # Break if the day is correct, and target is a weekday
                    if target == day and (first_dow + target - 7) % 7 > 1:
                        break

                elif field_type in L_FIELDS and cron_atom.endswith('L'):
                    # In dom field, L means the last day of the month
                    target = last_dom

                    if field_type == DAYS_OF_WEEK:
                        # Calculates the last occurence of given day of week
                        desired_dow = int(cron_atom[:-1])
                        target = (((desired_dow - first_dow) % 7) + 29)
                        target -= 7 if target > last_dom else 0

                    if target == day:
                        break
            else:
                # See 2010.11.15 of CHANGELOG
                if field_type == DAYS_OF_MONTH and self.string_tab[4] != '*':
                    dom_matched = False
                    continue
                elif field_type == DAYS_OF_WEEK and self.string_tab[2] != '*':
                    # If we got here, then days of months validated so it does
                    # not matter that days of the week failed.
                    return dom_matched

                # None of the expressions matched which means this field fails
                return False

        # Arriving at this point means the date landed within the constraints
        # of all fields; the associated trigger should be fired.
        return True


class FieldParser(object):
    """Parse and validate a field in a crontab entry."""

    name   = None
    bounds = None
    range_pattern = re.compile(r'''
        (?P<min>\d+|\*)     # Initial value
        (?:-(?P<max>\d+))?   # Optional max upper bound
        (?:/(?P<step>\d+))?   # Optional step increment
        ''', re.VERBOSE)

    def normalize(self, source):
        return source.strip()

    def get_groups(self, source):
        return source.split(',')

    def parse(self, source):
        source  = self.normalize(source)
        groups  = [self.get_values(group) for group in self.get_groups(source)]
        return sorted(set(itertools.chain.from_iterable(groups)))

    def get_values(self, source):
        match = self.range_pattern.match(source)
        if not match:
            raise ValueError("Unknown expression: %s" % source)

        min_bound, max_bound = min_value, max_value = self.bounds
        step                 = 1
        match_groups         = match.groupdict()

        if match_groups['min'] != '*':
            min_value = self.validate_bounds(match_groups['min'])
            max_value = min_value + 1
            if match_groups['max']:
                # Cron expressions are inclusive, range is exclusive on upper bound
                max_value = self.validate_bounds(match_groups['max']) + 1


        if match_groups['step']:
            step = self.validate_bounds(match_groups['step'])

        if min_value < max_value:
            return range(min_value, max_value, step)

        diff = (max_bound - min_value) + (max_value - min_bound)
        return [(min_value + i) % max_bound for i in xrange(0, diff, step)]

    def validate_bounds(self, value):
        min_value, max_value = self.bounds
        value = int(value)
        if not min_value <= value < max_value:
            raise ValueError("%s value out of range: %s" % (self.name, value))
        return value


class MinuteFieldParser(FieldParser):
    name    = 'minutes'
    bounds  = (0, 60)

class HourFieldParser(FieldParser):
    name    = 'hours'
    bounds  = (0, 24)

class MonthdayFieldParser(FieldParser):
    name    = 'monthdays'
    bounds  = (1, 32)

class MonthFieldParser(FieldParser):
    name        = 'months'
    bounds      = (1, 13)
    month_names = enumerate(calendar.month_abbr[1:], start=1)

    def normalize(self, month):
        month = super(MonthFieldParser, self).normalize(month)
        month = month.lower()

        for month_num, month_str in self.month_names:
            month = month.replace(month_str, str(month_num))
        return month

class WeekdayFieldParser(FieldParser):
    name        = 'weekdays'
    bounds      = (0, 7)
    day_names   = enumerate(['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'])


    def normalize(self, day_of_week):
        day_of_week = super(WeekdayFieldParser, self).normalize(day_of_week)
        day_of_week = day_of_week.lower()

        for dow_num, dow_str in self.day_names:
            day_of_week = day_of_week.replace(dow_str, str(dow_num))

        return day_of_week.replace('7', '0').replace('?', '*')