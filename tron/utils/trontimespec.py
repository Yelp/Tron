# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A complete time specification based on the Google App Engine GROC spec."""


import calendar
import datetime
import itertools

try:
    import pytz
    assert pytz
except ImportError:
    pytz = None

HOURS = 'hours'
MINUTES = 'minutes'

try:
    from pytz import NonExistentTimeError
    assert NonExistentTimeError
    from pytz import AmbiguousTimeError
    assert AmbiguousTimeError
except ImportError:
    class NonExistentTimeError(Exception):
        pass

    class AmbiguousTimeError(Exception):
        pass


def get_timezone(timezone_string):
    """Converts a timezone string to a pytz timezone object.

    Arguments:
      timezone_string: a string representing a timezone, or None

    Returns:
      a pytz timezone object, or None if the input timezone_string is None

    Raises:
      ValueError: if timezone_string is not None and the pytz module could not be
          loaded
    """
    if timezone_string:
        if pytz is None:
            raise ValueError('need pytz in order to specify a timezone')
        return pytz.timezone(timezone_string)
    else:
        return None


def to_timezone(t, tzinfo):
    """Converts 't' to the time zone 'tzinfo'.

    Arguments:
      t: a datetime object.  It may be in any pytz time zone, or it may be
          timezone-naive (interpreted as UTC).
      tzinfo: a pytz timezone object, or None (interpreted as UTC).

    Returns:
      a datetime object in the time zone 'tzinfo'
    """
    if pytz is None:
        return t.replace(tzinfo=tzinfo)
    elif tzinfo:
        if not t.tzinfo:
            t = pytz.utc.localize(t)
        return tzinfo.normalize(t.astimezone(tzinfo))
    elif t.tzinfo:
        return pytz.utc.normalize(t.astimezone(pytz.utc)).replace(tzinfo=None)
    else:
        return t


def get_time(time_string):
    """Converts a string to a datetime.time object.

    Arguments:
      time_string: a string representing a time ('hours:minutes')

    Returns:
      a datetime.time object
    """
    try:
        return datetime.datetime.strptime(time_string, "%H:%M").time()
    except ValueError:
        return None


TOKEN_LAST = 'LAST'

ordinal_range  = range(1, 6)
weekday_range  = range(0, 7)
month_range    = range(1, 13)
monthday_range = range(1, 32)
hour_range     = range(0, 24)
minute_range   = second_range = range(0, 60)


def validate_spec(source, value_range, type, default=None, allow_last=False):
    default = default if default is not None else value_range
    if not source:
        return default

    for item in source:
        if allow_last and item == TOKEN_LAST:
            continue
        if item not in value_range:
            raise ValueError("%s not in range %s" % (type, value_range))
    return sorted(set(source))


class TimeSpecification(object):
    """TimeSpecification determines the next time which matches the
    configured pattern.
    """

    def __init__(self,
            ordinals=None,
            weekdays=None,
            months=None,
            monthdays=None,
            timestr=None,
            timezone=None,
            minutes=None,
            hours=None,
            seconds=None):

        if weekdays and monthdays:
            raise ValueError('cannot supply both monthdays and weekdays')

        if timestr and (minutes or hours or seconds):
            raise ValueError('cannot supply both timestr and h/m/s')

        if not any((timestr, minutes, hours, seconds)):
            timestr = '00:00'

        if timestr:
            time    = get_time(timestr)
            hours   = [time.hour]
            minutes = [time.minute]
            seconds = [0]

        self.hours      = validate_spec(hours, hour_range, 'hour')
        self.minutes    = validate_spec(minutes, minute_range, 'minute')
        self.seconds    = validate_spec(seconds, second_range, 'second')
        self.ordinals   = validate_spec(ordinals, ordinal_range, 'ordinal')
        self.weekdays   = validate_spec(
            weekdays, weekday_range, 'weekdays', allow_last=True)
        self.months     = validate_spec(months, month_range, 'month')
        self.monthdays  = validate_spec(
            monthdays, monthday_range, 'monthdays', [], True)
        self.timezone   = get_timezone(timezone)

    def next_day(self, first_day, year, month):
        """Returns matching days for the given year and month.
        """
        first_day_of_month, last_day_of_month = calendar.monthrange(year, month)

        map_last   = lambda day: last_day_of_month if day == TOKEN_LAST else day
        day_filter = lambda day: first_day <= day <= last_day_of_month
        sort_days  = lambda days: sorted(itertools.ifilter(day_filter, days))

        if self.monthdays:
            return sort_days(map_last(day) for day in self.monthdays)

        start_day = (first_day_of_month + 1) % 7
        def days_from_weekdays():
            for ordinal in self.ordinals:
                week = (ordinal - 1) * 7
                for weekday in self.weekdays:
                    yield ((weekday - start_day) % 7) + week + 1

        return sort_days(days_from_weekdays())

    def next_month(self, start_date):
        """Create a generator which yields valid months after the start month.
        """
        current     = start_date.month
        potential   = [m for m in self.months if m >= current]
        year_wraps   = 0

        while True:
            if not potential:
                year_wraps += 1
                potential = list(self.months)

            yield potential.pop(0), start_date.year + year_wraps

    def next_time(self, start_date, is_start_day):
        """Return the next valid time."""
        start_hour   = start_date.time().hour
        hour_filter  = lambda hour: not is_start_day or hour >= start_hour

        for hour in itertools.ifilter(hour_filter, self.hours):
            for minute in self.minutes:
                for second in self.seconds:
                    candidate = datetime.time(hour, minute, second)

                    if is_start_day and start_date.time() >= candidate:
                        continue

                    return candidate

    def get_match(self, start):
        """Returns the next datetime match after start."""
        start_date  = to_timezone(start, self.timezone).replace(tzinfo=None)

        def get_first_day(month, year):
            if (month, year) != (start_date.month, start_date.year):
                return 1
            return start_date.day

        for month, year in self.next_month(start_date):
            first_day = get_first_day(month, year)

            for day in self.next_day(first_day, year, month):
                is_start_day = start_date.timetuple()[:3] == (year, month, day)

                time = self.next_time(start_date, is_start_day)
                if time is None:
                    continue

                candidate = start_date.replace(year, month, day, time.hour,
                    time.minute, second=time.second, microsecond=0)
                candidate = self.handle_timezone(candidate, start.tzinfo)
                if not candidate:
                    continue
                return candidate

    # TODO: test
    def handle_timezone(self, out, tzinfo):
        if self.timezone and pytz is not None:
            try:
                out = self.timezone.localize(out, is_dst=None)
            except AmbiguousTimeError:
                out = self.timezone.localize(out)
            except NonExistentTimeError:
                # TODO: this is duplicated in the scheduler
                for _ in range(24):
                    out += datetime.timedelta(minutes=60)
                    try:
                        out = self.timezone.localize(out)
                    except NonExistentTimeError:
                        return None
        return to_timezone(out, tzinfo)

    def __eq__(self, other):
        attrs = [
            'hours',
            'minutes',
            'seconds',
            'ordinals',
            'weekdays',
            'months',
            'monthdays',
            'timezone']
        return all(
            getattr(other, attr, None) == getattr(self, attr, None)
                for attr in attrs)

    def __ne__(self, other):
        return not self == other
