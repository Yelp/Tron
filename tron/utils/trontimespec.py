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

class GrocException(Exception):
    pass


def _GetTimezone(timezone_string):
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


def _ToTimeZone(t, tzinfo):
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


def _GetTime(time_string):
    """Converts a string to a datetime.time object.

    Arguments:
      time_string: a string representing a time ('hours:minutes')

    Returns:
      a datetime.time object
    """
    try:
        hourstr, minutestr = time_string.split(':')[0:2]
        return datetime.time(int(hourstr), int(minutestr))
    except ValueError:
        return None


# TODO: delete from tron.util.groctimespecification once complete
class TronTimeSpecification(object):
    """Specific time specification.

    A Specific interval is more complex, but defines a certain time to run and
    the days that it should run. It has the following attributes:
    time     - the time of day to run, as 'HH:MM'
    ordinals - first, second, third &c, as a set of integers in 1..5
    months   - the months that this should run, as a set of integers in 1..12
    weekdays - the days of the week that this should run, as a set of integers,
               0=Sunday, 6=Saturday
    timezone - the optional timezone as a string for this specification.
               Defaults to UTC - valid entries are things like Australia/Victoria
               or PST8PDT.

    A specific time schedule can be quite complex. A schedule could look like
    this:
    '1st,third sat,sun of jan,feb,mar 09:15'

    In this case, ordinals would be {1,3}, weekdays {0,6}, months {1,2,3} and
    time would be '09:15'.
    """

    def __init__(self, ordinals=None, weekdays=None, months=None, monthdays=None,
                 timestr='00:00', timezone=None):

        super(TronTimeSpecification, self).__init__()
        if weekdays and monthdays:
            raise ValueError('cannot supply both monthdays and weekdays')

        self.ordinals = set(range(1, 6)) if ordinals is None else set(ordinals)
        self.weekdays = set(range(7)) if weekdays is None else set(weekdays)
        self.months   = set(range(1, 13)) if months is None else set(months)

        if not monthdays:
            self.monthdays = set()
        else:
            if max(monthdays) > 31 or min(monthdays) < 1:
                raise ValueError('invalid day of month')
            self.monthdays = set(monthdays)

        self.time = _GetTime(timestr)
        self.timezone = _GetTimezone(timezone)

    def _MatchingDays(self, year, month):
        """Returns matching days for the given year and month.

        For the given year and month, return the days that match this instance's
        day specification, based on either (a) the ordinals and weekdays, or
        (b) the explicitly specified monthdays.  If monthdays are specified,
        dates that fall outside the range of the month will not be returned.

        Arguments:
          year: the year as an integer
          month: the month as an integer, in range 1-12

        Returns:
          a list of matching days, as ints in range 1-31
        """
        start_day, last_day = calendar.monthrange(year, month)
        if self.monthdays:
            return sorted([day for day in self.monthdays if day <= last_day])

        out_days = []
        start_day = (start_day + 1) % 7
        for ordinal in self.ordinals:
            for weekday in self.weekdays:
                day = ((weekday - start_day) % 7) + 1
                day += 7 * (ordinal - 1)
                if day <= last_day:
                    out_days.append(day)
        return sorted(out_days)

    def _NextMonthGenerator(self, start, matches):
        """Creates a generator that produces results from the set 'matches'.

        Matches must be >= 'start'. If none match, the wrap counter is incremented,
        and the result set is reset to the full set. Yields a 2-tuple of (match,
        wrapcount).

        Arguments:
          start: first set of matches will be >= this value (an int)
          matches: the set of potential matches (a sequence of ints)

        Yields:
          a two-tuple of (match, wrap counter). match is an int in range (1-12),
          wrapcount is a int indicating how many times we've wrapped around.
        """
        potential = matches = sorted(matches)

        after = start - 1
        wrapcount = 0
        while True:
            potential = [x for x in potential if x > after]
            if not potential:

                wrapcount += 1
                potential = matches
            after = potential[0]
            yield (after, wrapcount)

    def GetMatch(self, start):
        """Returns the next match after time start.

        Must be implemented in subclasses.

        Arguments:
          start: a datetime to start from. Matches will start from after this time.
              This may be in any pytz time zone, or it may be timezone-naive
              (interpreted as UTC).

        Returns:
          a datetime object in the timezone of the input 'start'
        """
        start_time = _ToTimeZone(start, self.timezone).replace(tzinfo=None)
        months = self._NextMonthGenerator(start_time.month, self.months)

        while True:
            month, yearwraps = months.next()
            candidate_month = start_time.replace(
                day=1, month=month, year=start_time.year + yearwraps)

            day_matches = self._MatchingDays(candidate_month.year, month)

            if ((candidate_month.year, candidate_month.month)
                == (start_time.year, start_time.month)):

                day_matches = [x for x in day_matches if x >= start_time.day]

                while (day_matches and day_matches[0] == start_time.day
                       and start_time.time() >= self.time):
                    day_matches.pop(0)

            while day_matches:
                out = candidate_month.replace(day=day_matches[0], hour=self.time.hour,
                    minute=self.time.minute, second=0,
                    microsecond=0)

                if self.timezone and pytz is not None:
                    try:
                        out = self.timezone.localize(out, is_dst=None)
                    except AmbiguousTimeError:
                        out = self.timezone.localize(out)
                    except NonExistentTimeError:
                        for _ in range(24):
                            out = out + datetime.timedelta(minutes=60)
                            try:
                                out = self.timezone.localize(out)
                            except NonExistentTimeError:
                                continue
                            break
                return _ToTimeZone(out, start.tzinfo)
