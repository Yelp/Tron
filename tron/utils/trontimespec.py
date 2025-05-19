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
from typing import Any
from typing import cast
from typing import Generator
from typing import Iterable
from typing import List
from typing import Optional
from typing import Protocol
from typing import Tuple
from typing import Union

import pytz


class PytzTimezone(Protocol):
    def localize(self, dt: datetime.datetime, is_dst: Optional[bool] = None) -> datetime.datetime:
        ...

    def normalize(self, dt: datetime.datetime) -> datetime.datetime:
        ...

    # feel free to add more methods as needed until we have saner types


def get_timezone(timezone_string: Optional[str]) -> Optional[PytzTimezone]:
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
        return pytz.timezone(timezone_string)
    else:
        return None


def to_timezone(t: datetime.datetime, tzinfo: Optional[PytzTimezone]) -> datetime.datetime:
    """Converts 't' to the time zone 'tzinfo'.

    Arguments:
      t: a datetime object.  It may be in any pytz time zone, or it may be
          timezone-naive (interpreted as UTC).
      tzinfo: a pytz timezone object, or None (interpreted as UTC).

    Returns:
      a datetime object in the time zone 'tzinfo'
    """
    if tzinfo:
        if not t.tzinfo:
            # Ensure we have a default timezone set (UTC) if no tzinfo was given
            t = pytz.utc.localize(t)
        # if tzinfo is provided, then the datetime object is converted to the given timezone
        # and normalized to adjust for discrepancies that might arise from daylight savings
        # time or other irregularities in the timezone.
        # HACK: maybe once we're on a newer python and can drop pytz we can clean this up
        return tzinfo.normalize(t.astimezone(cast(datetime.tzinfo, tzinfo)))
    elif t.tzinfo:
        # handles the case where tzinfo is not provided but t is timezone-aware
        # then it is converted to UTC then normalized to adjust for discrepancies
        # then lastly removing timezone info making t a timezone-naive datetime object
        return pytz.utc.normalize(t.astimezone(pytz.utc)).replace(tzinfo=None)
    else:
        # handles the case where tzinfo is not provided and t is timezone-naive
        return t


def naive_as_timezone(t: datetime.datetime, tzinfo: PytzTimezone) -> datetime.datetime:
    """Interprets the naive datetime with the given time zone."""
    try:
        result = tzinfo.localize(t, is_dst=None)
    except pytz.AmbiguousTimeError:
        # We are in the infamous 1 AM block which happens twice on
        # fall-back. Pretend like it's the first time, every time.
        result = tzinfo.localize(t, is_dst=True)
    except pytz.NonExistentTimeError:
        # We are in the infamous 2:xx AM block which does not
        # exist. Pretend like it's the later time, every time.
        result = tzinfo.localize(t, is_dst=False)
    return result


def get_time(time_string: str) -> Optional[datetime.time]:
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


TOKEN_LAST = "LAST"

ordinal_range = range(1, 6)
weekday_range = range(0, 7)
month_range = range(1, 13)
monthday_range = range(1, 32)
hour_range = range(0, 24)
minute_range = second_range = range(0, 60)


def validate_spec(
    source: Optional[List[Union[int, str]]],
    value_range: range,
    type: str,
    default: Optional[List[Union[int, str]]] = None,
    allow_last: bool = False,
) -> Union[List[Union[int, str]], range]:
    resolved_default: Union[List[Union[int, str]], range] = default if default is not None else value_range
    if not source:
        return resolved_default

    has_last = False
    source_wo_last = []
    for item in source:
        if allow_last and item == TOKEN_LAST:
            has_last = True
            continue
        if item not in value_range:
            raise ValueError(f"{type} not in range {value_range}")
        source_wo_last.append(item)

    sorted_source = sorted(source_wo_last)
    if has_last:
        sorted_source.append(TOKEN_LAST)

    return sorted_source


class TimeSpecification:
    """TimeSpecification determines the next time which matches the
    configured pattern.
    """

    def __init__(
        self,
        ordinals: Optional[List[Union[int, str]]] = None,
        weekdays: Optional[List[Union[int, str]]] = None,
        months: Optional[List[Union[int, str]]] = None,
        monthdays: Optional[List[Union[int, str]]] = None,
        timestr: Optional[str] = None,
        timezone: Optional[str] = None,
        minutes: Optional[List[Union[int, str]]] = None,
        hours: Optional[List[Union[int, str]]] = None,
        seconds: Optional[List[Union[int, str]]] = None,
    ):

        if weekdays and monthdays:
            raise ValueError("cannot supply both monthdays and weekdays")

        if timestr and (minutes or hours or seconds):
            raise ValueError("cannot supply both timestr and h/m/s")

        if not any((timestr, minutes, hours, seconds)):
            timestr = "00:00"

        if timestr:
            # TODO: there's a bug here if get_time returns None - fix this once we're done typing tron
            time = cast(datetime.time, get_time(timestr))
            hours = [time.hour]
            minutes = [time.minute]
            seconds = [0]

        self.hours = validate_spec(hours, hour_range, "hour")
        self.minutes = validate_spec(minutes, minute_range, "minute")
        self.seconds = validate_spec(seconds, second_range, "second")
        self.ordinals = validate_spec(ordinals, ordinal_range, "ordinal")
        self.weekdays = validate_spec(
            weekdays,
            weekday_range,
            "weekdays",
            allow_last=True,
        )
        self.months = validate_spec(months, month_range, "month")
        self.monthdays = validate_spec(
            monthdays,
            monthday_range,
            "monthdays",
            [],
            True,
        )
        self.timezone: Optional[PytzTimezone] = get_timezone(timezone)

    def next_day(self, first_day: int, year: int, month: int) -> List[int]:
        """Returns matching days for the given year and month."""
        first_day_of_month, last_day_of_month = calendar.monthrange(
            year,
            month,
        )

        def map_last(day: int) -> int:
            return last_day_of_month if day == TOKEN_LAST else day

        def day_filter(day: int) -> bool:
            return first_day <= day <= last_day_of_month

        def sort_days(days: Iterable[int]) -> List[int]:
            return sorted(filter(day_filter, days))

        if self.monthdays:
            # these casts are a necessary evil due to the TOKEN_LAST shenanigans we do
            return sort_days(map_last(day) for day in cast(List[int], self.monthdays))

        start_day = (first_day_of_month + 1) % 7

        def days_from_weekdays() -> Generator[int, None, None]:
            # these casts are a necessary evil due to the TOKEN_LAST shenanigans we do
            for ordinal in cast(List[int], self.ordinals):
                week = (ordinal - 1) * 7
                for weekday in cast(List[int], self.weekdays):
                    yield ((weekday - start_day) % 7) + week + 1

        return sort_days(days_from_weekdays())

    def next_month(self, start_date: datetime.datetime) -> Generator[Tuple[int, int], None, None]:
        """Create a generator which yields valid months after the start month."""
        current = start_date.month
        # these casts are a necessary evil due to the TOKEN_LAST shenanigans we do
        potential = [m for m in cast(List[int], self.months) if m >= current]
        year_wraps = 0

        while True:
            if not potential:
                year_wraps += 1
                potential = cast(List[int], list(self.months))

            yield potential.pop(0), start_date.year + year_wraps

    def next_time(self, start_date: datetime.datetime, is_start_day: bool) -> Optional[datetime.time]:
        """Return the next valid time."""
        start_hour = start_date.time().hour

        def hour_filter(hour: int) -> bool:
            return not is_start_day or hour >= start_hour

        # NOTE: these casts are a necessary evil due to the TOKEN_LAST shenanigans we do
        for hour in filter(hour_filter, cast(List[int], self.hours)):
            for minute in cast(List[int], self.minutes):
                for second in cast(List[int], self.seconds):
                    candidate = datetime.time(hour, minute, second)

                    if is_start_day and start_date.time() >= candidate:
                        continue

                    return candidate
        return None

    def get_match(self, start: datetime.datetime) -> Optional[datetime.datetime]:
        """Returns the next datetime match after start."""
        start_date = to_timezone(start, self.timezone).replace(tzinfo=None)

        def get_first_day(month: int, year: int) -> int:
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

                candidate = start_date.replace(
                    year,
                    month,
                    day,
                    time.hour,
                    time.minute,
                    second=time.second,
                    microsecond=0,
                )
                # HACK: as usual for this file, more casting to work around tz shenanigans
                candidate = self.handle_timezone(candidate, cast(PytzTimezone, start.tzinfo))
                if not candidate:
                    continue
                return candidate
        return None

    # TODO: test
    def handle_timezone(self, out: datetime.datetime, tzinfo: Optional[PytzTimezone]) -> datetime.datetime:
        if self.timezone:
            out = naive_as_timezone(out, self.timezone)
        return to_timezone(out, tzinfo)

    def __eq__(self, other: Any) -> bool:
        attrs = [
            "hours",
            "minutes",
            "seconds",
            "ordinals",
            "weekdays",
            "months",
            "monthdays",
            "timezone",
        ]
        return all(getattr(other, attr, None) == getattr(self, attr, None) for attr in attrs)

    def __ne__(self, other: Any) -> bool:
        return not self == other
