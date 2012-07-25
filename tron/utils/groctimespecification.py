#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
# Modified 2011 Yelp Inc.
# from http://code.google.com/p/googleappengine/source/browse/trunk/python/google/appengine/cron/groctimespecification.py
#
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
#
"""Implementation of scheduling for Groc format schedules.

A Groc schedule looks like '1st,2nd monday 9:00', or 'every 20 mins'. This
module takes a parsed schedule (produced by Antlr) and creates objects that
can produce times that match this schedule.
"""


import datetime

from tron.utils import trontimespec

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


_GetTimezone = trontimespec.get_timezone
_ToTimeZone = trontimespec.to_timezone
_GetTime = trontimespec.get_time


class IntervalTimeSpecification(object):
  """A time specification for a given interval.

  An Interval type spec runs at the given fixed interval. It has the following
  attributes:
  period - the type of interval, either 'hours' or 'minutes'
  interval - the number of units of type period.
  synchronized - whether to synchronize the times to be locked to a fixed
      period (midnight in the specified timezone).
  start_time, end_time - restrict matches to a given range of times every day.
      If these are None, there is no restriction.  Otherwise, they are
      datetime.time objects.
  timezone - the time zone in which start_time and end_time should be
      interpreted, or None (defaults to UTC).  This is a pytz timezone object.
  """
  def __init__(self, interval, period, synchronized=False,
               start_time_string='', end_time_string='', timezone=None):
    if interval < 1:
      raise GrocException('interval must be greater than zero')
    self.interval = interval
    self.period = period
    self.synchronized = synchronized
    if self.period == HOURS:
      self.seconds = self.interval * 3600
    else:
      self.seconds = self.interval * 60
    self.timezone = _GetTimezone(timezone)

    if self.synchronized:
      if start_time_string:
        raise ValueError(
            'start_time_string may not be specified if synchronized is true')
      if end_time_string:
        raise ValueError(
            'end_time_string may not be specified if synchronized is true')
      if (self.seconds > 86400) or ((86400 % self.seconds) != 0):
        raise GrocException('can only use synchronized for periods that'
                                 ' divide evenly into 24 hours')

      self.start_time = datetime.time(0, 0).replace(tzinfo=self.timezone)
      self.end_time = datetime.time(23, 59).replace(tzinfo=self.timezone)
    elif start_time_string:
      if not end_time_string:
        raise ValueError(
            'end_time_string must be specified if start_time_string is')
      self.start_time = (
          _GetTime(start_time_string).replace(tzinfo=self.timezone))
      self.end_time = _GetTime(end_time_string).replace(tzinfo=self.timezone)
    else:
      if end_time_string:
        raise ValueError(
            'start_time_string must be specified if end_time_string is')
      self.start_time = None
      self.end_time = None

  def GetMatch(self, start):
    """Returns the next match after 'start'.

    Arguments:
      start: a datetime to start from. Matches will start from after this time.
          This may be in any pytz time zone, or it may be timezone-naive
          (interpreted as UTC).

    Returns:
      a datetime object in the timezone of the input 'start'
    """
    if self.start_time is None:
      return start + datetime.timedelta(seconds=self.seconds)

    t = _ToTimeZone(start, self.timezone)

    start_time = self._GetPreviousDateTime(t, self.start_time)

    t_delta = t - start_time
    t_delta_seconds = (t_delta.days * 60 * 24 + t_delta.seconds)
    num_intervals = (t_delta_seconds + self.seconds) / self.seconds
    interval_time = (
        start_time + datetime.timedelta(seconds=(num_intervals * self.seconds)))
    if self.timezone:
      interval_time = self.timezone.normalize(interval_time)

    next_start_time = self._GetNextDateTime(t, self.start_time)
    if (self._TimeIsInRange(t) and
        self._TimeIsInRange(interval_time) and
        interval_time < next_start_time):
      result = interval_time
    else:
      result = next_start_time


    return _ToTimeZone(result, start.tzinfo)

  def _TimeIsInRange(self, t):
    """Returns true if 't' falls between start_time and end_time, inclusive.

    Arguments:
      t: a datetime object, in self.timezone

    Returns:
      a boolean
    """
    previous_start_time = self._GetPreviousDateTime(t, self.start_time)
    previous_end_time = self._GetPreviousDateTime(t, self.end_time)
    if previous_start_time > previous_end_time:
      return True
    else:
      return t == previous_end_time

  @staticmethod
  def _GetPreviousDateTime(t, target_time):
    """Returns the latest datetime <= 't' that has the time target_time.

    Arguments:
      t: a datetime.datetime object, in self.timezone
      target_time: a datetime.time object, in self.timezone

    Returns:
      a datetime.datetime object, in self.timezone
    """
    date = t.date()
    while True:
      result = IntervalTimeSpecification._CombineDateAndTime(date, target_time)
      if result <= t:
        return result
      date -= datetime.timedelta(days=1)

  @staticmethod
  def _GetNextDateTime(t, target_time):
    """Returns the earliest datetime > 't' that has the time target_time.

    Arguments:
      t: a datetime.datetime object, in self.timezone
      target_time: a time object, in self.timezone

    Returns:
      a datetime.datetime object, in self.timezone
    """
    date = t.date()
    while True:
      result = IntervalTimeSpecification._CombineDateAndTime(date, target_time)
      if result > t:
        return result
      date += datetime.timedelta(days=1)

  @staticmethod
  def _CombineDateAndTime(date, time):
    """Creates a datetime object from date and time objects.

    This is similar to the datetime.combine method, but its timezone
    calculations are designed to work with pytz.

    Arguments:
      date: a datetime.date object, in any timezone
      time: a datetime.time object, in any timezone

    Returns:
      a datetime.datetime object, in the timezone of the input 'time'
    """
    if time.tzinfo:
      naive_result = datetime.datetime(
          date.year, date.month, date.day, time.hour, time.minute, time.second)
      try:
        return time.tzinfo.localize(naive_result, is_dst=None)
      except AmbiguousTimeError:


        return min(time.tzinfo.localize(naive_result, is_dst=True),
                   time.tzinfo.localize(naive_result, is_dst=False))
      except NonExistentTimeError:
        while True:
          naive_result += datetime.timedelta(minutes=1)
          try:
            return time.tzinfo.localize(naive_result, is_dst=None)
          except NonExistentTimeError:
            pass
    else:
      return datetime.datetime.combine(date, time)