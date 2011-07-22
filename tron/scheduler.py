import calendar
import datetime
import logging
import re

from collections import deque
from tron.groctimespecification import IntervalTimeSpecification, SpecificTimeSpecification
from tron.utils import timeutils

log = logging.getLogger('tron.scheduler')

WEEK = 'mtwrfsu'

# Also support Monday, Mon, mon, mo, Tuesday, Tue, tue, tu...
CONVERT_DAYS = dict()
CONVERT_DAYS_INT = dict()
for day_list in (calendar.day_name,
                 calendar.day_abbr,
                 WEEK,
                 ('mo', 'tu', 'we', 'th', 'fr', 'sa', 'su')):
    for key, value in zip(day_list, range(7)):
        CONVERT_DAYS_INT[key] = value
        CONVERT_DAYS_INT[key.lower()] = value
        CONVERT_DAYS[key] = WEEK[value]
        CONVERT_DAYS[key.lower()] = WEEK[value]

# Support January, Jan, january, jan, February, Feb...
CONVERT_MONTHS = dict()
# calendar stores month data with a useless element in front
# map their values to int indices
for month_list in (calendar.month_name[1:], calendar.month_abbr[1:]):
    for key, value in zip(month_list, range(1, 13)):
        CONVERT_MONTHS[key] = value
        CONVERT_MONTHS[key.lower()] = value

# Build a regular expression that matches this:
# ("every"|ordinal) (days) ["of" (monthspec)] ("at" time)
# Where:
# ordinal specifies a comma separated list of "1st" and so forth
# days specifies a comma separated list of days of the week (for example,
#   "mon", "tuesday", with both short and long forms being accepted); "every
#   day" is equivalent to "every mon,tue,wed,thu,fri,sat,sun"
# monthspec specifies a comma separated list of month names (for example,
#   "jan", "march", "sep"). If omitted, implies every month. You can also say
#   "month" to mean every month, as in "1,8,15,22 of month 09:00".
# time specifies the time of day, as HH:MM in 24 hour time.
# from http://code.google.com/appengine/docs/python/config/cron.html#The_Schedule_Format

DAY_VALUES = '|'.join(CONVERT_DAYS.keys() + ['day'])
MONTH_VALUES = '|'.join(CONVERT_MONTHS.keys() + ['month'])
DATE_SUFFIXES = 'st|nd|rd|th'

MONTH_DAYS_EXPR = '(?P<month_days>every|((\d+(%s),?)+))?' % DATE_SUFFIXES
DAYS_EXPR = r'((?P<days>((%s),?)+))?' % DAY_VALUES
MONTHS_EXPR = r'((in|of) (?P<months>((%s),?)+))?' % MONTH_VALUES
TIME_EXPR = r'(at (?P<time>\d\d:\d\d))?'

GROC_SCHEDULE_EXPR = ''.join([
    r'^',
    MONTH_DAYS_EXPR, r' ?',
    DAYS_EXPR, r' ?',
    MONTHS_EXPR, r' ?',
     TIME_EXPR, r' ?',
    r'$'
])

GROC_SCHEDULE_RE = re.compile(GROC_SCHEDULE_EXPR)

class ConstantScheduler(object):
    """The constant scheduler only schedules the first one.  The job run starts then next when finished"""
    def next_runs(self, job):
        if job.next_to_finish():
            return []
        
        job_runs = job.build_runs()
        for job_run in job_runs:
            job_run.set_run_time(timeutils.current_time())
        
        return job_runs

    def job_setup(self, job):
        job.constant = True
        job.queueing = False

    def __str__(self):
        return "CONSTANT"

    def __eq__(self, other):
        return isinstance(other, ConstantScheduler)

    def __ne__(self, other):
        return not self == other


class DailyScheduler(object):
    """The daily scheduler schedules one run per day"""
    def __init__(self, start_time=None, days=1):
        # What time of day does this thing start ? Default to 1 second after midnight
        self.start_time = start_time or datetime.time(hour=0, minute=0, second=1)
        self.wait_days = self.get_daily_waits(days)

    def get_daily_waits(self, days):
        """Computes how many days to wait till the next run from any day, starting with Monday.
        Example: MF runner:  [4, 3, 2, 1, 3, 2, 1]
        """
        if isinstance(days, int):
            return [days for i in range(7)]

        week = [False for i in range(7)]
        for day in days:
            week[WEEK.index(CONVERT_DAYS[day[0:2].lower()])] = True

        count = week.index(True) + 1
        waits = deque()

        for val in reversed(week):
            waits.appendleft(count)
            count = 1 if val else count + 1
        return waits

    def next_runs(self, job):
        # Find the next time to run
        if job.runs:
            next_day = job.runs[0].run_time + datetime.timedelta(days=self.wait_days[job.runs[0].run_time.weekday()])
        else:
            next_day = timeutils.current_time() + datetime.timedelta(self.wait_days[timeutils.current_time().weekday()])

        run_time = next_day.replace(
                            hour=self.start_time.hour, 
                            minute=self.start_time.minute, 
                            second=self.start_time.second,
                            microsecond=0)

        job_runs = job.build_runs()
        for job_run in job_runs:
            job_run.set_run_time(run_time)
 
        return job_runs
    
    def job_setup(self, job):
        job.queueing = True
    
    def __str__(self):
        return "DAILY"
    
    def __eq__(self, other):
        return isinstance(other, DailyScheduler) and \
           self.wait_days == other.wait_days and self.start_time == other.start_time

    def __ne__(self, other):
        return not self == other


class GrocScheduler(object):
    """Wrapper around SpecificTimeSpecification in the Google App Engine cron library"""
    def __init__(self, ordinals=None, weekdays=None, months=None, monthdays=None,
                 timestr='00:00', timezone=None):
        """Parameters:
          time      - the time of day to run, as 'HH:MM'
          ordinals  - first, second, third &c, as a set of integers in 1..5 to be
                      used with "1st <weekday>", etc.
          monthdays - set of integers to be used with "<month> 3rd", etc.
          months    - the months that this should run, as a set of integers in 1..12
          weekdays  - the days of the week that this should run, as a set of integers,
                      0=Sunday, 6=Saturday
          timezone  - the optional timezone as a string for this specification.
                      Defaults to UTC - valid entries are things like Australia/Victoria
                      or PST8PDT.
        """
        self.ordinals = ordinals
        self.weekdays = weekdays
        self.months = months
        self.monthdays = monthdays
        self.timestr = timestr
        self.timezone = timezone

        self.time_spec = None

    def parse(self, scheduler_str):
        self.string_epr = scheduler_str

        def parse_number(day):
            return int(''.join(c for c in day if c.isdigit()))

        m = GROC_SCHEDULE_RE.match(scheduler_str.lower())

        if m.group('time') is None:
            self.timestr = '00:00'
        else:
            self.timestr = m.group('time')

        if m.group('days') in (None, 'day'):
            self.weekdays = None
        else:
            self.weekdays = set(CONVERT_DAYS_INT[d] for d in m.group('days').split(','))

        self.monthdays = None
        self.ordinals = None
        if m.group('month_days') != 'every':
            values = set(parse_number(n) for n in m.group('month_days').split(','))
            if self.weekdays is None:
                self.monthdays = values
            else:
                self.ordinals = values

        if m.group('months') in (None, 'month'):
            self.months = None
        else:
            self.months = set(CONVERT_MONTHS[mo] for mo in m.group('months').split(','))

        self.time_spec = SpecificTimeSpecification(ordinals=self.ordinals,
                                                   weekdays=self.weekdays,
                                                   months=self.months,
                                                   monthdays=self.monthdays,
                                                   timestr=self.timestr,
                                                   timezone=self.timezone)

    def next_runs(self, job):
        # Find the next time to run
        if job.runs:
            start_time = job.runs[0].run_time
        else:
            start_time = timeutils.current_time()

        run_time = self.time_spec.GetMatch(start_time)

        job_runs = job.build_runs()
        for job_run in job_runs:
            job_run.set_run_time(run_time)
 
        return job_runs
    
    def job_setup(self, job):
        job.queueing = True
    
    def __str__(self):
        return self.string_repr
    
    def __eq__(self, other):
        return isinstance(other, GrocScheduler) and \
           True == True

    def __ne__(self, other):
        return not self == other


class IntervalScheduler(object):
    """The interval scheduler runs a job (to success) based on a configured interval
    """
    def __init__(self, interval=None):
        self.interval = interval
    
    def next_runs(self, job):
        run_time = timeutils.current_time() + self.interval
        
        job_runs = job.build_runs()
        for job_run in job_runs:
            job_run.set_run_time(run_time)
        
        return job_runs

    def job_setup(self, job):
        job.queueing = False
    
    def __str__(self):
        return "INTERVAL:%s" % self.interval
        
    def __eq__(self, other):
        return isinstance(other, IntervalScheduler) and self.interval == other.interval
    
    def __ne__(self, other):
        return not self == other
