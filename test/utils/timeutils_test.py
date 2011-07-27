from datetime import *
from testify import *

from tron.utils.timeutils import *

class TimeDeltaTestCase(TestCase):

    @setup
    def make_dates(self):
        self.start_nonleap = datetime.datetime(year=2011, month=1, day=1)
        self.end_nonleap = datetime.datetime(year=2011, month=12, day=31)
        self.begin_feb_nonleap = datetime.datetime(year=2011, month=2, day=1)
        self.start_leap = datetime.datetime(year=2012, month=1, day=1)
        self.end_leap = datetime.datetime(year=2012, month=12, day=31)
        self.begin_feb_leap = datetime.datetime(year=2012, month=2, day=1)

    def check_delta(self, start, target, years=0, months=0, days=0):
        assert_equal(start + macro_timedelta(start, years=years, months=months, days=days),
                     target)

    def test_days(self):
        self.check_delta(self.start_nonleap,
                         datetime.datetime(year=2011, month=1, day=11),
                         days=10)
        self.check_delta(self.end_nonleap,
                         datetime.datetime(year=2012, month=1, day=10),
                         days=10)
        self.check_delta(self.start_leap,
                         datetime.datetime(year=2012, month=1, day=11),
                         days=10)
        self.check_delta(self.end_leap,
                         datetime.datetime(year=2013, month=1, day=10),
                         days=10)
        self.check_delta(self.begin_feb_nonleap,
                         datetime.datetime(year=2011, month=3, day=1),
                         days=28)
        self.check_delta(self.begin_feb_leap,
                         datetime.datetime(year=2012, month=3, day=1),
                         days=29)

    def test_months(self):
        self.check_delta(self.start_nonleap,
                         datetime.datetime(year=2011, month=11, day=1),
                         months=10)
        self.check_delta(self.end_nonleap,
                         datetime.datetime(year=2012, month=10, day=31),
                         months=10)
        self.check_delta(self.start_leap,
                         datetime.datetime(year=2012, month=11, day=1),
                         months=10)
        self.check_delta(self.end_leap,
                         datetime.datetime(year=2013, month=10, day=31),
                         months=10)
        self.check_delta(self.begin_feb_nonleap,
                         datetime.datetime(year=2011, month=12, day=1),
                         months=10)
        self.check_delta(self.begin_feb_leap,
                         datetime.datetime(year=2012, month=12, day=1),
                         months=10)

    def test_years(self):
        self.check_delta(self.start_nonleap,
                         datetime.datetime(year=2015, month=1, day=1),
                         years=4)
        self.check_delta(self.end_nonleap,
                         datetime.datetime(year=2015, month=12, day=31),
                         years=4)
        self.check_delta(self.start_leap,
                         datetime.datetime(year=2016, month=1, day=1),
                         years=4)
        self.check_delta(self.end_leap,
                         datetime.datetime(year=2016, month=12, day=31),
                         years=4)
        self.check_delta(self.begin_feb_nonleap,
                         datetime.datetime(year=2015, month=2, day=1),
                         years=4)
        self.check_delta(self.begin_feb_leap,
                         datetime.datetime(year=2016, month=2, day=1),
                         years=4)
