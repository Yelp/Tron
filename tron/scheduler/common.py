from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import random

from tron.utils import timeutils


def get_jitter(time_delta):
    if not time_delta:
        return datetime.timedelta()
    seconds = timeutils.delta_total_seconds(time_delta)
    return datetime.timedelta(seconds=random.randint(-seconds, seconds))


def get_jitter_str(time_delta):
    if not time_delta:
        return ''
    return ' (+/- %s)' % time_delta
