from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from unittest import mock

from testifycompat import assert_equal
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tests.assertions import assert_call
from tests.assertions import assert_length
from tron import crash_reporter


class TestError(Exception):
    pass


class TestSimpleDeferred(object):
    @setup
    def setup_crash_reporter(self):
        self.emailer = mock.MagicMock()
        self.emailer.send = mock.MagicMock()
        self.mcp = mock.MagicMock()
        self.reporter = crash_reporter.CrashReporter(self.emailer)
        self.event_dict = {'isError': False, 'message': ['']}

    def test__init__(self):
        assert_equal(self.reporter.emailer, self.emailer)
        assert self.reporter.event_recorder

    def test_get_level(self):
        event_dict = {'logLevel': 'WHAT'}
        assert_equal(self.reporter._get_level(event_dict), 'WHAT')

    def test_get_level_error(self):
        event_dict = {'isError': True}
        assert_equal(self.reporter._get_level(event_dict), logging.ERROR)

    def test_get_level_default(self):
        assert_equal(self.reporter._get_level(self.event_dict), logging.INFO)

    def test_emit_no_text(self):
        self.reporter.emit(self.event_dict)
        assert_length(self.emailer.send.mock_calls, 0)

    def test_emit_unhandled(self):
        self.event_dict['message'] = ["Unhandled error in Deferred:"]
        self.event_dict['isError'] = True
        self.reporter.emit(self.event_dict)
        assert_length(self.emailer.send.mock_calls, 0)

    def test_emit_ignored_level(self):
        self.event_dict['message'] = "Some message."
        self.reporter.emit(self.event_dict)
        assert_length(self.emailer.send.mock_calls, 0)

    def test_emit_crash(self):
        self.event_dict['message'] = ["Ooops"]
        self.event_dict['isError'] = True
        self.reporter.emit(self.event_dict)
        assert_call(self.emailer.send, 0, "Ooops")


if __name__ == '__main__':
    run()
