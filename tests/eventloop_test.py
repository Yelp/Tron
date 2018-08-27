from __future__ import absolute_import
from __future__ import unicode_literals

import mock

from testifycompat import assert_equal
from testifycompat import setup
from testifycompat import TestCase
from tron import eventloop


class TestUniqueCall(TestCase):
    @setup
    def setup_monitor(self):
        self.func = mock.Mock()
        self.callback = eventloop.UniqueCallback(5, self.func)
        self.callback.delayed_call = mock.Mock()

    def test__init__(self):
        assert_equal(self.callback.delay, 5)
        assert_equal(self.callback.func, self.func)

    def test_start(self):
        self.callback.delayed_call.active.return_value = False
        with mock.patch(
            'tron.eventloop.call_later',
            autospec=True,
        ) as mock_call_later:
            self.callback.start()
            mock_call_later.assert_called_with(
                self.callback.delay,
                self.callback.func,
            )
            assert_equal(
                self.callback.delayed_call,
                mock_call_later.return_value,
            )

    def test_start_already_actice(self):
        self.callback.delayed_call.active.return_value = True
        with mock.patch(
            'tron.eventloop.call_later',
            autospec=True,
        ) as mock_call_later:
            self.callback.start()
            assert not mock_call_later.call_count
