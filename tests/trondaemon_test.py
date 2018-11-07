from __future__ import absolute_import
from __future__ import unicode_literals

import os
import tempfile
from collections import defaultdict

import mock

from testifycompat import setup
from testifycompat import teardown
from testifycompat import TestCase
from tron.trondaemon import TronDaemon


class TronDaemonTestCase(TestCase):
    @setup
    def setup(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        trond_opts = mock.Mock()
        trond_opts.working_dir = self.tmpdir.name
        trond_opts.lock_file = os.path.join(self.tmpdir.name, "lockfile")
        with mock.patch('tron.trondaemon.setup_logging', autospec=True):
            self.trond = TronDaemon(trond_opts)

    @teardown
    def teardown(self):
        self.tmpdir.cleanup()

    @mock.patch('tron.trondaemon.setup_logging', mock.Mock(), autospec=None)
    def test_init(self):
        daemon = TronDaemon.__new__(TronDaemon)  # skip __init__
        daemon._make_sigint_handler = mock.Mock()
        options = mock.Mock()
        _sig_handlers = defaultdict(lambda: 'original_handler')

        with mock.patch(
            'signal.getsignal',
            mock.Mock(side_effect=_sig_handlers.__getitem__),
            autospec=None
        ), mock.patch(
            'signal.signal',
            mock.Mock(side_effect=_sig_handlers.__setitem__),
            autospec=None
        ), mock.patch(
            'tron.utils.flock',
            autospec=True,
        ) as mock_flock:
            daemon.__init__(options)

            assert daemon._make_sigint_handler.call_count == 1
            assert daemon._make_sigint_handler.call_args == mock.call(
                'original_handler'
            )
            assert mock_flock.call_count == 0

    def test_make_sigint_handler_keyboardinterrupt(self):
        daemon = TronDaemon.__new__(TronDaemon)  # skip __init__
        daemon._handle_shutdown = mock.Mock()

        def prev_handler(signum, frame):
            raise KeyboardInterrupt

        handler = daemon._make_sigint_handler(prev_handler)
        handler('fake_signum', 'fake_frame')

        daemon._handle_shutdown.call_args == mock.call(
            'fake_signum',
            'fake_frame',
        )

    def test_make_sigint_handler_exception(self):
        daemon = TronDaemon.__new__(TronDaemon)  # skip __init__
        daemon._handle_shutdown = mock.Mock()

        def prev_handler(signum, frame):
            raise Exception

        handler = daemon._make_sigint_handler(prev_handler)
        handler('fake_signum', 'fake_frame')

        daemon._handle_shutdown.call_count == 0

    def test_run_manhole_new_manhole(self):
        with open(self.trond.manhole_sock, 'w+'):
            pass

        with mock.patch(
            'twisted.internet.reactor.listenUNIX',
            autospec=True,
        ) as mock_listenUNIX:
            self.trond._run_manhole()

            assert mock_listenUNIX.call_count == 1
            # _run_manhole will remove the old manhole.sock but not recreate
            # it because we mocked out listenUNIX
            assert not os.path.exists(self.trond.manhole_sock)
