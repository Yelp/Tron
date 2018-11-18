from __future__ import absolute_import
from __future__ import unicode_literals

import os
import tempfile

import mock

from testifycompat import setup
from testifycompat import teardown
from testifycompat import TestCase
from tron.trondaemon import TronDaemon


class TronDaemonTestCase(TestCase):
    @setup
    @mock.patch('tron.trondaemon.setup_logging', mock.Mock(), autospec=None)
    @mock.patch('signal.signal', mock.Mock(), autospec=None)
    def setup(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        trond_opts = mock.Mock()
        trond_opts.working_dir = self.tmpdir.name
        trond_opts.lock_file = os.path.join(self.tmpdir.name, "lockfile")
        self.trond = TronDaemon(trond_opts)

    @teardown
    def teardown(self):
        self.tmpdir.cleanup()

    @mock.patch('tron.trondaemon.setup_logging', mock.Mock(), autospec=None)
    @mock.patch('signal.signal', mock.Mock(), autospec=None)
    def test_init(self):
        daemon = TronDaemon.__new__(TronDaemon)  # skip __init__
        options = mock.Mock()

        with mock.patch(
            'tron.utils.flockfile.FlockFile',
            autospec=True,
        ) as mock_flockfile:
            daemon.__init__(options)

            assert mock_flockfile.call_count == 1
            assert daemon.context.lockfile == mock_flockfile.return_value

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
