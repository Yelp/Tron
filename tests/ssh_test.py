from __future__ import absolute_import
from __future__ import unicode_literals

import mock
from twisted.python import failure

from testifycompat import assert_equal
from testifycompat import assert_not_equal
from testifycompat import setup
from testifycompat import TestCase
from tests.testingutils import autospec_method
from tron import ssh


class TestClientTransport(TestCase):
    @setup
    def setup_transport(self):
        self.username = 'username'
        self.options = mock.Mock()
        self.expected_pub_key = mock.Mock()
        self.transport = ssh.ClientTransport(
            self.username,
            self.options,
            self.expected_pub_key,
        )

    def test_verifyHostKey_missing_pub_key(self):
        self.transport.expected_pub_key = None
        result = self.transport.verifyHostKey(mock.Mock(), mock.Mock())
        assert_equal(result.result, 1)

    @mock.patch('tron.ssh.keys', autospec=True)
    def test_verifyHostKey_matching_pub_key(self, mock_keys):
        mock_keys.Key.fromString.return_value = self.expected_pub_key
        public_key = mock.Mock()
        result = self.transport.verifyHostKey(public_key, mock.Mock())
        assert_equal(result.result, 2)
        mock_keys.Key.fromString.assert_called_with(public_key)

    @mock.patch('tron.ssh.keys', autospec=True)
    def test_verifyHostKey_mismatch_pub_key(self, _):
        public_key = mock.Mock()
        result = self.transport.verifyHostKey(public_key, mock.Mock())
        assert isinstance(result.result, failure.Failure)

    def test_connnectionSecure(self):
        self.transport.connection_defer = mock.Mock()
        autospec_method(self.transport.requestService)
        self.transport.connectionSecure()
        conn = self.transport.connection_defer.mock_calls[0][1][0]
        assert isinstance(conn, ssh.ClientConnection)
        auth_service = self.transport.requestService.mock_calls[0][1][0]
        assert isinstance(auth_service, ssh.NoPasswordAuthClient)


class TestSSHAuthOptions(TestCase):
    def test_from_config_none(self):
        ssh_conf = mock.Mock(agent=False, identities=[])
        ssh_options = ssh.SSHAuthOptions.from_config(ssh_conf)
        assert_equal(ssh_options['noagent'], True)
        assert_equal(ssh_options.identitys, [])

    def test_from_config_both(self):
        identities = ['one', 'two']
        ssh_conf = mock.Mock(agent=True, identities=identities)
        ssh_options = ssh.SSHAuthOptions.from_config(ssh_conf)
        assert_equal(ssh_options['noagent'], False)
        assert_equal(ssh_options.identitys, identities)

    def test__eq__true(self):
        config = mock.Mock(agent=True, identities=['one', 'two'])
        assert_equal(
            ssh.SSHAuthOptions.from_config(config),
            ssh.SSHAuthOptions.from_config(config),
        )

    def test__eq__false(self):
        config = mock.Mock(agent=True, identities=['one', 'two'])
        second_config = mock.Mock(agent=True, identities=['two'])
        assert_not_equal(
            ssh.SSHAuthOptions.from_config(config),
            ssh.SSHAuthOptions.from_config(second_config),
        )
