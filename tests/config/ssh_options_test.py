import mock
from pyrsistent import InvariantException
from testify import TestCase

from tests.assertions import assert_raises
from tron.config.ssh_options import SSHOptions


class SSHOptionsTestCase(TestCase):
    @mock.patch('tron.config.ssh_options.os.environ', {})
    def test_post_validation_failed(self):
        assert_raises(InvariantException, SSHOptions, agent=True)

    @mock.patch(
        'tron.config.ssh_options.os.environ', {'SSH_AUTH_SOCK': 'something'}
    )
    def test_post_validation_success(self):
        config = SSHOptions(agent=True)
        assert (config.agent)
