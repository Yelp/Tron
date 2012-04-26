import tempfile

from testify import TestCase, setup, teardown
from testify import  assert_equal, run
from testify.utils import turtle

from tron import mcp


class MasterControlProgramTestCase(TestCase):

    @setup
    def setup_mcp(self):
        self.working_dir = tempfile.mkdtemp()
        config_file = tempfile.NamedTemporaryFile(dir=self.working_dir)
        self.mcp = mcp.MasterControlProgram(self.working_dir, config_file.name)

    @teardown
    def teardown_mcp(self):
        self.mcp.nodes.clear()
        self.mcp.event_manager.clear()

    def test_ssh_options_from_config(self):
        ssh_conf = turtle.Turtle(agent=False, identities=[])
        ssh_options = self.mcp._ssh_options_from_config(ssh_conf)

        assert_equal(ssh_options['agent'], False)
        assert_equal(ssh_options.identitys, [])
        # TODO: tests with agent and identities

if __name__ == '__main__':
    run()
