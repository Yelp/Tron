import os
import tempfile
import shutil
import StringIO

from testify import *
from testify.utils import turtle

from tron import node, action, job
from tron.utils import testingutils


class NodeTestCase(TestCase):

    class TestConnection(object):
        def openChannel(self, chan):
            self.chan = chan

    @setup
    def setup(self):
        self.stdout = StringIO.StringIO()
        self.stderr = StringIO.StringIO()

    def test_output_logging(self):
        nod = node.Node(hostname="localhost")

        action_cmd = action.ActionCommand("test", "false", stdout=self.stdout, stderr=self.stderr)

        nod.connection = self.TestConnection()
        nod.run_states = {action_cmd.id:turtle.Turtle(state=0)}
        nod.run_states[action_cmd.id].state = node.RUN_STATE_CONNECTING

        nod._open_channel(action_cmd)
        assert not nod.connection.chan is None
        nod.connection.chan.dataReceived("test")

        self.stdout.seek(0)
        assert_equal(self.stdout.read(4), "test")


class NodeTimeoutTest(testingutils.ReactorTestCase):
    @setup
    def build_node(self):
        self.node = node.Node(hostname="testnodedoesnotexist")
        self.node.conch_options = turtle.Turtle()

        # Make this test faster
        node.CONNECT_TIMEOUT = 1

    @setup
    def build_run(self):
        self.run = turtle.Turtle()

    def test_connect_timeout(self):
        self.job_marked_failed = False
        def fail_job(*args):
            self.job_marked_failed = True

        df = self.node.run(self.run)
        df.addErrback(fail_job)

        testingutils.wait_for_deferred(df)
        assert df.called
        assert self.job_marked_failed


if __name__ == '__main__':
    run()
