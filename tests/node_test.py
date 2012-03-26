from testify import setup, TestCase, assert_equal, run
from testify.utils import turtle

from tron import node
from tron.core import actionrun
from tron.utils import testingutils


class NodeTestCase(TestCase):

    class TestConnection(object):
        def openChannel(self, chan):
            self.chan = chan

    def test_output_logging(self):
        nod = node.Node(hostname="localhost",
                        ssh_options=turtle.Turtle())

        fh = turtle.Turtle()
        serializer = turtle.Turtle(open=lambda fn: fh)
        action_cmd = actionrun.ActionCommand("test", "false", serializer)

        nod.connection = self.TestConnection()
        nod.run_states = {action_cmd.id: turtle.Turtle(state=0)}
        nod.run_states[action_cmd.id].state = node.RUN_STATE_CONNECTING

        nod._open_channel(action_cmd)
        assert nod.connection.chan is not None
        nod.connection.chan.dataReceived("test")

        assert_equal(fh.write.calls, [(("test",), {})])

class NodeTimeoutTest(testingutils.ReactorTestCase):
    @setup
    def build_node(self):
        self.node = node.Node(hostname="testnodedoesnotexist",
                              ssh_options=turtle.Turtle())

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

        with testingutils.no_handlers_for_logger():
            testingutils.wait_for_deferred(df)
        assert df.called
        assert self.job_marked_failed


if __name__ == '__main__':
    run()
