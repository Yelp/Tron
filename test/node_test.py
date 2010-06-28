import os
import tempfile

from testify import *
from testify.utils import turtle

from tron import node, job
from tron.utils.testingutils import run_reactor

class NodeTestCase(TestCase):
    class TestConnection(object):
        def openChannel(self, chan):
            self.chan = chan

    class TestRunState(object):
        state = 0

    def test_output_logging(self):
        jo = job.Job(name="Test Job")
        jo.command = "echo Hello"

        run = jo.build_run()
        run.output_file = tempfile.TemporaryFile('w+b')
        nod = node.Node(hostname="localhost")
        
        nod.connection = self.TestConnection()
        nod.run_states = {run.id:self.TestRunState()}
        nod.run_states[run.id].state = node.RUN_STATE_CONNECTING

        nod._open_channel(run)
        assert not nod.connection.chan is None
        nod.connection.chan.dataReceived("test")

        run.output_file.seek(0)
        assert run.output_file.read(4) == "test"
        run.output_file.close()


if __name__ == '__main__':
    run()
