from testify import *
from testify.utils import turtle

from tron import node
from tron.utils.testing import run_reactor

class NodeTestCase(TestCase):
    @run_reactor()
    def test_stuff(self):
        my_node = node.Node("bastion.yelpcorp.com")

        job = turtle.Turtle()
        job.command = "/bin/false"

        run = turtle.Turtle()
        run.id = "1"
        run.job = job
        run.command = job.command
        
        my_node.run(run)
        df = my_node.run_defer[run.id]
        df.addCallback(self._cb_test_stuff)
        return df

    def _cb_test_stuff(self, conn):
        self.log.info("Job is runned")

if __name__ == '__main__':
    run()