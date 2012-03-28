import StringIO

from testify import setup, TestCase, assert_equal, run
from testify import assert_in, assert_raises, assert_lt
from testify.utils import turtle
from tests import testingutils

from tron import node, action


class NodeTestCase(TestCase):

    class TestConnection(object):
        def openChannel(self, chan):
            self.chan = chan

    @setup
    def setup_node(self):
        self.ssh_options = turtle.Turtle()
        self.node = node.Node('localhost', 'thename', self.ssh_options)
        self.stdout = StringIO.StringIO()
        self.stderr = StringIO.StringIO()

    def test_run_output_logging(self):
        nod = self.node
        action_cmd = action.ActionCommand("test", "false",
                                          stdout=self.stdout,
                                          stderr=self.stderr)

        nod.connection = self.TestConnection()
        nod.run_states = {action_cmd.id: turtle.Turtle(state=0)}
        nod.run_states[action_cmd.id].state = node.RUN_STATE_CONNECTING

        nod._open_channel(action_cmd)
        assert nod.connection.chan is not None
        nod.connection.chan.dataReceived("test")

        self.stdout.seek(0)
        assert_equal(self.stdout.read(4), "test")

    def test_from_config(self):
        node_config = turtle.Turtle(hostname='localhost', name='thename')
        ssh_options = turtle.Turtle()
        new_node = node.Node.from_config(node_config, ssh_options)
        assert_equal(new_node.name, node_config.name)
        assert_equal(new_node.hostname, node_config.hostname)

    def test_next(self):
        for _ in xrange(3):
            assert_equal(self.node.next(), self.node)

    def test_next_round_robin(self):
        for _ in xrange(3):
            assert_equal(self.node.next_round_robin(), self.node)

    def test_nodes(self):
        assert_equal(self.node.nodes, [self.node])

    def test__getitem__(self):
        assert_equal(self.node['thename'], self.node)
        assert_raises(KeyError, lambda: self.node['notthename'])

    def test__cmp__(self):
        other_node = node.Node('mocalhost', 'mocal', self.ssh_options)
        assert_lt(self.node, 'thename')
        assert_lt(self.node, other_node)

    def test_determine_fudge_factor(self):
        assert_equal(self.node._determine_fudge_factor(), 0)

        self.node.run_states = dict((i, i) for i in xrange(20))
        assert 0 < self.node._determine_fudge_factor() < 20


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


class NodePoolTestCase(TestCase):

    @setup
    def setup_nodes(self):
        ssh_options = turtle.Turtle(agent=True)
        self.nodes = [
            node.Node(str(i), 'node%s' % i, ssh_options) for i in xrange(5)
        ]
        self.node_pool = node.NodePool(self.nodes, 'thename')

    def test_from_config(self):
        node_pool_config = turtle.Turtle(
                name='thename', nodes=['node1', 'node3'])

        node_dict = dict((n.name, n) for n in self.nodes)

        new_pool = node.NodePool.from_config(node_pool_config, node_dict)
        assert_equal(new_pool.name, node_pool_config.name)
        assert_equal(len(new_pool.nodes), 2)
        node_names = set(n.name for n in new_pool.nodes)
        assert_equal(set(node_names), set(node_pool_config.nodes))

    def test__init__(self):
        new_node = node.NodePool(self.nodes, 'thename')
        assert_equal(new_node.name, 'thename')

        new_node = node.NodePool(self.nodes)
        assert_equal(new_node.name, 'node0_node1_node2_node3_node4')

    def test__eq__(self):
        other_pool = node.NodePool(self.nodes, 'othername')
        assert_equal(self.node_pool, other_pool)

    def test_next(self):
        # Call next many times
        for _ in xrange(len(self.nodes) * 2 + 1):
            assert_in(self.node_pool.next(), self.nodes)

    def test_next_round_robin(self):
        node_order = [
            self.node_pool.next_round_robin()
            for _ in xrange(len(self.nodes) * 2)
        ]
        assert_equal(node_order, self.nodes + self.nodes)

    def test__getitem__(self):
        assert_equal(self.node_pool['node0'], self.nodes[0])
        assert_equal(self.node_pool['node3'], self.nodes[3])

        assert_raises(KeyError, lambda: self.node_pool['node7'])

    def test_repr_data(self):
        repr_data = self.node_pool.repr_data()
        assert_equal(repr_data['name'], self.node_pool.name)
        assert_equal(len(repr_data['nodes']), len(self.nodes))


if __name__ == '__main__':
    run()
