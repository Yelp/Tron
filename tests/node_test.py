from testify import setup, TestCase, assert_equal, run
from testify import assert_in, assert_raises, assert_lt
from testify.assertions import assert_not_in
from testify.utils import turtle

from tron import node
from tron.core import actionrun


class NodePoolStore(TestCase):

    @setup
    def setup_store(self):
        self.node = turtle.Turtle()
        self.store = node.NodePoolStore.get_instance()
        self.store.put(self.node)

    def test_single_instance(self):
        assert_raises(ValueError, node.NodePoolStore)
        assert self.store is node.NodePoolStore.get_instance()

    def test_put(self):
        n = turtle.Turtle()
        self.store.put(n)
        assert_in(n.name, self.store)

    def test_update(self):
        nodes = [turtle.Turtle(), turtle.Turtle()]
        self.store.update(nodes)
        for n in nodes:
            assert_in(n.name, self.store)

    def test__getitem__(self):
        assert_equal(self.node, self.store[self.node.name])

    def test_get(self):
        assert_equal(self.node, self.store.get(self.node.name))

    def test_get_miss(self):
        assert_equal(None, self.store.get('bogus'))

    def test_clear(self):
        self.store.clear()
        assert_not_in(self.node, self.store)

class NodeTestCase(TestCase):

    class TestConnection(object):
        def openChannel(self, chan):
            self.chan = chan

    @setup
    def setup_node(self):
        self.ssh_options = turtle.Turtle()
        self.node = node.Node(hostname='localhost', username='theuser', name='thename', ssh_options=self.ssh_options)

    def test_output_logging(self):
        nod = node.Node(hostname="localhost", username='theuser',
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

    def test_from_config(self):
        node_config = turtle.Turtle(hostname='localhost', username='theuser', name='thename')
        ssh_options = turtle.Turtle()
        new_node = node.Node.from_config(node_config, ssh_options)
        assert_equal(new_node.name, node_config.name)
        assert_equal(new_node.hostname, node_config.hostname)
        assert_equal(new_node.username, node_config.username)

    def test_next(self):
        for _ in xrange(3):
            assert_equal(self.node.next(), self.node)

    def test_next_round_robin(self):
        for _ in xrange(3):
            assert_equal(self.node.next_round_robin(), self.node)

    def test_nodes(self):
        assert_equal(self.node.nodes, [self.node])

    def test__getitem__(self):
        assert_equal(self.node['localhost'], self.node)
        assert_raises(KeyError, lambda: self.node['thename'])

    def test__cmp__(self):
        other_node = node.Node(hostname='mocalhost', username='mser', name='mocal', ssh_options=self.ssh_options)
        assert_lt(self.node, 'thename')
        assert_lt(self.node, other_node)

    def test_determine_fudge_factor(self):
        assert_equal(self.node._determine_fudge_factor(), 0)

        self.node.run_states = dict((i, i) for i in xrange(20))
        assert 0 < self.node._determine_fudge_factor() < 20


class NodePoolTestCase(TestCase):

    @setup
    def setup_nodes(self):
        ssh_options = turtle.Turtle(agent=True)
        self.nodes = [
            node.Node(hostname=str(i), username='user', name='node%s' % i, ssh_options=ssh_options) for i in xrange(5)
        ]
        self.node_pool = node.NodePool(self.nodes, 'thename')

    def test_from_config(self):
        node_pool_config = turtle.Turtle(
                name='thename', nodes=['node1', 'node3'])

        node.NodePoolStore.get_instance().update(self.nodes)

        new_pool = node.NodePool.from_config(node_pool_config)
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
        assert_equal(self.node_pool['0'], self.nodes[0])
        assert_equal(self.node_pool['3'], self.nodes[3])

        assert_raises(KeyError, lambda: self.node_pool['node0'])

    def test_repr_data(self):
        repr_data = self.node_pool.repr_data()
        assert_equal(repr_data['name'], self.node_pool.name)
        assert_equal(len(repr_data['nodes']), len(self.nodes))


if __name__ == '__main__':
    run()
