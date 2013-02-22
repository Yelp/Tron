import mock
from testify import setup, TestCase, assert_equal, run
from testify import assert_in, assert_raises, assert_lt
from testify.assertions import assert_not_in
from testify.test_case import teardown
from testify.utils import turtle

from twisted.conch.client.options import ConchOptions
from tron import node
from tron.core import actionrun


def create_mock_node(name=None):
    mock_node = mock.create_autospec(node.Node)
    if name:
        mock_node.get_name.return_value = name
    return mock_node


def create_mock_pool():
    return mock.create_autospec(node.NodePool)


class NodePoolStoreTestCase(TestCase):

    @setup
    def setup_store(self):
        self.node = create_mock_node()
        self.store = node.NodePoolStore.get_instance()
        self.store.add(self.node)

    @teardown
    def teardown_store(self):
        node.NodePoolStore.clear()

    def test_single_instance(self):
        assert_raises(ValueError, node.NodePoolStore)
        assert self.store is node.NodePoolStore.get_instance()

    def test_add(self):
        mock_node = create_mock_node()
        self.store.add(mock_node)
        assert_in(mock_node, self.store)

    def test__getitem__(self):
        assert_equal(self.node, self.store[self.node.get_name()])

    def test_get(self):
        assert_equal(self.node, self.store.get(self.node.get_name()))

    def test_get_miss(self):
        assert_equal(None, self.store.get('bogus'))

    def test_clear(self):
        node.NodePoolStore.clear()
        assert_not_in(self.node, self.store)

    def test_filter_by_name(self):
        self.store.add(create_mock_node('a'))
        self.store.add(create_mock_node('b'))
        self.store._filter_by_name(['b', 'c'])
        assert_equal(self.store.nodes.keys(), ['b'])

    @mock.patch('tron.node.NodePool')
    @mock.patch('tron.node.Node')
    def test_update_from_config(self, _mock_node, _mock_node_pool):
        node_config = {'a': mock.Mock()}
        node_pool_config = {'c': mock.Mock()}
        ssh_options = mock.create_autospec(ConchOptions)
        node.NodePoolStore.update_from_config(
            node_config, node_pool_config, ssh_options)

        assert_equal(len(self.store.nodes), 2)


class NodeTestCase(TestCase):

    class TestConnection(object):
        def openChannel(self, chan):
            self.chan = chan

    @setup
    def setup_node(self):
        self.ssh_options = turtle.Turtle()
        self.node = node.Node('localhost', self.ssh_options, username='theuser', name='thename')

    def test_output_logging(self):
        nod = node.Node('localhost', turtle.Turtle(), username='theuser')

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
        other_node = node.Node('mocalhost', self.ssh_options, username='mser', name='mocal')
        assert_lt(self.node, 'thename')
        assert_lt(self.node, other_node)

    def test_determine_fudge_factor(self):
        assert_equal(self.node._determine_fudge_factor(), 0)

        self.node.run_states = dict((i, i) for i in xrange(20))
        assert 0 < self.node._determine_fudge_factor() < 20


class NodePoolTestCase(TestCase):

    @setup
    def setup_nodes(self):
        ssh_options = mock.create_autospec(ConchOptions)
        self.nodes = [
            node.Node(str(i), ssh_options, username='user', name='node%s' % i)
            for i in xrange(5)]
        self.node_pool = node.NodePool(self.nodes, 'thename')

    @mock.patch('tron.node.NodePoolStore')
    def test_from_config(self, mock_pool_store):
        node_names = ['node1', 'node3']
        node_pool_config = mock.Mock(name='thename', nodes=node_names)
        new_pool = node.NodePool.from_config(node_pool_config)
        assert_equal(new_pool.name, node_pool_config.name)
        store = mock_pool_store.get_instance.return_value
        expected = [mock.call(name) for name in node_names]
        assert_equal(store.__getitem__.mock_calls, expected)
        expected = [store.__getitem__.return_value] * 2
        assert_equal(new_pool.nodes, expected)

    def test__init__(self):
        new_node = node.NodePool(self.nodes, 'thename')
        assert_equal(new_node.name, 'thename')

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


if __name__ == '__main__':
    run()
