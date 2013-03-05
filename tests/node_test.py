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


class NodePoolRepositoryTestCase(TestCase):

    @setup
    def setup_store(self):
        self.node = create_mock_node()
        self.repo = node.NodePoolRepository.get_instance()
        self.repo.add_node(self.node)

    @teardown
    def teardown_store(self):
        self.repo.clear()

    def test_single_instance(self):
        assert_raises(ValueError, node.NodePoolRepository)
        assert self.repo is node.NodePoolRepository.get_instance()

    def test_get_by_name(self):
        node_pool = self.repo.get_by_name(self.node.get_name())
        assert_equal(self.node, node_pool.next())

    def test_get_by_name_miss(self):
        assert_equal(None, self.repo.get_by_name('bogus'))

    def test_clear(self):
        self.repo.clear()
        assert_not_in(self.node, self.repo.nodes)
        assert_not_in(self.node, self.repo.pools)

    def test_update_from_config(self):
        mock_nodes = {'a': create_mock_node('a'), 'b': create_mock_node('b')}
        self.repo.nodes.update(mock_nodes)
        node_config = {'a': mock.Mock(), 'b': mock.Mock()}
        node_pool_config = {'c': mock.Mock(nodes=['a', 'b'])}
        ssh_options = mock.create_autospec(ConchOptions)
        node.NodePoolRepository.update_from_config(
            node_config, node_pool_config, ssh_options)
        node_names = [node_config['a'].name, node_config['b'].name]
        assert_equal(set(self.repo.pools), set(node_names + [node_pool_config['c'].name]))
        assert_equal(set(self.repo.nodes), set(node_names + mock_nodes.keys()))

    def test_nodes_by_name(self):
        mock_nodes = {'a': mock.Mock(), 'b': mock.Mock()}
        self.repo.nodes.update(mock_nodes)
        nodes = self.repo._get_nodes_by_name(['a', 'b'])
        assert_equal(nodes, mock_nodes.values())

    def test_get_node(self):
        returned_node = self.repo.get_node(self.node.get_name())
        assert_equal(returned_node, self.node)


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

    def test_from_config(self):
        name = 'the pool name'
        nodes = [create_mock_node(), create_mock_node()]
        config = mock.Mock(name=name)
        new_pool = node.NodePool.from_config(config, nodes)
        assert_equal(new_pool.name, config.name)
        assert_equal(new_pool.nodes, nodes)

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


if __name__ == '__main__':
    run()
