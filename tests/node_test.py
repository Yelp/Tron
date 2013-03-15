import mock
from testify import setup, TestCase, assert_equal, run
from testify import assert_in, assert_raises
from testify.assertions import assert_not_in, assert_not_equal
from testify.test_case import teardown

from twisted.conch.client.options import ConchOptions
from tron import node
from tron.core import actionrun
from tron.serialize import filehandler


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
        ssh_options = mock.Mock(identities=[], known_hosts_file=None)
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


class KnownHostTestCase(TestCase):

    @setup
    def setup_known_hosts(self):
        self.known_hosts = node.KnownHosts(None)
        self.entry = mock.Mock()
        self.known_hosts._entries.append(self.entry)

    def test_get_public_key(self):
        hostname = 'hostname'
        pub_key = self.known_hosts.get_public_key(hostname)
        self.entry.matchesHost.assert_called_with(hostname)
        assert_equal(pub_key, self.entry.publicKey)

    def test_get_public_key_not_found(self):
        self.entry.matchesHost.return_value = False
        assert not self.known_hosts.get_public_key('hostname')


class NodeTestCase(TestCase):

    class TestConnection(object):
        def openChannel(self, chan):
            self.chan = chan

    @setup
    def setup_node(self):
        self.ssh_options = mock.create_autospec(ConchOptions)
        self.node = node.Node('localhost', self.ssh_options, username='theuser', name='thename')

    def test_output_logging(self):
        nod = node.Node('localhost', mock.Mock(), username='theuser')
        serializer = mock.create_autospec(filehandler.FileHandleManager)
        action_cmd = actionrun.ActionCommand("test", "false", serializer)

        nod.connection = self.TestConnection()
        nod.run_states = {action_cmd.id: mock.Mock(state=0)}
        nod.run_states[action_cmd.id].state = node.RUN_STATE_CONNECTING

        nod._open_channel(action_cmd)
        assert nod.connection.chan is not None
        nod.connection.chan.dataReceived("test")
        serializer.open.return_value.write.assert_called_with('test')

    def test_from_config(self):
        node_config = mock.Mock(hostname='localhost', username='theuser', name='thename')
        self.ssh_options.__getitem__.return_value = 'something'
        public_key = mock.Mock()
        new_node = node.Node.from_config(node_config, self.ssh_options, public_key)
        assert_equal(new_node.name, node_config.name)
        assert_equal(new_node.hostname, node_config.hostname)
        assert_equal(new_node.username, node_config.username)
        assert_equal(new_node.pub_key, public_key)

    def test_determine_fudge_factor(self):
        assert_equal(node.determine_fudge_factor(0), 0)
        assert 0 < node.determine_fudge_factor(20) < 20

    def test__eq__true(self):
        other_node = node.Node('localhost', self.ssh_options,
            username='theuser', name='thename')
        assert_equal(other_node, self.node)

    def test__eq__false(self):
        other_node = node.Node('localhost', self.ssh_options,
            username='different', name='thename')
        assert_not_equal(other_node, self.node)
        other_node = node.Node('localhost', self.ssh_options,
            username='theuser', name='thename', pub_key="something")
        assert_not_equal(other_node, self.node)


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


class BuildSSHOptionsFromConfigTestCase(TestCase):

    def test_build_ssh_options_from_config_none(self):
        ssh_conf = mock.Mock(agent=False, identities=[])
        ssh_options = node.build_ssh_options_from_config(ssh_conf)
        assert_equal(ssh_options['agent'], False)
        assert_equal(ssh_options['noagent'], True)
        assert_equal(ssh_options.identitys, [])

    def test_build_ssh_options_from_config_both(self):
        identities = ['one', 'two']
        ssh_conf = mock.Mock(agent=True, identities=identities)
        ssh_options = node.build_ssh_options_from_config(ssh_conf)
        assert_equal(ssh_options['agent'], True)
        assert_equal(ssh_options['noagent'], False)
        assert_equal(ssh_options.identitys, identities)


if __name__ == '__main__':
    run()
