from __future__ import absolute_import
from __future__ import unicode_literals

import mock

from testifycompat import assert_equal
from testifycompat import assert_in
from testifycompat import assert_not_equal
from testifycompat import assert_not_in
from testifycompat import assert_raises
from testifycompat import run
from testifycompat import setup
from testifycompat import setup_teardown
from testifycompat import teardown
from testifycompat import TestCase
from tests.testingutils import autospec_method
from tron import actioncommand
from tron import node
from tron import ssh
from tron.config import schema
from tron.core import actionrun
from tron.serialize import filehandler


def create_mock_node(name=None):
    mock_node = mock.create_autospec(node.Node)
    if name:
        mock_node.get_name.return_value = name
    return mock_node


def create_mock_pool():
    return mock.create_autospec(node.NodePool)


class TestNodePoolRepository(TestCase):
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
            node_config,
            node_pool_config,
            ssh_options,
        )
        node_names = [node_config['a'].name, node_config['b'].name]
        assert_equal(
            set(self.repo.pools),
            set(node_names + [node_pool_config['c'].name], ),
        )
        assert_equal(
            set(self.repo.nodes),
            set(list(node_names) + list(mock_nodes.keys())),
        )

    def test_nodes_by_name(self):
        mock_nodes = {'a': mock.Mock(), 'b': mock.Mock()}
        self.repo.nodes.update(mock_nodes)
        nodes = self.repo._get_nodes_by_name(['a', 'b'])
        assert_equal(nodes, list(mock_nodes.values()))

    def test_get_node(self):
        returned_node = self.repo.get_node(self.node.get_name())
        assert_equal(returned_node, self.node)


class TestKnownHost(TestCase):
    @setup
    def setup_known_hosts(self):
        self.known_hosts = node.KnownHosts(None)
        self.entry = mock.Mock()
        self.known_hosts._added.append(self.entry)

    def test_get_public_key(self):
        hostname = 'hostname'
        pub_key = self.known_hosts.get_public_key(hostname)
        self.entry.matchesHost.assert_called_with(hostname)
        assert_equal(pub_key, self.entry.publicKey)

    def test_get_public_key_not_found(self):
        self.entry.matchesHost.return_value = False
        assert not self.known_hosts.get_public_key('hostname')


class TestDetermineJitter(TestCase):
    @setup
    def setup_node_settings(self):
        self.settings = mock.Mock(
            jitter_load_factor=1,
            jitter_min_load=4,
            jitter_max_delay=20,
        )

    @setup_teardown
    def patch_random(self):
        with mock.patch('tron.node.random', autospec=True) as mock_random:
            mock_random.random.return_value = 1
            yield

    def test_jitter_under_min_load(self):
        assert_equal(node.determine_jitter(3, self.settings), 0)
        assert_equal(node.determine_jitter(4, self.settings), 0)

    def test_jitter_with_load_factor(self):
        self.settings.jitter_load_factor = 2
        assert_equal(node.determine_jitter(3, self.settings), 2.0)
        assert_equal(node.determine_jitter(2, self.settings), 0)

    def test_jitter_with_max_delay(self):
        self.settings.jitter_max_delay = 15
        assert_equal(node.determine_jitter(20, self.settings), 15.0)
        assert_equal(node.determine_jitter(100, self.settings), 15.0)


def build_node(
    hostname='localhost',
    username='theuser',
    name='thename',
    pub_key=None,
):
    config = mock.Mock(hostname=hostname, username=username, name=name)
    ssh_opts = mock.create_autospec(ssh.SSHAuthOptions)
    node_settings = mock.create_autospec(schema.ConfigSSHOptions)
    return node.Node(config, ssh_opts, pub_key, node_settings)


class TestNode(TestCase):
    class TestConnection(object):
        def openChannel(self, chan):
            self.chan = chan

    @setup
    def setup_node(self):
        self.node = build_node()

    def test_output_logging(self):
        test_node = build_node()
        serializer = mock.create_autospec(filehandler.FileHandleManager)
        action_cmd = actionrun.ActionCommand("test", "false", serializer)

        test_node.connection = self.TestConnection()
        test_node.run_states = {action_cmd.id: mock.Mock(state=0)}
        test_node.run_states[action_cmd.id].state = node.RUN_STATE_CONNECTING
        test_node.run_states[action_cmd.id].run = action_cmd

        test_node._open_channel(action_cmd)
        assert test_node.connection.chan is not None
        test_node.connection.chan.dataReceived("test")
        serializer.open.return_value.write.assert_called_with('test')

    def test_from_config(self):
        ssh_options = self.node.conch_options
        node_config = mock.Mock(
            hostname='localhost',
            username='theuser',
            name='thename',
        )
        ssh_options.__getitem__.return_value = 'something'
        public_key = mock.Mock()
        node_settings = mock.Mock()
        new_node = node.Node.from_config(
            node_config,
            ssh_options,
            public_key,
            node_settings,
        )
        assert_equal(new_node.name, node_config.name)
        assert_equal(new_node.hostname, node_config.hostname)
        assert_equal(new_node.username, node_config.username)
        assert_equal(new_node.pub_key, public_key)
        assert_equal(new_node.node_settings, node_settings)

    def test__eq__true(self):
        other_node = build_node()
        other_node.conch_options = self.node.conch_options
        other_node.node_settings = self.node.node_settings
        other_node.config = self.node.config
        assert_equal(other_node, self.node)

    def test__eq__false_config_changed(self):
        other_node = build_node(username='different')
        assert_not_equal(other_node, self.node)

    def test__eq__false_pub_key_changed(self):
        other_node = build_node(pub_key='something')
        assert_not_equal(other_node, self.node)

    def test__eq__false_ssh_options_changed(self):
        other_node = build_node()
        other_node.conch_options = mock.create_autospec(ssh.SSHAuthOptions)
        assert_not_equal(other_node, self.node)

    def test_stop_not_tracked(self):
        action_command = mock.create_autospec(
            actioncommand.ActionCommand,
            id=mock.Mock(),
        )
        self.node.stop(action_command)

    def test_stop(self):
        autospec_method(self.node._fail_run)
        action_command = mock.create_autospec(
            actioncommand.ActionCommand,
            id=mock.Mock(),
        )
        self.node.run_states[action_command.id] = mock.Mock()
        self.node.stop(action_command)
        assert_equal(self.node._fail_run.call_count, 1)


class TestNodePool(TestCase):
    @setup
    def setup_nodes(self):
        self.nodes = [build_node(name='node%s' % i) for i in range(5)]
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
        for _ in range(len(self.nodes) * 2 + 1):
            assert_in(self.node_pool.next(), self.nodes)

    def test_next_round_robin(self):
        node_order = [
            self.node_pool.next_round_robin()
            for _ in range(len(self.nodes) * 2)
        ]
        assert_equal(node_order, self.nodes + self.nodes)


if __name__ == '__main__':
    run()
