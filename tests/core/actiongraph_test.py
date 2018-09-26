from __future__ import absolute_import
from __future__ import unicode_literals

from unittest import mock

from testifycompat import assert_equal
from testifycompat import assert_raises
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tron.core import actiongraph


class TestActionGraph(TestCase):
    @setup
    def setup_graph(self):
        self.action_names = [
            'base_one',
            'base_two',
            'dep_one',
            'dep_one_one',
            'dep_multi',
        ]
        am = self.action_map = {}
        for name in self.action_names:
            am[name] = mock.MagicMock()
            am[name].name = name

        am['dep_multi'].required_actions = [am['dep_one_one'], am['base_two']]
        am['dep_one_one'].required_actions = [am['dep_one']]
        am['dep_one'].required_actions = [am['base_one']]

        self.graph = [am['base_one'], am['base_two']]
        self.action_graph = actiongraph.ActionGraph(self.graph, am)

    def test_from_config(self):
        config = {}
        for name in self.action_names:
            config[name] = mock.MagicMock(name=name, node='first', requires=[])
            config[name].name = name
            config[name].node = 'first'
            config[name].requires = []
        config['dep_multi'].requires = ['dep_one_one', 'base_two']
        config['dep_one_one'].requires = ['dep_one']
        config['dep_one'].requires = ['base_one']

        built_graph = actiongraph.ActionGraph.from_config(config)
        am = built_graph.action_map

        graph_base_names = {a.name for a in built_graph.graph}
        assert_equal(graph_base_names, {a.name for a in self.graph})
        assert_equal(graph_base_names, {'base_one', 'base_two'})
        assert_equal(
            set(am['dep_multi'].required_actions),
            {'dep_one_one', 'base_two'},
        )

        assert_equal(set(am.keys()), set(self.action_names))
        assert_equal(am['base_one'].dependent_actions, {'dep_one'})
        assert_equal(am['dep_one'].dependent_actions, {'dep_one_one'})

    def test_actions_for_names(self):
        actions = list(
            self.action_graph.actions_for_names(['base_one', 'dep_multi']),
        )
        expected_actions = [
            self.action_map['base_one'],
            self.action_map['dep_multi'],
        ]
        assert_equal(actions, expected_actions)

    def test__getitem__(self):
        assert_equal(
            self.action_graph['base_one'],
            self.action_map['base_one'],
        )

    def test__getitem__miss(self):
        assert_raises(KeyError, lambda: self.action_graph['unknown'])

    def test__eq__(self):
        other_graph = mock.MagicMock(
            graph=self.graph,
            action_map=self.action_map,
        )
        assert_equal(self.action_graph, other_graph)

        other_graph.graph = None
        assert not self.action_graph == other_graph

    def test__ne__(self):
        other_graph = mock.MagicMock()
        assert self.graph != other_graph


if __name__ == "__main__":
    run()
