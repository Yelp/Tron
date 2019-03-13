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
        self.action_map = {}
        for name in self.action_names:
            self.action_map[name] = mock.MagicMock()
            self.action_map[name].name = name

        self.required_actions = {
            'base_one': set(),
            'base_two': set(),
            'dep_multi': {'dep_one_one', 'base_two'},
            'dep_one_one': {'dep_one'},
            'dep_one': {'base_one'},
        }
        self.required_triggers = {
            'base_one': {'MASTER.otherjob.first'},
            'base_two': set(),
            'dep_multi': set(),
            'dep_one_one': set(),
            'dep_one': set(),
        }

        self.action_graph = actiongraph.ActionGraph(self.action_map, self.required_actions, self.required_triggers)

    def test_get_dependencies(self):
        assert self.action_graph.get_dependencies('not_in_job') == []
        assert self.action_graph.get_dependencies('base_one') == []
        assert self.action_graph.get_dependencies('base_one', include_triggers=True)[0].name == 'MASTER.otherjob.first'
        assert sorted([d.name for d in self.action_graph.get_dependencies('dep_multi')]) == sorted([
            'dep_one_one', 'base_two',
        ])

    def test_names(self):
        assert sorted(self.action_graph.names()) == sorted(self.action_names)
        assert sorted(self.action_graph.names(include_triggers=True)) == sorted(
            self.action_names + ['MASTER.otherjob.first']
        )

    def test__getitem__(self):
        assert_equal(
            self.action_graph['base_one'],
            self.action_map['base_one'],
        )

    def test__getitem__miss(self):
        assert_raises(KeyError, lambda: self.action_graph['unknown'])

    def test__eq__(self):
        other_graph = mock.MagicMock(
            action_map=self.action_map,
            required_actions=self.required_actions,
            required_triggers=self.required_triggers,
        )
        assert_equal(self.action_graph, other_graph)

        other_graph.required_actions = None
        assert not self.action_graph == other_graph

    def test__ne__(self):
        other_graph = mock.MagicMock()
        assert self.action_graph != other_graph


if __name__ == "__main__":
    run()
