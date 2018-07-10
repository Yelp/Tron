from __future__ import absolute_import
from __future__ import unicode_literals

from testify import assert_equal
from testify import run
from testify import setup
from testify import TestCase

from tron.core import actiongraph
from tron.core.action import ActionMap


class ActionGraphTestCase(TestCase):
    @setup
    def setup_graph(self):
        self.action_names = [
            'base_one',
            'base_two',
            'dep_one',
            'dep_one_one',
            'dep_multi',
        ]
        self.action_map = ActionMap.from_config([
            {
                'name': 'base_one',
                'command': ''
            },
            {
                'name': 'base_two',
                'command': ''
            },
            {
                'name': 'dep_one',
                'command': '',
                'requires': ['base_one']
            },
            {
                'name': 'dep_one_one',
                'command': '',
                'requires': ['dep_one'],
            },
            {
                'name': 'dep_multi',
                'command': '',
                'requires': ['dep_one_one', 'base_two'],
            },
        ])

        self.action_graph = actiongraph.ActionGraph.from_config(
            self.action_map
        )

    def test_from_config(self):
        assert_equal(
            set(self.action_graph.graph),
            {'base_two', 'base_one'},
        )
        assert_equal(
            self.action_graph['dep_one'].dependent_actions,
            {'dep_one_one'},
        )
        assert_equal(
            self.action_graph['dep_one_one'].required_actions,
            {'dep_one'},
        )

    def test__eq__(self):
        ag = actiongraph.ActionGraph.from_config(self.action_map)
        assert_equal(self.action_graph, ag)


if __name__ == "__main__":
    run()
