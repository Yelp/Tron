from testify import setup, run, TestCase, assert_equal, turtle

from tron.core import actiongraph

class ActionGraphTestCase(TestCase):

    @setup
    def setup_graph(self):
        self.action_names = [
            'base_one', 'base_two', 'dep_one', 'dep_one_one', 'dep_multi']
        am = self.action_map = dict(
            (name, turtle.Turtle(name=name)) for name in self.action_names)

        am['dep_multi'].required_actions   = [am['dep_one_one'], am['base_two']]
        am['dep_one_one'].required_actions = [am['dep_one']]
        am['dep_one'].required_actions     = [am['base_one']]

        self.graph = [am['base_one'], am['base_two']]
        self.action_graph = actiongraph.ActionGraph(self.graph, am)

    def test_from_config(self):
        nodes = dict(first=turtle.Turtle())
        config = dict(
            (name, turtle.Turtle(name=name, node='first', requires=[]))
            for name in self.action_names
        )
        config['dep_multi'].requires    = ['dep_one_one', 'base_two']
        config['dep_one_one'].requires  = ['dep_one']
        config['dep_one'].requires      = ['base_one']

        built_graph = actiongraph.ActionGraph.from_config(config, nodes)
        am = built_graph.action_map

        graph_base_names = set(a.name for a in built_graph.graph)
        assert_equal(graph_base_names, set(a.name for a in self.graph))
        assert_equal(graph_base_names, set(['base_one', 'base_two']))
        assert_equal(
            set(am['dep_multi'].required_actions),
            set([am['dep_one_one'], am['base_two']])
        )

        assert_equal(set(am.keys()), set(self.action_names))
        assert_equal(am['base_one'].dependent_actions, [am['dep_one']])
        assert_equal(am['dep_one'].dependent_actions, [am['dep_one_one']])



if __name__ == "__main__":
    run()