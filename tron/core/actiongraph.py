import logging
from tron.action import Action

log = logging.getLogger('tron.core.actiongraph')


class ActionGraph(object):
    """A directed graph of actions and their requirements."""

    def __init__(self, graph, action_map):
        self.graph = graph
        self.action_map = action_map

    @classmethod
    def from_config(cls, actions_config, nodes):
        """Create this graph from a job config."""
        actions = dict(
            (name, Action.from_config(conf, nodes))
            for name, conf in actions_config.iteritems()
        )
        graph = cls._build_dag(actions, actions_config)
        return cls(graph, actions)

    @classmethod
    def _build_dag(cls, actions, actions_config):
        """Return a directed graph from a dict of actions keyed by name."""
        base = []
        for action in actions.itervalues():
            dependencies = actions_config[action.name].requires
            if not dependencies:
                base.append(action)
                continue

            for dependency in dependencies:
                action.required_actions.append(actions[dependency])
        return base

    def names(self):
        """Returns all the names for the actions in this graph."""
        return self.action_map.keys()

    def build_action_runs(self, job_run):
        """Build actions and setup requirements"""
        return ActionRunCollection.for_job_run_and_action(job_run, self)


class ActionRunCollection(object):
    """A collection of ActionRuns used a JobRun."""

    def __init__(self, runs, graph):
        self.runs = runs

    @classmethod
    def for_job_run_and_action(cls, job_run, action_graph):
        runs = [
            action.build_run(job_run)
            for action in action_graph.action_map.values()
        ]
        return cls(runs, action_graph)
