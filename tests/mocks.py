from __future__ import absolute_import
from __future__ import unicode_literals

import atexit
import datetime
import itertools
import shutil
import tempfile
from unittest.mock import MagicMock


class MockAction(MagicMock):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('name', 'action_name')
        kwargs.setdefault('required_actions', [])
        kwargs.setdefault('dependent_actions', [])
        super(MockAction, self).__init__(*args, **kwargs)


class MockActionGraph(MagicMock):
    def __init__(self, *args, **kwargs):
        action = MockAction()
        kwargs.setdefault('graph', [action])
        kwargs.setdefault('action_map', {action.name: action})
        super(MockActionGraph, self).__init__(*args, **kwargs)

    def __getitem__(self, item):
        action = MockAction(name=item)
        self.action_map.setdefault(item, action)
        return self.action_map[item]

    def get_required_actions(self, name):
        return []


class MockActionRun(MagicMock):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('output_path', [tempfile.mkdtemp()])
        kwargs.setdefault('start_time', datetime.datetime.now())
        kwargs.setdefault('end_time', datetime.datetime.now())
        atexit.register(lambda: shutil.rmtree(kwargs['output_path'][0]))
        super(MockActionRun, self).__init__(*args, **kwargs)


class MockActionRunCollection(MagicMock):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('action_graph', MockActionGraph())
        kwargs.setdefault('run_map', {})
        super(MockActionRunCollection, self).__init__(*args, **kwargs)

    def __getitem__(self, item):
        action_run = MockActionRun(name=item)
        self.run_map.setdefault(item, action_run)
        return self.run_map[item]


class MockJobRun(MagicMock):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('output_path', [tempfile.mkdtemp()])
        kwargs.setdefault('action_graph', MockActionGraph())
        action_runs = MockActionRunCollection(
            action_graph=kwargs['action_graph'],
        )
        kwargs.setdefault('action_runs', action_runs)
        atexit.register(lambda: shutil.rmtree(kwargs['output_path'][0]))
        super(MockJobRun, self).__init__(*args, **kwargs)


class MockNode(MagicMock):
    def __init__(self, hostname=None):
        super(MockNode, self).__init__()
        self.name = self.hostname = hostname

    def run(self, runnable):
        runnable.started()
        return type(self)()


class MockNodePool(object):
    _node = None

    def __init__(self, *node_names):
        self.nodes = []
        self._ndx_cycle = None
        for hostname in node_names:
            self.nodes.append(MockNode(hostname=hostname))

        if self.nodes:
            self._ndx_cycle = itertools.cycle(range(0, len(self.nodes)))

    def __getitem__(self, value):
        for node in self.nodes:
            if node.hostname == value:
                return node
        else:
            raise KeyError

    def next(self):
        if not self.nodes:
            self.nodes.append(MockNode())

        if self._ndx_cycle:
            return self.nodes[next(self._ndx_cycle)]
        else:
            return self.nodes[0]

    next_round_robin = next


class MockJobRunCollection(MagicMock):
    def __iter__(self):
        return iter(self.runs)
