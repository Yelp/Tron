from unittest import mock

import pytest

from tron.config.schema import ConfigAction
from tron.config.schema import ConfigJob
from tron.core.jobgraph import AdjListEntry
from tron.core.jobgraph import JobGraph


MISSING_DEPENDENCY_ERR_MSG = '''The following actions are dependencies of other actions but missing:
Action other.job2.action3 is dependency of actions:
  - MASTER.job3.action5
Please check if you have deleted/renamed any of them or their containing jobs.'''


def _setup_job_graph_config_container():
    action1 = ConfigAction(
        name='action1',
        command='do something',
    )
    action2 = ConfigAction(
        name='action2',
        command='do something',
        requires=['action1'],
    )
    job1_config = ConfigJob(
        name='job1',
        node='default',
        schedule=mock.Mock(),
        actions={'action1': action1, 'action2': action2},
        namespace='MASTER',
    )

    action3 = ConfigAction(
        name='action3',
        command='do something',
        triggered_by=['MASTER.job1.action2.shortdate.{shortdate}'],
    )
    job2_config = ConfigJob(
        name='job1',
        node='default',
        schedule=mock.Mock(),
        actions={'action3': action3},
        namespace='other',
    )

    action4 = ConfigAction(
        name='action4',
        command='do something',
    )
    action5 = ConfigAction(
        name='action5',
        command='do something',
        requires=['action4'],
        triggered_by=['other.job2.action3.shortdate.{shortdate}'],
    )
    job3_config = ConfigJob(
        name='job1',
        node='default',
        schedule=mock.Mock(),
        actions={'action4': action4, 'action5': action5},
        namespace='MASTER',
    )
    config_container = mock.Mock()
    config_container.get_jobs.return_value = {
        'MASTER.job1': job1_config,
        'other.job2': job2_config,
        'MASTER.job3': job3_config,
    }
    return config_container


class TestJobGraph:

    def setup_method(self):
        self.job_graph = JobGraph(_setup_job_graph_config_container(), should_validate_missing_dependency=True)

    def test_job_graph_missing_dependency(self):
        missing_dependency_config_container = _setup_job_graph_config_container()
        missing_dependency_config_container.get_jobs.return_value.pop('other.job2')
        with pytest.raises(ValueError) as e:
            JobGraph(missing_dependency_config_container, should_validate_missing_dependency=True)
        assert str(e.value) == MISSING_DEPENDENCY_ERR_MSG

    def test_job_graph(self):
        assert sorted(list(self.job_graph.action_map.keys())) == [
            'MASTER.job1.action1',
            'MASTER.job1.action2',
            'MASTER.job3.action4',
            'MASTER.job3.action5',
            'other.job2.action3',
        ]
        assert self.job_graph._actions_for_job == {
            'MASTER.job1': ['MASTER.job1.action1', 'MASTER.job1.action2'],
            'other.job2': ['other.job2.action3'],
            'MASTER.job3': ['MASTER.job3.action4', 'MASTER.job3.action5'],
        }
        assert self.job_graph._adj_list == {
            'MASTER.job1.action1': [AdjListEntry('MASTER.job1.action2', False)],
            'MASTER.job1.action2': [AdjListEntry('other.job2.action3', True)],
            'other.job2.action3': [AdjListEntry('MASTER.job3.action5', True)],
            'MASTER.job3.action4': [AdjListEntry('MASTER.job3.action5', False)],
        }
        assert self.job_graph._rev_adj_list == {
            'MASTER.job1.action1': [],
            'MASTER.job1.action2': [AdjListEntry('MASTER.job1.action1', False)],
            'other.job2.action3': [AdjListEntry('MASTER.job1.action2', True)],
            'MASTER.job3.action4': [],
            'MASTER.job3.action5': [
                AdjListEntry('MASTER.job3.action4', False),
                AdjListEntry('other.job2.action3', True),
            ],
        }

    def test_get_action_graph_for_job(self):
        action_graph_1 = self.job_graph.get_action_graph_for_job('MASTER.job1')
        assert sorted((action_graph_1.action_map.keys())) == [
            'action1',
            'action2',
        ]
        assert action_graph_1.required_actions == {
            'action1': set(),
            'action2': {'action1'}
        }
        assert action_graph_1.required_triggers == {
            'other.job2.action3': {'action2'},
            'MASTER.job3.action5': {'other.job2.action3'},
        }

        action_graph_2 = self.job_graph.get_action_graph_for_job('other.job2')
        assert sorted((action_graph_2.action_map.keys())) == [
            'action3',
        ]
        assert action_graph_2.required_actions == {
            'action3': set(),
        }
        assert action_graph_2.required_triggers == {
            'action3': {'MASTER.job1.action2'},
            'MASTER.job3.action5': {'action3'},
        }

        action_graph_3 = self.job_graph.get_action_graph_for_job('MASTER.job3')
        assert sorted((action_graph_3.action_map.keys())) == [
            'action4',
            'action5',
        ]
        assert action_graph_3.required_actions == {
            'action4': set(),
            'action5': {'action4'},
        }
        assert action_graph_3.required_triggers == {
            'action5': {'other.job2.action3'},
            'other.job2.action3': {'MASTER.job1.action2'},
        }
