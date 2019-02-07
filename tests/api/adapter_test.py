from __future__ import absolute_import
from __future__ import unicode_literals

import shutil
import tempfile

import mock

from testifycompat import assert_equal
from testifycompat import run
from testifycompat import setup
from testifycompat import teardown
from testifycompat import TestCase
from tests import mocks
from tests.assertions import assert_length
from tron import node
from tron import scheduler
from tron.api import adapter
from tron.api.adapter import ActionRunAdapter
from tron.api.adapter import JobRunAdapter
from tron.api.adapter import ReprAdapter
from tron.api.adapter import RunAdapter
from tron.core import actiongraph
from tron.core import actionrun
from tron.core import job


class MockAdapter(ReprAdapter):

    field_names = ['one', 'two']
    translated_field_names = ['three', 'four']

    def get_three(self):
        return 3

    def get_four(self):
        return 4


class TestReprAdapter(TestCase):
    @setup
    def setup_adapter(self):
        self.original = mock.Mock(one=1, two=2)
        self.adapter = MockAdapter(self.original)

    def test__init__(self):
        assert_equal(self.adapter._obj, self.original)
        assert_equal(self.adapter.fields, MockAdapter.field_names)

    def test_get_translation_mapping(self):
        expected = {
            'three': self.adapter.get_three,
            'four': self.adapter.get_four,
        }
        assert_equal(self.adapter.translators, expected)

    def test_get_repr(self):
        expected = dict(one=1, two=2, three=3, four=4)
        assert_equal(self.adapter.get_repr(), expected)


class SampleClassStub(object):
    def __init__(self):
        self.true_flag = True
        self.false_flag = False

    @adapter.toggle_flag('true_flag')
    def expects_true(self):
        return "This is true"

    @adapter.toggle_flag('false_flag')
    def expects_false(self):
        return "This is false"


class TestToggleFlag(TestCase):
    @setup
    def setup_stub(self):
        self.stub = SampleClassStub()

    def test_toggle_flag_true(self):
        assert_equal(self.stub.expects_true(), "This is true")

    def test_toggle_flag_false(self):
        assert not self.stub.expects_false()


class TestRunAdapter(TestCase):
    @setup
    def setup_adapter(self):
        self.original = mock.Mock()
        self.adapter = RunAdapter(self.original)

    def test_get_state(self):
        assert_equal(self.adapter.get_state(), self.original.state)

    @mock.patch('tron.api.adapter.NodeAdapter', autospec=True)
    def test_get_node(self, mock_node_adapter):
        assert_equal(
            self.adapter.get_node(),
            mock_node_adapter.return_value.get_repr.return_value,
        )
        mock_node_adapter.assert_called_with(self.original.node)

    def test_get_duration(self):
        self.original.start_time = None
        assert_equal(self.adapter.get_duration(), '')


class TestActionRunAdapter(TestCase):
    @setup
    def setup_adapter(self):
        self.temp_dir = tempfile.mkdtemp()
        self.action_run = mock.MagicMock()
        self.job_run = mock.MagicMock()
        self.adapter = ActionRunAdapter(self.action_run, self.job_run, 4)

    @teardown
    def teardown_adapter(self):
        shutil.rmtree(self.temp_dir)

    def test__init__(self):
        assert_equal(self.adapter.max_lines, 4)
        assert_equal(self.adapter.job_run, self.job_run)
        assert_equal(self.adapter._obj, self.action_run)

    def test_get_repr(self):
        result = self.adapter.get_repr()
        assert_equal(result['command'], self.action_run.rendered_command)


class TestActionRunGraphAdapter(TestCase):
    @setup
    def setup_adapter(self):
        self.ar1 = mock.MagicMock(action_name="a1")
        self.ar2 = mock.MagicMock(action_name="a2")
        self.a1 = mock.MagicMock()
        self.a2 = mock.MagicMock()
        self.a1.name = 'a1'
        self.a2.name = 'a2'
        self.action_runs = mock.create_autospec(
            actionrun.ActionRunCollection,
            action_graph=actiongraph.ActionGraph(
                {
                    'a1': self.a1,
                    'a2': self.a2
                },
                {'a1': set(), 'a2': {'a1'}},
                {'a1': set(), 'a2': set()},
            ),
        )
        self.adapter = adapter.ActionRunGraphAdapter(self.action_runs)
        self.action_runs.__iter__.return_value = [self.ar1, self.ar2]

    def test_get_repr(self):
        result = self.adapter.get_repr()
        assert len(result) == 2
        assert self.ar1.id == result[0]['id']
        assert ['a1'] == result[1]['dependencies']


class TestJobRunAdapter(TestCase):
    @setup
    def setup_adapter(self):
        action_runs = mock.MagicMock()
        action_runs.__iter__.return_value = iter([mock.Mock(), mock.Mock()])
        self.job_run = mock.Mock(
            action_runs=action_runs,
            action_graph=mocks.MockActionGraph(),
        )
        self.adapter = JobRunAdapter(self.job_run, include_action_runs=True)

    def test__init__(self):
        assert self.adapter.include_action_runs

    def test_get_runs(self):
        with mock.patch('tron.api.adapter.ActionRunAdapter', autospec=True):
            assert_length(self.adapter.get_runs(), 2)

    def test_get_runs_without_action_runs(self):
        self.adapter.include_action_runs = False
        assert_equal(self.adapter.get_runs(), None)


class TestNodeAdapter(TestCase):
    @setup
    def setup_adapter(self):
        self.node = mock.create_autospec(node.Node)
        self.adapter = adapter.NodeAdapter(self.node)

    def test_repr(self):
        result = self.adapter.get_repr()
        assert_equal(result['hostname'], self.node.hostname)
        assert_equal(result['username'], self.node.username)


class TestNodePoolAdapter(TestCase):
    @setup
    def setup_adapter(self):
        self.pool = mock.create_autospec(node.NodePool)
        self.adapter = adapter.NodePoolAdapter(self.pool)

    @mock.patch('tron.api.adapter.adapt_many', autospec=True)
    def test_repr(self, mock_many):
        result = self.adapter.get_repr()
        assert_equal(result['name'], self.pool.get_name.return_value)
        mock_many.assert_called_with(
            adapter.NodeAdapter,
            self.pool.get_nodes.return_value,
        )


class TestJobIndexAdapter(TestCase):
    @setup
    def setup_adapter(self):
        self.job = mock.create_autospec(job.Job)
        self.adapter = adapter.JobIndexAdapter(self.job)

    def test_repr(self):
        result = self.adapter.get_repr()
        self.job.get_runs.assert_called_with()
        runs = self.job.get_runs.return_value
        runs.get_newest.assert_called_with()
        expected = {
            'name': self.job.get_name.return_value,
            'actions': [],
        }
        assert_equal(result, expected)

    def test_get_actions(self):
        action_run = mock.Mock()
        job_run = self.job.get_runs.return_value.get_newest.return_value
        job_run.action_runs.__iter__.return_value = [action_run]
        result = self.adapter.get_actions()
        expected = {
            'name': action_run.action_name,
            'command': action_run.bare_command,
        }
        assert_equal(result, [expected])

    def test_get_actions_no_runs(self):
        self.job.get_runs.return_value.get_newest.return_value = None
        result = self.adapter.get_actions()
        assert_equal(result, [])


class TestSchedulerAdapter(TestCase):
    @setup
    def setup_adapter(self):
        self.scheduler = mock.create_autospec(scheduler.GeneralScheduler)
        self.adapter = adapter.SchedulerAdapter(self.scheduler)

    @mock.patch('tron.api.adapter.scheduler.get_jitter_str', autospec=True)
    def test_repr(self, mock_get_jitter):
        result = self.adapter.get_repr()
        expected = {
            'type': self.scheduler.get_name.return_value,
            'value': self.scheduler.get_value.return_value,
            'jitter': mock_get_jitter.return_value,
        }
        assert_equal(result, expected)
        mock_get_jitter.assert_called_with(self.scheduler.get_jitter())


if __name__ == "__main__":
    run()
