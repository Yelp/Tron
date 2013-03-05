import shutil
import tempfile
import mock
from testify import TestCase, assert_equal, run, setup, teardown
from tests import mocks
from tests.assertions import assert_length
from tests.testingutils import Turtle
from tron.api.adapter import ReprAdapter, RunAdapter, ActionRunAdapter
from tron.api.adapter import JobRunAdapter, ServiceAdapter


class MockAdapter(ReprAdapter):

    field_names = ['one', 'two']
    translated_field_names = ['three', 'four']

    def get_three(self):
        return 3

    def get_four(self):
        return 4


class ReprAdapterTestCase(TestCase):

    @setup
    def setup_adapter(self):
        self.original           = Turtle(one=1, two=2)
        self.adapter            = MockAdapter(self.original)

    def test__init__(self):
        assert_equal(self.adapter._obj, self.original)
        assert_equal(self.adapter.fields, MockAdapter.field_names)

    def test_get_translation_mapping(self):
        expected = {
            'three': self.adapter.get_three,
            'four':  self.adapter.get_four
        }
        assert_equal(self.adapter.translators, expected)

    def test_get_repr(self):
        expected = dict(one=1, two=2, three=3, four=4)
        assert_equal(self.adapter.get_repr(), expected)


class RunAdapterTestCase(TestCase):

    @setup
    def setup_adapter(self):
        self.original           = mock.Mock()
        self.adapter            = RunAdapter(self.original)

    def test_get_state(self):
        assert_equal(self.adapter.get_state(), self.original.state.short_name)

    def test_get_node(self):
        assert_equal(self.adapter.get_node(), str(self.original.node))

    def test_get_duration(self):
        self.original.start_time = None
        assert_equal(self.adapter.get_duration(), '')


class ActionRunAdapterTestCase(TestCase):

    @setup
    def setup_adapter(self):
        self.temp_dir = tempfile.mkdtemp()
        self.action_run = Turtle(output_path=[self.temp_dir])
        self.job_run = Turtle(action_runs={'action_name': self.action_run})
        self.adapter = ActionRunAdapter(self.action_run, self.job_run, 4)

    @teardown
    def teardown_adapter(self):
        shutil.rmtree(self.temp_dir)

    def test__init__(self):
        assert_equal(self.adapter.max_lines, 4)
        assert_equal(self.adapter.job_run, self.job_run)
        assert_equal(self.adapter._obj, self.action_run)


class JobRunAdapterTestCase(TestCase):

    @setup
    def setup_adapter(self):
        action_runs = mock.MagicMock()
        action_runs.__iter__.return_value = iter([mock.Mock(), mock.Mock()])
        self.job_run = mock.Mock(
                action_runs=action_runs, action_graph=mocks.MockActionGraph())
        self.adapter = JobRunAdapter(self.job_run, include_action_runs=True)

    def test__init__(self):
        assert self.adapter.include_action_runs

    def test_get_runs(self):
        with mock.patch('tron.api.adapter.ActionRunAdapter'):
            assert_length(self.adapter.get_runs(), 2)

    def test_get_runs_without_action_runs(self):
        self.adapter.include_action_runs = False
        assert_equal(self.adapter.get_runs(), None)


class ServiceAdapterTestCase(TestCase):

    @setup
    def setup_adapter(self):
        self.service = mock.MagicMock()
        self.adapter = ServiceAdapter(self.service)

    def test_repr(self):
        result = self.adapter.get_repr()
        assert_equal(result['name'], self.service.name)
        assert_equal(result['node_pool'], self.service.config.node)


if __name__ == "__main__":
    run()