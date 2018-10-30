"""
Test cases for the web services interface to tron
"""
from unittest.mock import MagicMock

import mock
import pytest
import six
import twisted.web.http
import twisted.web.resource
import twisted.web.server
from twisted.web import http

from tests.assertions import assert_call
from tests.testingutils import autospec_method
from tron import mcp
from tron import node
from tron.api import controller
from tron.core import job
from tron.core import jobrun
from tron.core.job_collection import JobCollection
from tron.core.job_scheduler import JobScheduler

with mock.patch(
    'tron.api.async_resource.AsyncResource.bounded',
    lambda fn: fn,
    autospec=None,
):
    with mock.patch(
        'tron.api.async_resource.AsyncResource.exclusive',
        lambda fn: fn,
        autospec=None,
    ):
        from tron.api import resource as www

REQUEST = twisted.web.server.Request(mock.Mock(), None)
REQUEST.childLink = lambda val: "/jobs/%s" % val


def build_request(**kwargs):
    args = {k.encode(): [v.encode()] for k, v in six.iteritems(kwargs)}
    return mock.create_autospec(twisted.web.server.Request, args=args)


@pytest.fixture
def request():
    return build_request()


@pytest.fixture
def mock_respond():
    with mock.patch(
        'tron.api.resource.respond',
        autospec=True,
    ) as mock_respond:
        mock_respond.side_effect = lambda _req, output, code=None: output
        yield mock_respond


@pytest.mark.usefixtures("mock_respond")
class WWWTestCase:
    """Patch www.response to not json encode."""
    pass


class TestHandleCommand:
    @pytest.fixture
    def mock_respond(self, mock_respond):
        # in this test case, we don't want a side effect
        mock_respond.side_effect = None
        return mock_respond

    def test_handle_command_unknown(self, mock_respond):
        command = 'the command'
        request = build_request(command=command)
        mock_controller, obj = mock.Mock(), mock.Mock()
        error = controller.UnknownCommandError()
        mock_controller.handle_command.side_effect = error
        www.handle_command(request, mock_controller, obj)
        mock_controller.handle_command.assert_called_with(command)
        mock_respond.assert_called_with(
            request,
            {'error': f"Unknown command '{command}' for '{obj}'"},
            code=http.NOT_IMPLEMENTED
        )

    def test_handle_command(self, mock_respond):
        command = 'the command'
        request = build_request(command=command)
        mock_controller, obj = mock.Mock(), mock.Mock()
        www.handle_command(request, mock_controller, obj)
        mock_controller.handle_command.assert_called_with(command)
        mock_respond.assert_called_with(
            request,
            {'result': mock_controller.handle_command.return_value},
        )

    def test_handle_command_error(self, mock_respond):
        command = 'the command'
        request = build_request(command=command)
        mock_controller, obj = mock.Mock(), mock.Mock()
        error = Exception("uncaught exception")
        mock_controller.handle_command.side_effect = error
        www.handle_command(request, mock_controller, obj)
        mock_controller.handle_command.assert_called_with(command)
        mock_respond.assert_called_with(request, {'error': mock.ANY})


class TestActionRunResource(WWWTestCase):
    @pytest.fixture(autouse=True)
    def setup_resource(self):
        self.job_run = mock.MagicMock()
        self.action_run = mock.MagicMock(output_path=['one'])
        self.resource = www.ActionRunResource(self.action_run, self.job_run)

    def test_render_GET(self, mock_respond):
        request = build_request(num_lines="12")
        response = self.resource.render_GET(request)
        assert response['id'] == self.resource.action_run.id


class TestJobrunResource(WWWTestCase):
    @pytest.fixture(autouse=True)
    def setup_resource(self):
        self.job_run = mock.MagicMock()
        self.job_scheduler = mock.Mock()
        self.resource = www.JobRunResource(self.job_run, self.job_scheduler)

    def test_render_GET(self, request):
        response = self.resource.render_GET(request)
        assert response['id'] == self.job_run.id


class TestApiRootResource(WWWTestCase):
    @pytest.fixture(autouse=True)
    def setup_resource(self):
        self.mcp = mock.create_autospec(mcp.MasterControlProgram)
        self.resource = www.ApiRootResource(self.mcp)

    def test__init__(self):
        expected_children = [
            b'jobs',
            b'config',
            b'metrics',
            b'status',
            b'events',
            b'',
        ]
        assert set(expected_children) == set(self.resource.children)

    def test_render_GET(self):
        expected_keys = [
            'jobs',
            'namespaces',
        ]
        response = self.resource.render_GET(build_request())
        assert set(response.keys()) == set(expected_keys)
        self.mcp.get_job_collection().get_jobs.assert_called_with()


class TestRootResource(WWWTestCase):
    @pytest.fixture(autouse=True)
    def setup_resource(self):
        self.web_path = '/bogus/path'
        self.mcp = mock.create_autospec(mcp.MasterControlProgram)
        self.resource = www.RootResource(self.mcp, self.web_path)

    def test_render_GET(self):
        request = build_request()
        response = self.resource.render_GET(request)
        assert response == 1
        assert request.redirect.call_count == 1
        request.finish.assert_called_with()

    def test_get_children(self):
        assert set(self.resource.children) == {b'api', b'web', b''}


class TestActionRunHistoryResource(WWWTestCase):
    @pytest.fixture(autouse=True)
    def setup_resource(self):
        self.action_runs = [mock.MagicMock(), mock.MagicMock()]
        self.resource = www.ActionRunHistoryResource(self.action_runs)

    def test_render_GET(self, request):
        response = self.resource.render_GET(request)
        assert len(response) == len(self.action_runs)


class TestJobCollectionResource(WWWTestCase):
    @pytest.fixture(autouse=True)
    def setup_resource(self):
        job_collection = mock.create_autospec(JobCollection)
        job_collection.get_by_name = lambda name: name if name == 'testname' else None
        self.resource = www.JobCollectionResource(job_collection)

    def test_render_GET(self):
        self.resource.get_data = MagicMock()
        result = self.resource.render_GET(REQUEST)
        assert_call(self.resource.get_data, 0, False, False, True, True)
        assert 'jobs' in result

    def test_getChild(self):
        child = self.resource.getChild(b"testname", mock.Mock())
        assert isinstance(child, www.JobResource)

    def test_getChild_missing_job(self):
        child = self.resource.getChild(b"bar", mock.Mock())
        assert isinstance(child, www.ErrorResource)


class TestJobResource(WWWTestCase):
    @pytest.fixture(autouse=True)
    def setup_resource(self):
        self.job_scheduler = mock.create_autospec(JobScheduler)
        self.job_runs = mock.create_autospec(jobrun.JobRunCollection)
        self.job = mock.create_autospec(
            job.Job,
            runs=self.job_runs,
            all_nodes=False,
            allow_overlap=True,
            queueing=True,
            action_graph=mock.MagicMock(),
            scheduler=mock.Mock(),
            node_pool=mock.create_autospec(node.NodePool, ),
            max_runtime=mock.Mock(),
            expected_runtime=mock.MagicMock(),
        )
        self.job.get_name.return_value = 'foo'
        self.job_scheduler.get_job.return_value = self.job
        self.job_scheduler.get_job_runs.return_value = self.job_runs
        self.resource = www.JobResource(self.job_scheduler)

    def test_render_GET(self, request):
        result = self.resource.render_GET(request)
        assert result['name'] == self.job_scheduler.get_job().get_name()

    def test_get_run_from_identifier_HEAD(self):
        job_run = self.resource.get_run_from_identifier('HEAD')
        self.job_scheduler.get_job_runs.assert_called_with()
        assert job_run == self.job_runs.get_newest.return_value

    def test_get_run_from_identifier_number(self):
        job_run = self.resource.get_run_from_identifier('3')
        self.job_scheduler.get_job_runs.assert_called_with()
        assert job_run == self.job_runs.get_run_by_num.return_value
        self.job_runs.get_run_by_num.assert_called_with(3)

    def test_get_run_from_identifier_negative_index(self):
        job_run = self.resource.get_run_from_identifier('-2')
        assert job_run == self.job_runs.get_run_by_index.return_value
        self.job_runs.get_run_by_index.assert_called_with(-2)

    def test_getChild(self):
        autospec_method(self.resource.get_run_from_identifier)
        identifier = b'identifier'
        resource = self.resource.getChild(identifier, None)
        assert resource.job_run == self.resource.get_run_from_identifier.return_value

    def test_getChild_action_run_history(self):
        autospec_method(
            self.resource.get_run_from_identifier,
            return_value=None,
        )
        action_name = 'action_name'
        action_runs = [mock.Mock(), mock.Mock()]
        self.job.action_graph.names = [action_name]
        self.job.runs.get_action_runs.return_value = action_runs
        resource = self.resource.getChild(action_name, None)
        assert resource.__class__ == www.ActionRunHistoryResource
        assert resource.action_runs == action_runs


class TestConfigResource:
    @pytest.fixture(autouse=True)
    def setup_resource(self):
        self.mcp = mock.create_autospec(mcp.MasterControlProgram)
        self.resource = www.ConfigResource(self.mcp)
        self.controller = self.resource.controller = mock.create_autospec(
            controller.ConfigController,
        )

    def test_render_GET(self, mock_respond):
        name = 'the_name'
        request = build_request(name=name)
        self.resource.render_GET(request)
        self.controller.read_config.assert_called_with(name)
        mock_respond.assert_called_with(
            request,
            self.resource.controller.read_config.return_value,
        )

    def test_render_POST_update(self, mock_respond):
        name, config, hash = 'the_name', 'config', 'hash'
        request = build_request(name=name, config=config, hash=hash)
        self.resource.render_POST(request)
        self.resource.controller.update_config.assert_called_with(name, config, hash)
        expected_response = {
            'status': 'Active',
            'error': self.resource.controller.update_config.return_value,
        }
        mock_respond.assert_called_with(request, expected_response)

    def test_render_POST_delete(self, mock_respond):
        name, config, hash = 'the_name', '', ''
        request = build_request(name=name, config=config, hash=hash)
        self.resource.render_POST(request)
        self.resource.controller.delete_config.assert_called_with(name, config, hash)
        expected_response = {
            'status': 'Active',
            'error': self.resource.controller.delete_config.return_value,
        }
        mock_respond.assert_called_with(request, expected_response)


class TestMetricsResource:
    @mock.patch('tron.api.resource.view_all_metrics', autospec=True)
    def test_render_GET(self, mock_view_metrics, request, mock_respond):
        resource = www.MetricsResource()
        resource.render_GET(request)
        mock_respond.assert_called_with(
            request, mock_view_metrics.return_value
        )


class TestTronSite:
    @mock.patch('tron.api.resource.meter', autospec=True)
    def test_log_request(self, mock_meter):
        site = www.TronSite.create(
            mock.create_autospec(mcp.MasterControlProgram),
            'webpath',
        )
        request = mock.Mock(code=500)
        site.log(request)
        assert mock_meter.call_count == 1
