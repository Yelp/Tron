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
def respond():
    with mock.patch('tron.api.resource.respond', autospec=True) as resp:
        resp.side_effect = lambda _req, output, code=None: output
        yield resp


class TestHandleCommand:
    def test_handle_command_unknown(self, respond):
        command = 'the command'
        request = build_request(command=command)
        mock_controller, obj = mock.Mock(), mock.Mock()
        error = controller.UnknownCommandError()
        mock_controller.handle_command.side_effect = error
        response = www.handle_command(request, mock_controller, obj)
        mock_controller.handle_command.assert_called_with(command)
        assert "Unknown command" in response['error']
        respond.assert_called_with(
            request,
            {'error': f"Unknown command '{command}' for '{obj}'"},
            code=http.NOT_IMPLEMENTED
        )

    def test_handle_command(self, respond):
        command = 'the command'
        request = build_request(command=command)
        mock_controller, obj = mock.Mock(), mock.Mock()
        response = www.handle_command(request, mock_controller, obj)
        mock_controller.handle_command.assert_called_with(command)
        assert response['result'] == mock_controller.handle_command.return_value
        respond.assert_called_with(
            request,
            {'result': mock_controller.handle_command.return_value},
        )

    def test_handle_command_error(self, respond):
        command = 'the command'
        request = build_request(command=command)
        mock_controller, obj = mock.Mock(), mock.Mock()
        error = Exception("uncaught exception")
        mock_controller.handle_command.side_effect = error
        response = www.handle_command(request, mock_controller, obj)
        mock_controller.handle_command.assert_called_with(command)
        assert "uncaught exception" in response['error']
        respond.assert_called_with(
            request,
            {'error': mock.ANY},
            code=http.INTERNAL_SERVER_ERROR,
        )


@pytest.fixture
def action_run_resource():
    job_run = mock.MagicMock()
    action_run = mock.MagicMock(output_path=['one'])
    return www.ActionRunResource(action_run, job_run)


class TestActionRunResource:
    def test_render_GET(self, action_run_resource, respond):
        request = build_request(num_lines="12")
        response = action_run_resource.render_GET(request)
        assert response['id'] == action_run_resource.action_run.id


@pytest.fixture
def job_run_resource():
    job_run = mock.MagicMock()
    job_scheduler = mock.Mock()
    return www.JobRunResource(job_run, job_scheduler)


class TestJobrunResource:
    def test_render_GET(self, request, job_run_resource, respond):
        response = job_run_resource.render_GET(request)
        assert response['id'] == job_run_resource.job_run.id


@pytest.fixture
def api_root_resource():
    return www.ApiRootResource(mock.create_autospec(mcp.MasterControlProgram))


class TestApiRootResource:
    def test__init__(self, api_root_resource):
        expected_children = [
            b'jobs',
            b'config',
            b'status',
            b'events',
            b'',
        ]
        assert set(expected_children) == set(api_root_resource.children)

    def test_render_GET(self, request, api_root_resource, respond):
        expected_keys = {'jobs', 'namespaces'}
        response = api_root_resource.render_GET(request)
        assert set(response.keys()) == expected_keys
        api_root_resource._master_control.get_job_collection().get_jobs.assert_called_with()


@pytest.fixture
def root_resource():
    return www.RootResource(
        mock.create_autospec(mcp.MasterControlProgram), '/bogus/path')


class TestRootResource:
    def test_render_GET(self, request, root_resource, respond):
        response = root_resource.render_GET(request)
        assert response == 1
        assert request.redirect.call_count == 1
        request.finish.assert_called_with()

    def test_get_children(self, root_resource):
        assert set(root_resource.children) == {b'api', b'web', b''}


@pytest.fixture
def action_run_history_resource():
    action_runs = [mock.MagicMock(), mock.MagicMock()]
    return www.ActionRunHistoryResource(action_runs)


class TestActionRunHistoryResource:
    def test_render_GET(self, action_run_history_resource, request, respond):
        response = action_run_history_resource.render_GET(request)
        assert len(response) == len(action_run_history_resource.action_runs)


@pytest.fixture
def job_collection_resource():
    job_collection = mock.create_autospec(JobCollection)
    job_collection.get_by_name.return_value = None
    return www.JobCollectionResource(job_collection)


class TestJobCollectionResource:
    def test_render_GET(self, job_collection_resource, respond):
        job_collection_resource.get_data = MagicMock()
        result = job_collection_resource.render_GET(REQUEST)
        assert_call(job_collection_resource.get_data, 0, False, False, True, True)
        assert 'jobs' in result

    def test_getChild(self, job_collection_resource):
        job_collection_resource.job_collection.get_by_name.return_value = "testname"
        child = job_collection_resource.getChild(b"testname", mock.Mock())
        assert isinstance(child, www.JobResource)

    def test_getChild_missing_job(self, job_collection_resource):
        child = job_collection_resource.getChild(b"bar", mock.Mock())
        assert isinstance(child, www.ErrorResource)


@pytest.fixture
def job_scheduler():
    return mock.create_autospec(JobScheduler)


@pytest.fixture
def job_runs():
    return mock.create_autospec(jobrun.JobRunCollection)


@pytest.fixture
def job_fix(job_runs):
    return mock.create_autospec(
        job.Job,
        runs=job_runs,
        all_nodes=False,
        allow_overlap=True,
        queueing=True,
        action_graph=mock.MagicMock(),
        scheduler=mock.Mock(),
        node_pool=mock.create_autospec(node.NodePool, ),
        max_runtime=mock.Mock(),
        expected_runtime=mock.MagicMock(),
    )


@pytest.fixture
def job_resource(job_scheduler, job_runs, job_fix):
    job_fix.get_name.return_value = 'foo'
    job_scheduler.get_job.return_value = job_fix
    job_scheduler.get_job_runs.return_value = job_runs
    return www.JobResource(job_scheduler)


class TestJobResource:
    def test_render_GET(self, job_resource, request, respond):
        result = job_resource.render_GET(request)
        assert result['name'] == job_resource.job_scheduler.get_job().get_name()

    def test_get_run_from_identifier_HEAD(self, job_resource, job_runs):
        job_run = job_resource.get_run_from_identifier('HEAD')
        job_resource.job_scheduler.get_job_runs.assert_called_with()
        assert job_run == job_runs.get_newest.return_value

    def test_get_run_from_identifier_number(self, job_resource, job_scheduler, job_runs):
        job_run = job_resource.get_run_from_identifier('3')
        job_scheduler.get_job_runs.assert_called_with()
        assert job_run == job_runs.get_run_by_num.return_value
        job_runs.get_run_by_num.assert_called_with(3)

    def test_get_run_from_identifier_negative_index(self, job_resource, job_runs):
        job_run = job_resource.get_run_from_identifier('-2')
        assert job_run == job_runs.get_run_by_index.return_value
        job_runs.get_run_by_index.assert_called_with(-2)

    def test_getChild(self, job_resource):
        autospec_method(job_resource.get_run_from_identifier)
        identifier = b'identifier'
        resource = job_resource.getChild(identifier, None)
        assert resource.job_run == job_resource.get_run_from_identifier.return_value

    def test_getChild_action_run_history(self, job_resource, job_fix):
        autospec_method(
            job_resource.get_run_from_identifier,
            return_value=None,
        )
        action_name = 'action_name'
        action_runs = [mock.Mock(), mock.Mock()]
        job_fix.action_graph.names = [action_name]
        job_fix.runs.get_action_runs.return_value = action_runs
        resource = job_resource.getChild(action_name, None)
        assert resource.__class__ == www.ActionRunHistoryResource
        assert resource.action_runs == action_runs


@pytest.fixture
def config_resource():
    mcp_mock = mock.create_autospec(mcp.MasterControlProgram)
    resource = www.ConfigResource(mcp_mock)
    resource.controller = mock.create_autospec(controller.ConfigController)
    return resource


class TestConfigResource:
    def test_render_GET(self, config_resource, respond):
        name = 'the_name'
        request = build_request(name=name)
        actual_response = config_resource.render_GET(request)
        config_resource.controller.read_config.assert_called_with(name)
        assert actual_response == config_resource.controller.read_config.return_value

    def test_render_POST_update(self, config_resource, respond):
        name, config, hash = 'the_name', 'config', 'hash'
        request = build_request(name=name, config=config, hash=hash)
        actual_response = config_resource.render_POST(request)
        config_resource.controller.update_config.assert_called_with(name, config, hash)
        expected_response = {
            'status': 'Active',
            'error': config_resource.controller.update_config.return_value,
        }
        assert actual_response == expected_response

    def test_render_POST_delete(self, config_resource, respond):
        name, config, hash = 'the_name', '', ''
        request = build_request(name=name, config=config, hash=hash)
        actual_response = config_resource.render_POST(request)
        config_resource.controller.delete_config.assert_called_with(name, config, hash)
        expected_response = {
            'status': 'Active',
            'error': config_resource.controller.delete_config.return_value,
        }
        assert actual_response == expected_response
