"""
Test cases for the web services interface to tron
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import six
import twisted.web.http
import twisted.web.resource
import twisted.web.server
from testify import assert_equal
from testify import class_setup
from testify import run
from testify import setup
from testify import setup_teardown
from testify import teardown
from testify import TestCase
from twisted.web import http

from tests import mocks
from tests.assertions import assert_call
from tests.testingutils import autospec_method
from tests.testingutils import Turtle
from tron import event
from tron import mcp
from tron import node
from tron.api import controller
from tron.core import job
from tron.core import jobrun

with mock.patch(
    'tron.api.async_resource.AsyncResource.bounded', lambda fn: fn
):
    with mock.patch(
        'tron.api.async_resource.AsyncResource.exclusive', lambda fn: fn
    ):
        from tron.api import resource as www

REQUEST = twisted.web.server.Request(mock.Mock(), None)
REQUEST.childLink = lambda val: "/jobs/%s" % val


def build_request(**kwargs):
    args = {k.encode(): [v.encode()] for k, v in six.iteritems(kwargs)}
    return mock.create_autospec(twisted.web.server.Request, args=args)


class WWWTestCase(TestCase):
    """Patch www.response to not json encode."""

    @setup_teardown
    def mock_respond(self):
        with mock.patch(
            'tron.api.resource.respond',
            autospec=True,
        ) as self.respond:
            self.respond.side_effect = lambda _req, output, code=None: output
            yield

    @setup
    def setup_request(self):
        self.request = build_request()


class HandleCommandTestCase(TestCase):
    @setup_teardown
    def mock_respond(self):
        with mock.patch(
            'tron.api.resource.respond',
            autospec=True,
        ) as self.respond:
            yield

    def test_handle_command_unknown(self):
        command = 'the command'
        request = build_request(command=command)
        mock_controller, obj = mock.Mock(), mock.Mock()
        error = controller.UnknownCommandError("No")
        mock_controller.handle_command.side_effect = error
        response = www.handle_command(request, mock_controller, obj)
        mock_controller.handle_command.assert_called_with(command)
        assert_equal(response, self.respond.return_value)
        self.respond.assert_called_with(
            request,
            {'error': str(error)},
            code=http.NOT_IMPLEMENTED,
        )

    def test_handle_command(self):
        command = 'the command'
        request = build_request(command=command)
        mock_controller, obj = mock.Mock(), mock.Mock()
        response = www.handle_command(request, mock_controller, obj)
        mock_controller.handle_command.assert_called_with(command)
        assert_equal(response, self.respond.return_value)
        self.respond.assert_called_with(
            request,
            {'result': mock_controller.handle_command.return_value},
        )


class ActionRunResourceTestCase(WWWTestCase):
    @setup
    def setup_resource(self):
        self.job_run = mock.MagicMock()
        self.action_run = mock.MagicMock(output_path=['one'])
        self.resource = www.ActionRunResource(self.action_run, self.job_run)

    def test_render_GET(self):
        request = build_request(num_lines="12")
        response = self.resource.render_GET(request)
        assert_equal(response['id'], self.action_run.id)


class JobrunResourceTestCase(WWWTestCase):
    @setup
    def setup_resource(self):
        self.job_run = mock.MagicMock()
        self.job_scheduler = mock.Mock()
        self.resource = www.JobRunResource(self.job_run, self.job_scheduler)

    def test_render_GET(self):
        response = self.resource.render_GET(self.request)
        assert_equal(response['id'], self.job_run.id)


class ApiRootResourceTestCase(WWWTestCase):
    @setup
    def build_resource(self):
        self.mcp = mock.create_autospec(mcp.MasterControlProgram)
        self.resource = www.ApiRootResource(self.mcp)

    def test__init__(self):
        expected_children = [
            b'jobs',
            b'config',
            b'status',
            b'events',
            b'',
        ]
        assert_equal(set(expected_children), set(self.resource.children))

    def test_render_GET(self):
        expected_keys = [
            'jobs',
            'namespaces',
        ]
        response = self.resource.render_GET(build_request())
        assert_equal(set(response.keys()), set(expected_keys))
        self.mcp.get_job_collection().get_jobs.assert_called_with()


class RootResourceTestCase(WWWTestCase):
    @setup
    def build_resource(self):
        self.web_path = '/bogus/path'
        self.mcp = mock.create_autospec(mcp.MasterControlProgram)
        self.resource = www.RootResource(self.mcp, self.web_path)

    def test_render_GET(self):
        request = build_request()
        response = self.resource.render_GET(request)
        assert_equal(response, 1)
        assert_equal(request.redirect.call_count, 1)
        request.finish.assert_called_with()

    def test_get_children(self):
        assert_equal(set(self.resource.children), {b'api', b'web', b''})


class ActionRunHistoryResourceTestCase(WWWTestCase):
    @setup
    def setup_resource(self):
        self.action_runs = [mock.MagicMock(), mock.MagicMock()]
        self.resource = www.ActionRunHistoryResource(self.action_runs)

    def test_render_GET(self):
        response = self.resource.render_GET(self.request)
        assert_equal(len(response), len(self.action_runs))


class JobCollectionResourceTestCase(WWWTestCase):
    @class_setup
    def build_resource(self):
        self.job = mock.Mock(
            repr_data=lambda: {'name': 'testname'},
            name="testname",
            last_success=None,
            runs=mock.Mock(),
            scheduler_str="testsched",
            node_pool=mocks.MockNodePool(),
        )
        self.job_collection = mock.create_autospec(job.JobCollection)
        self.resource = www.JobCollectionResource(self.job_collection)

    def test_render_GET(self):
        self.resource.get_data = Turtle()
        result = self.resource.render_GET(REQUEST)
        assert_call(self.resource.get_data, 0, False, False, True, True)
        assert 'jobs' in result

    def test_getChild(self):
        child = self.resource.getChild(b"testname", mock.Mock())
        assert isinstance(child, www.JobResource)
        self.job_collection.get_by_name.assert_called_with("testname")

    def test_getChild_missing_job(self):
        self.job_collection.get_by_name.return_value = None
        child = self.resource.getChild(b"bar", mock.Mock())
        assert isinstance(child, twisted.web.resource.NoResource)


class JobResourceTestCase(WWWTestCase):
    @setup
    def setup_resource(self):
        self.job_scheduler = mock.create_autospec(job.JobScheduler)
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

    def test_render_GET(self):
        result = self.resource.render_GET(self.request)
        assert_equal(result['name'], self.job_scheduler.get_job().get_name())

    def test_get_run_from_identifier_HEAD(self):
        job_run = self.resource.get_run_from_identifier('HEAD')
        self.job_scheduler.get_job_runs.assert_called_with()
        assert_equal(job_run, self.job_runs.get_newest.return_value)

    def test_get_run_from_identifier_number(self):
        job_run = self.resource.get_run_from_identifier('3')
        self.job_scheduler.get_job_runs.assert_called_with()
        assert_equal(job_run, self.job_runs.get_run_by_num.return_value)
        self.job_runs.get_run_by_num.assert_called_with(3)

    def test_get_run_from_identifier_state_name(self):
        job_run = self.resource.get_run_from_identifier('SUCC')
        assert_equal(
            job_run,
            self.job_runs.get_run_by_state_short_name.return_value,
        )
        self.job_runs.get_run_by_state_short_name.assert_called_with('SUCC')

    def test_get_run_from_identifier_negative_index(self):
        job_run = self.resource.get_run_from_identifier('-2')
        assert_equal(job_run, self.job_runs.get_run_by_index.return_value)
        self.job_runs.get_run_by_index.assert_called_with(-2)

    def test_getChild(self):
        autospec_method(self.resource.get_run_from_identifier)
        identifier = b'identifier'
        resource = self.resource.getChild(identifier, None)
        assert_equal(
            resource.job_run,
            self.resource.get_run_from_identifier.return_value,
        )

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
        assert_equal(resource.__class__, www.ActionRunHistoryResource)
        assert_equal(resource.action_runs, action_runs)


class EventResourceTestCase(WWWTestCase):
    @setup
    def setup_resource(self):
        self.name = 'the_name'
        self.resource = www.EventResource(self.name)

    @teardown
    def teardown_resource(self):
        event.EventManager.reset()

    def test_render_GET(self):
        recorder = event.get_recorder(self.name)
        ok_message, critical_message = 'ok message', 'critical message'
        recorder.ok(ok_message)
        recorder.critical(critical_message)
        response = self.resource.render_GET(self.request())
        names = [e['name'] for e in response['data']]
        assert_equal(names, [critical_message, ok_message])


class ConfigResourceTestCase(TestCase):
    @setup_teardown
    def setup_resource(self):
        self.mcp = mock.create_autospec(mcp.MasterControlProgram)
        self.resource = www.ConfigResource(self.mcp)
        self.controller = self.resource.controller = mock.create_autospec(
            controller.ConfigController,
        )
        with mock.patch(
            'tron.api.resource.respond',
            autospec=True,
        ) as self.respond:
            yield

    def test_render_GET(self):
        name = 'the_nane'
        request = build_request(name=name, no_header='1')
        self.resource.render_GET(request)
        self.controller.read_config.assert_called_with(name, add_header=False)
        self.respond.assert_called_with(
            request,
            self.resource.controller.read_config.return_value,
        )

    def test_render_POST_update(self):
        name, config, hash = 'the_name', 'config', 'hash'
        request = build_request(name=name, config=config, hash=hash)
        self.resource.render_POST(request)
        self.controller.update_config.assert_called_with(name, config, hash)
        response_content = {
            'status': 'Active',
            'error': self.controller.update_config.return_value,
        }
        self.respond.assert_called_with(request, response_content)

    def test_render_POST_delete(self):
        name, config, hash = 'the_name', '', ''
        request = build_request(name=name, config=config, hash=hash)
        self.resource.render_POST(request)
        self.controller.delete_config.assert_called_with(name, config, hash)
        response_content = {
            'status': 'Active',
            'error': self.controller.delete_config.return_value,
        }
        self.respond.assert_called_with(request, response_content)


if __name__ == '__main__':
    run()
