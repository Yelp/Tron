"""
Test cases for the web services interface to tron
"""
import mock
import twisted.web.resource
import twisted.web.http
import twisted.web.server

from testify import TestCase, class_setup, assert_equal, run, setup
from testify import class_teardown, teardown
from testify import setup_teardown
from testify.assertions import assert_in
from testify.utils import turtle
from tests import mocks
from twisted.web import http
from tests.assertions import assert_call
from tron import event
from tron import mcp
from tron.api import www, controller
from tests.testingutils import Turtle
from tron.core import service, serviceinstance


REQUEST = twisted.web.server.Request(mock.Mock(), None)
REQUEST.childLink = lambda val : "/jobs/%s" % val


def build_request(**kwargs):
    args = dict((k, [v]) for k, v in kwargs.iteritems())
    return mock.create_autospec(twisted.web.server.Request, args=args)


class HandleCommandTestCase(TestCase):

    @setup_teardown
    def mock_respond(self):
        with mock.patch('tron.api.www.respond', autospec=True) as self.respond:
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
        self.respond.assert_called_with(request, {'error': str(error)},
            code=http.NOT_IMPLEMENTED)

    def test_handle_command(self):
        command = 'the command'
        request = build_request(command=command)
        mock_controller, obj = mock.Mock(), mock.Mock()
        response = www.handle_command(request, mock_controller, obj)
        mock_controller.handle_command.assert_called_with(command)
        assert_equal(response, self.respond.return_value)
        self.respond.assert_called_with(request,
                {'result': mock_controller.handle_command.return_value})


class WWWTestCase(TestCase):
    """Patch www.response to not json encode."""

    @class_setup
    def mock_respond(self):
        self.orig_respond = www.respond
        www.respond = lambda _req, output, code=None: output

    @class_teardown
    def teardown_respond(self):
        www.respond = self.orig_respond

    @setup
    def setup_request(self):
        self.request = mock.Mock(args={})

class ActionRunResourceTestCase(WWWTestCase):

    @setup
    def setup_resource(self):
        self.job_run = mocks.MockJobRun()
        self.action_name = 'theactionname'
        self.res = www.ActionRunResource(self.job_run, self.action_name)

    def test_render_GET(self):
        resp = self.res.render_GET(self.request)
        assert_equal(resp['id'], self.job_run.action_runs[self.action_name].id)

    def test_start_action_when_job_run_has_started(self):
        self.job_run.is_scheduled = False
        self.request.args['command'] = ['start']
        resp = self.res.render_POST(self.request)
        assert_in('Action run now in state', resp['result'])

    def test_start_action_when_job_run_not_started(self):
        self.job_run.is_scheduled = True
        self.request.args['command'] = ['start']
        resp = self.res.render_POST(self.request)
        assert_in('Failed to start action run', resp['result'])


class RootResourceTestCase(TestCase):

    @setup
    def build_resource(self):
        self.mcp = mock.create_autospec(mcp.MasterControlProgram)
        self.resource = www.RootResource(self.mcp)

    def test__init__(self):
        expected_children = ['jobs', 'services', 'config', 'status', 'events']
        assert_equal(set(expected_children), set(self.resource.children.keys()))

    def test_render_GET(self):
        expected_keys = [
            'jobs', 'jobs_href',
            'services', 'services_href',
            'config_href',
            'status_href']
        with mock.patch('tron.api.www.respond', autospec=True) as respond:
            self.resource.render_GET(build_request())
            assert_equal(set(respond.call_args[0][1].keys()), set(expected_keys))


class JobCollectionResourceTestCase(WWWTestCase):

    @class_setup
    def build_resource(self):
        self.job = mock.Mock(
            repr_data=lambda: {'name': 'testname'},
            name="testname",
            last_success=None,
            runs=mock.Mock(),
            scheduler_str="testsched",
            node_pool=mocks.MockNodePool()
        )
        self.mcp = mock.Mock()
        self.resource = www.JobCollectionResource(self.mcp)

    def test_render_GET(self):
        self.resource.get_data = Turtle()
        result = self.resource.render_GET(REQUEST)
        assert_call(self.resource.get_data, 0, False, False)
        assert 'jobs' in result

    def test_getChild(self):
        child = self.resource.getChild("testname", mock.Mock())
        assert isinstance(child, www.JobResource)
        self.mcp.get_job_by_name.assert_called_with("testname")

    def test_getChild_missing_job(self):
        self.mcp.get_job_by_name = lambda n: None
        child = self.resource.getChild("bar", mock.Mock())
        assert isinstance(child, twisted.web.resource.NoResource)


class JobResourceTestCase(WWWTestCase):
    @class_setup
    def build_resource(self):
        self.job = turtle.Turtle(
            name="foo",
            runs=mocks.MockJobRunCollection(runs=[
                mocks.MockJobRun(
                    id="foo.1",
                    node=mocks.MockNode(),
                    run_num=1,
                    start_time=None,
                    end_time=None,
                    exit_status=None,
                    repr_data=lambda: {'id': "foo.1"}
                )
            ]),
            scheduler_str="testsched",
            node_pool=mocks.MockNodePool(),
            topo_actions=[],
            repr_data=lambda: {'name': 'foo'}
        )

        job_sched = mock.Mock(job=self.job)
        self.resource = www.JobResource(job_sched)

    def test_detail(self):
        result = self.resource.render_GET(REQUEST)

        assert_equal(result['name'], self.job.name)
        assert_equal(len(result['runs']), 1)
        assert_equal(result['runs'][0]['id'], "foo.1")


class JobQueueTest(TestCase):
    """Test that we can create a new job run"""
    @class_setup
    def build_resource(self):
        self.job = mock.Mock(
                                 name="foo",
                                 runs=[],
                                 scheduler=None,
                                 node_pool=mocks.MockNodePool()
                                )

        self.resource = www.JobResource(self.job)

    def test(self):
        req = twisted.web.server.Request(mock.Mock(), None)
        req.args = {'command': ['disable']}
        req.childLink = lambda val : "/jobs/foo/%s" % val
        self.resource.render_POST(req)

        # Verify the response
        assert_equal(req.code, twisted.web.http.OK)


class JobQueueDuplicateTest(TestCase):
    """Test that queuing a job that is already waiting is handled correctly"""
    @class_setup
    def build_resource(self):
        self.job = mock.Mock(
                                 name="foo",
                                 runs=[
                                     mock.Mock(
                                                   id="1",
                                                   start_time=None,
                                                   end_time=None,
                                                   exit_status=None,
                                                   is_done=False,
                                                   )
                                 ],
                                 scheduler=None,
                                 node_pool=mocks.MockNodePool()
                                )

        self.resource = www.JobResource(self.job)

    def test(self):
        req = twisted.web.server.Request(mock.Mock(), None)
        req.args = {'command': ['disable']}
        req.childLink = lambda val : "/jobs/foo/%s" % val
        self.resource.render_POST(req)

        # Verify the response
        assert_equal(req.code, twisted.web.http.OK)
        # Check if a run would have been queued
        self.job.build_run.assert_not_called()


class JobRunStartTest(TestCase):
    """Test that we can force start a job run"""
    @class_setup
    def build_resource(self):
        self.run = mock.Mock(
                      id="1",
                      start_time=None,
                      end_time=None,
                      exit_status=None,
                      is_done=False,
                      )

        self.job = mock.Mock(
                                 name="foo",
                                 runs=[self.run],
                                 scheduler=None,
                                 node_pool=mocks.MockNodePool(),
                                )

        self.resource = www.JobRunResource(self.run, Turtle())

    def test(self):
        req = twisted.web.server.Request(mock.Mock(), None)
        req.prePathURL = lambda : "/jobs/foo/1"
        req.args = {'command': ['start']}
        req.childLink = lambda val : "/jobs/foo/%s" % val
        self.resource.render_POST(req)

        # Verify the response
        assert_equal(req.code, twisted.web.http.OK)
        # Check if a run would have been queued
        self.run.start.assert_called_with()


class ServiceResourceTestCase(WWWTestCase):

    @setup
    def setup_resource(self):
        instances = mock.create_autospec(serviceinstance.ServiceInstanceCollection)
        self.service = mock.create_autospec(service.Service,
            instances=instances, enabled=True, config=mock.Mock())
        self.resource = www.ServiceResource(self.service)
        self.resource.controller = mock.create_autospec(
            controller.ServiceController)

    def test_getChild(self):
        number = '3'
        resource = self.resource.getChild(number, None)
        assert isinstance(resource, www.ServiceInstanceResource)
        self.service.instances.get_by_number.assert_called_with(3)

    def test_render_GET(self):
        response = self.resource.render_GET(build_request())
        assert_equal(response['name'], self.service.name)

    def test_render_POST(self):
        response = self.resource.render_POST(build_request())
        assert_equal(response['result'],
            self.resource.controller.handle_command.return_value)


class ServiceCollectionResourceTestCase(TestCase):

    @setup
    def build_resource(self):
        self.mcp = mock.create_autospec(mcp.MasterControlProgram)
        self.resource = www.ServiceCollectionResource(self.mcp)
        self.resource.collection = mock.create_autospec(service.ServiceCollection)

    def test_getChild(self):
        child = self.resource.collection.get_by_name.return_value = mock.Mock()
        child_resource = self.resource.getChild('name', None)
        assert isinstance(child_resource, www.ServiceResource)
        assert_equal(child_resource.service, child)

    def test_getChild_missing(self):
        self.resource.collection.get_by_name.return_value = None
        child_resource = self.resource.getChild('name', None)
        assert isinstance(child_resource, twisted.web.resource.NoResource)

    def test_render_GET(self):
        service_count = 3
        services = [mock.MagicMock() for _ in xrange(service_count)]
        self.resource.collection.__iter__.return_value = services
        with mock.patch('tron.api.www.respond', autospec=True) as respond:
            response = self.resource.render_GET(build_request())
            assert_equal(response, respond.return_value)
            assert_equal(len(respond.call_args[0][1]['services']), service_count)


class EventResourceTestCase(WWWTestCase):

    @setup
    def setup_resource(self):
        self.name       = 'the_name'
        self.resource   = www.EventResource(self.name)

    @teardown
    def teardown_resource(self):
        event.EventManager.reset()

    def test_render_GET(self):
        recorder = event.get_recorder(self.name)
        ok_message, critical_message ='ok message', 'critical message'
        recorder.ok(ok_message)
        recorder.critical(critical_message)
        response = self.resource.render_GET(self.request())
        names = [e['name'] for e in response['data']]
        assert_equal(names, [ok_message, critical_message])


class ConfigResourceTestCase(TestCase):

    @setup_teardown
    def setup_resource(self):
        self.mcp = mock.create_autospec(mcp.MasterControlProgram)
        self.resource = www.ConfigResource(self.mcp)
        self.controller = self.resource.controller = mock.create_autospec(
            controller.ConfigController)
        with mock.patch('tron.api.www.respond', autospec=True) as self.respond:
            yield

    def test_render_GET(self):
        name = 'the_nane'
        request = build_request(name=name)
        self.resource.render_GET(request)
        self.controller.read_config.assert_called_with(name)
        self.respond.assert_called_with(request,
                self.resource.controller.read_config.return_value)

    def test_render_POST(self):
        name, config, hash = 'the_name', mock.Mock(), mock.Mock()
        request = build_request(name=name, config=config, hash=hash)
        self.resource.render_POST(request)
        self.controller.update_config.assert_called_with(name, config, hash)
        response_content = {
            'status': 'Active',
            'error': self.controller.update_config.return_value}
        self.respond.assert_called_with(request, response_content)


if __name__ == '__main__':
    run()
