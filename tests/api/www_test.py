"""
Test cases for the web services interface to tron
"""
import twisted.web.resource
import twisted.web.http
import twisted.web.server

from testify import TestCase, class_setup, assert_equal, run, setup
from testify import class_teardown
from testify.utils import turtle
from tests import mocks
from tron.api import www
from tests.testingutils import Turtle

try:
    import simplejson
    assert simplejson
except ImportError:
    import json as simplejson

REQUEST = twisted.web.server.Request(turtle.Turtle(), None)
REQUEST.childLink = lambda val : "/jobs/%s" % val


class WWWTestCase(TestCase):
    """Patch www.response to not json encode."""

    @class_setup
    def mock_respond(self):
        self.orig_respond = www.respond
        www.respond = lambda _req, output: output

    @class_teardown
    def teardown_respond(self):
        www.respond = self.orig_respond

    @setup
    def setup_request(self):
        self.request = Turtle(args=[])

class ActionRunResourceTestCase(WWWTestCase):

    @setup
    def setup_resource(self):
        self.job_run = mocks.MockJobRun()
        self.action_name = 'theactionname'
        self.res = www.ActionRunResource(self.job_run, self.action_name)

    def test_render_GET(self):
        resp = self.res.render_GET(self.request)
        assert_equal(resp['id'], self.job_run.action_runs[self.action_name].id)


class RootTest(TestCase):
    @class_setup
    def build_root(self):
        self.mc = turtle.Turtle()
        self.resource = www.RootResource(self.mc)

    def test_status(self):
        """Verify that we return a status"""
        request = turtle.Turtle()
        resp = self.resource.getChildWithDefault("status", turtle.Turtle()).render_GET(request)

        status = simplejson.loads(resp)
        assert status['status']

    def test_children(self):
        """Verify that the jobs child is available"""
        child = self.resource.getChildWithDefault("jobs", turtle.Turtle())
        assert isinstance(child, www.JobsResource), child

        child = self.resource.getChildWithDefault("services", turtle.Turtle())
        assert isinstance(child, www.ServicesResource), child


class JobsTest(WWWTestCase):
    @class_setup
    def build_resource(self):
        self.mc = turtle.Turtle()
        self.job = turtle.Turtle(
            repr_data=lambda: {'name': 'testname'},
            name="testname",
            last_success=None,
            runs=turtle.Turtle(),
            scheduler_str="testsched",
            node_pool=mocks.MockNodePool()
        )

        self.mc.jobs = {self.job.name: turtle.Turtle(job=self.job)}
        self.resource = www.JobsResource(self.mc)

    def test_job_list(self):
        """Test that we get a proper job list"""
        result = self.resource.render_GET(REQUEST)
        assert 'jobs' in result
        assert result['jobs'][0]['name'] == "testname"

    def test_get_job(self):
        """Test that we can find a specific job"""
        child = self.resource.getChildWithDefault("testname", turtle.Turtle())
        assert isinstance(child, www.JobResource)
        assert child._job_sched.job is self.job

    def test_missing_job(self):
        child = self.resource.getChildWithDefault("bar", turtle.Turtle())
        assert isinstance(child, twisted.web.resource.NoResource)


class JobDetailTest(WWWTestCase):
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

        job_sched = turtle.Turtle(job=self.job)
        self.resource = www.JobResource(job_sched, turtle.Turtle())

    def test_detail(self):
        result = self.resource.render_GET(REQUEST)

        assert_equal(result['name'], self.job.name)
        assert_equal(len(result['runs']), 1)
        assert_equal(result['runs'][0]['id'], "foo.1")


class JobQueueTest(TestCase):
    """Test that we can create a new job run"""
    @class_setup
    def build_resource(self):
        self.job = turtle.Turtle(
                                 name="foo",
                                 runs=[],
                                 scheduler=None,
                                 node_pool=mocks.MockNodePool()
                                )

        self.resource = www.JobResource(self.job, turtle.Turtle())

    def test(self):
        req = twisted.web.server.Request(turtle.Turtle(), None)
        req.args = {'command': ['disable']}
        req.childLink = lambda val : "/jobs/foo/%s" % val
        self.resource.render_POST(req)

        # Verify the response
        assert_equal(req.code, twisted.web.http.OK)


class JobQueueDuplicateTest(TestCase):
    """Test that queuing a job that is already waiting is handled correctly"""
    @class_setup
    def build_resource(self):
        self.job = turtle.Turtle(
                                 name="foo",
                                 runs=[
                                     turtle.Turtle(
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

        self.resource = www.JobResource(self.job, turtle.Turtle())

    def test(self):
        req = twisted.web.server.Request(turtle.Turtle(), None)
        req.args = {'command': ['disable']}
        req.childLink = lambda val : "/jobs/foo/%s" % val
        self.resource.render_POST(req)

        # Verify the response
        assert_equal(req.code, twisted.web.http.OK)
        # Check if a run would have been queued
        func = self.job.build_run
        assert_equal(len(func.calls), 0)


class JobRunStartTest(TestCase):
    """Test that we can force start a job run"""
    @class_setup
    def build_resource(self):
        self.run = turtle.Turtle(
                      id="1",
                      start_time=None,
                      end_time=None,
                      exit_status=None,
                      is_done=False,
                      )

        self.job = turtle.Turtle(
                                 name="foo",
                                 runs=[self.run],
                                 scheduler=None,
                                 node_pool=mocks.MockNodePool(),
                                )

        self.resource = www.JobRunResource(self.run, Turtle())

    def test(self):
        req = twisted.web.server.Request(turtle.Turtle(), None)
        req.prePathURL = lambda : "/jobs/foo/1"
        req.args = {'command': ['start']}
        req.childLink = lambda val : "/jobs/foo/%s" % val
        self.resource.render_POST(req)

        # Verify the response
        assert_equal(req.code, twisted.web.http.OK)
        # Check if a run would have been queued
        func = self.run.start
        assert_equal(len(func.calls), 1)


class ServiceTest(TestCase):

    @class_setup
    def build_resource(self):
        self.mc = turtle.Turtle()
        self.service = turtle.Turtle(
                            name="testname",
                            state=turtle.Turtle(name="up"),
                            command="run_service.py",
                            count=2,
                            node_pool=mocks.MockNodePool(),
                            instances=[
                                turtle.Turtle(
                                    id="testname.0",
                                    node=mocks.MockNodePool(),
                                    state=turtle.Turtle(name="up")
                                )
                            ])

        self.mc.services = {self.service.name: self.service}

        self.resource = www.ServicesResource(self.mc)

    def test_service_list(self):
        """Test that we get a proper job list"""
        resp = self.resource.render_GET(REQUEST)
        result = simplejson.loads(resp)
        assert 'services' in result
        assert result['services'][0]['name'] == "testname"

    def test_get_service(self):
        """Test that we can find a specific service"""
        child = self.resource.getChildWithDefault("testname", turtle.Turtle())
        assert isinstance(child, www.ServiceResource)
        assert child._service is self.service

    def test_missing_service(self):
        child = self.resource.getChildWithDefault("bar", turtle.Turtle())
        assert isinstance(child, twisted.web.resource.NoResource)


if __name__ == '__main__':
    run()
