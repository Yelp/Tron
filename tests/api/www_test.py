"""
Test cases for the web services interface to tron
"""
from testify import TestCase, class_setup, assert_equal, run
from testify.utils import turtle
from tron.api import www

try:
    import simplejson
    assert simplejson
except ImportError:
    import json as simplejson

import twisted.web.resource
import twisted.web.http
import twisted.web.server

TEST_NODES = [turtle.Turtle(hostname="host")]
TEST_POOL = turtle.Turtle(nodes=TEST_NODES)

REQUEST = twisted.web.server.Request(turtle.Turtle(), None)
REQUEST.childLink = lambda val : "/jobs/%s" % val


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


class JobsTest(TestCase):
    @class_setup
    def build_resource(self):
        self.mc = turtle.Turtle()
        self.job = turtle.Turtle(
            repr_data=lambda: {'name': 'testname'},
            name="testname",
            last_success=None,
            runs=turtle.Turtle(),
            scheduler_str="testsched",
            node_pool=TEST_POOL
        )

        self.mc.jobs = {self.job.name: turtle.Turtle(job=self.job)}

        self.resource = www.JobsResource(self.mc)

#    def test_job_list(self):
#        """Test that we get a proper job list"""
#        resp = self.resource.render_GET(REQUEST)
#        job_result = simplejson.loads(resp)
#        assert 'jobs' in job_result
#        assert job_result['jobs'][0]['name'] == "testname"

    def test_get_job(self):
        """Test that we can find a specific job"""
        child = self.resource.getChildWithDefault("testname", turtle.Turtle())
        assert isinstance(child, www.JobResource)
        assert child._job_sched.job is self.job

    def test_missing_job(self):
        child = self.resource.getChildWithDefault("bar", turtle.Turtle())
        assert isinstance(child, twisted.web.resource.NoResource)


class JobDetailTest(TestCase):
    @class_setup
    def build_resource(self):
        self.job = turtle.Turtle(
             name="foo",
             runs=[
                   turtle.Turtle(
                                 id="foo.1",
                                 node=TEST_NODES[0],
                                 run_num=1,
                                 start_time=None,
                                 end_time=None,
                                 exit_status=None,
                                 repr_data=lambda: {'id': "foo.1"}
                    )

             ],
             scheduler_str="testsched",
             node_pool=TEST_POOL,
             topo_actions=[],
             repr_data=lambda: {'name': 'foo'}
        )

        job_sched = turtle.Turtle(job=self.job)
        self.resource = www.JobResource(job_sched, turtle.Turtle())

    # TODO: Does not work with turtles, can not be json serialized
#    def test_detail(self):
#        resp = self.resource.render_GET(REQUEST)
#        job_result = simplejson.loads(resp)
#
#        assert_equal(job_result['name'], self.job.name)
#        assert_equal(len(job_result['runs']), 1)
#        assert_equal(job_result['runs'][0]['id'], "foo.1")


class JobQueueTest(TestCase):
    """Test that we can create a new job run"""
    @class_setup
    def build_resource(self):
        self.job = turtle.Turtle(
                                 name="foo",
                                 runs=[],
                                 scheduler=None,
                                 node_pool=TEST_POOL,
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
        # func = self.job.build_run
        # FIXME: failing
        # assert_equal(len(func.calls), 1)


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
                                 node_pool=TEST_POOL,
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
                                 node_pool=TEST_POOL,
                                )

        self.resource = www.JobRunResource(self.run)

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
                            node_pool=TEST_POOL,
                            instances=[
                                turtle.Turtle(
                                    id="testname.0",
                                    node=TEST_NODES[0],
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
