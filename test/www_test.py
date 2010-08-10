"""
Test cases for the web services interface to tron
"""
from testify import *
from testify.utils import turtle

import simplejson
import twisted.web.resource
import twisted.web.http
import twisted.web.server

from tron import www

TEST_NODES = [turtle.Turtle(hostname="host")]
TEST_POOL = turtle.Turtle(nodes=TEST_NODES)
REQUEST = twisted.web.server.Request(turtle.Turtle(), None)
REQUEST.childLink = lambda val : "/jobs/%s" % val

# class JobsResourceServer(turtle.Turtle):
#     def childLink(self, child):
#         return "/jobs/%s" % child
# 
# class JobResourceServer(turtle.Turtle):
#     def __init__(self, job):
#         self._job = job
#     def childLink(self, child):
#         return "/jobs/%s/%s" % (self._job.name, child)
        
class RootTest(TestCase):
    @class_setup
    def build_root(self):
        self.mc = turtle.Turtle()
        self.resource = www.RootResource(self.mc)
    
    def test_status(self):
        """Verify that we return a status"""
        request = turtle.Turtle()
        resp = self.resource.render_GET(request)
        
        status = simplejson.loads(resp)
        assert status['status']

    def test_children(self):
        """Verify that the jobs child is available"""
        child = self.resource.getChildWithDefault("jobs", turtle.Turtle())
        assert isinstance(child, www.JobsResource), child


class JobsTest(TestCase):
    @class_setup
    def build_resource(self):
        self.mc = turtle.Turtle()
        self.job = turtle.Turtle(
                            name="testname", 
                            runs=[], 
                            scheduler_str="testsched", 
                            node_pool=TEST_POOL)
        
        self.mc.jobs = {self.job.name: self.job}

        self.resource = www.JobsResource(self.mc)

    def test_job_list(self):
        """Test that we get a proper job list"""
        resp = self.resource.render_GET(REQUEST)
        job_result = simplejson.loads(resp)
        assert 'jobs' in job_result
        assert job_result['jobs'][0]['name'] == "testname"
    
    def test_get_job(self):
        """Test that we can find a specific job"""
        child = self.resource.getChildWithDefault("testname", turtle.Turtle())
        assert isinstance(child, www.JobResource)
        assert child._job is self.job

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
                                                     id="1",
                                                     start_time=None,
                                                     end_time=None,
                                                     exit_status=None,
                                                     )
                                                     
                                 ],
                                 scheduler_str="testsched",
                                 node_pool=TEST_POOL,
                                 topo_actions=[],
                                )

        self.resource = www.JobResource(self.job, turtle.Turtle())
    
    def test_detail(self):
        resp = self.resource.render_GET(REQUEST)
        job_result = simplejson.loads(resp)
        
        assert_equal(job_result['name'], self.job.name)
        assert_equal(len(job_result['runs']), 1)
        assert_equal(job_result['runs'][0]['id'], "1")


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
        req.args = {'command': ['queue']}
        req.childLink = lambda val : "/jobs/foo/%s" % val
        resp = self.resource.render_POST(req)
        
        # Verify the response
        assert_equal(req.code, twisted.web.http.SEE_OTHER)
        assert req.responseHeaders.getRawHeaders('Location')[0].startswith("/jobs/%s/" % (self.job.name,))
        
        # Check if a run would have been queued
        func = self.job.build_run
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
        req.args = {'command': ['queue']}
        req.childLink = lambda val : "/jobs/foo/%s" % val
        resp = self.resource.render_POST(req)

        # Verify the response
        assert_equal(req.code, twisted.web.http.SEE_OTHER)
        assert_equal(req.responseHeaders.getRawHeaders('Location')[0], "/jobs/%s/%s" % (self.job.name, "1"))

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
        resp = self.resource.render_POST(req)

        # Verify the response
        assert_equal(req.code, twisted.web.http.SEE_OTHER)
        assert_equal(req.responseHeaders.getRawHeaders('Location')[0], "/jobs/1")

        # Check if a run would have been queued
        func = self.run.start
        assert_equal(len(func.calls), 0)


