"""
Test cases for the web services interface to tron
"""
from testify import *
from testify.utils import turtle

import simplejson
import twisted.web.error

from tron import www

TEST_NODE = turtle.Turtle(hostname="host")
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
                            name="foo", 
                            runs=[], 
                            scheduler=None, 
                            node=TEST_NODE)
        
        self.mc.jobs = {self.job.name: self.job}

        self.resource = www.JobsResource(self.mc)
    
    def test_job_list(self):
        """Test that we get a proper job list"""
        resp = self.resource.render_GET(turtle.Turtle())
        job_result = simplejson.loads(resp)
        assert 'jobs' in job_result
        assert job_result['jobs'][0]['name'] == "foo"
    
    def test_get_job(self):
        """Test that we can find a specific job"""
        child = self.resource.getChildWithDefault("foo", turtle.Turtle())
        assert isinstance(child, www.JobResource)
        assert child._job is self.job

    def test_missing_job(self):
        child = self.resource.getChildWithDefault("bar", turtle.Turtle())
        assert isinstance(child, twisted.web.error.NoResource)
        

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
                                 scheduler=None,
                                 node=TEST_NODE,
                                )

        self.resource = www.JobResource(self.job)
    
    def test_detail(self):
        resp = self.resource.render_GET(turtle.Turtle())
        job_result = simplejson.loads(resp)
        
        assert_equal(job_result['name'], self.job.name)
        assert_equal(len(job_result['runs']), 1)
        assert_equal(job_result['runs'][0]['id'], "1")
        