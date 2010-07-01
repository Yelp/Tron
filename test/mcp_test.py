import datetime

from testify import *
from testify.utils import turtle

from tron.utils import timeutils
from tron import mcp, job, scheduler

class MockNode(object):
    def run(self, run):
        return run

class MockJob(turtle.Turtle):
    name = "Test Job"
    scheduler = scheduler.ConstantScheduler()
    
    def __init__(self):
        self.runs = []
        self.node = MockNode()
    def next_run(self):
        class MockJobRun(turtle.Turtle):
            should_start = True
            self.prev = None    
            
            def scheduled_start(self):
                self.start()

            def start(self):
                self.is_done = True
                self.is_success = True

            @property
            def is_cancelled(self):
                return True

        my_run = MockJobRun()
        self.runs.append(my_run)
        return my_run

def equals_with_delta(val, check, delta):
    return val <= check + delta and val >= check - delta

class TestMasterControlProgram(TestCase):
    @setup
    def build_jobs(self):
        self.job = MockJob()
        self.mcp = mcp.MasterControlProgram()
        
    def test_add_job(self):
        assert_equal(len(self.mcp.jobs), 0)
        assert_equal(len(self.mcp.nodes), 0)

        self.mcp.add_job(self.job)
        
        assert_equal(len(self.mcp.jobs), 1)
        assert_equal(self.mcp.jobs[self.job.name], self.job)
        assert_equal(len(self.mcp.nodes), 1)
        assert_equal(self.mcp.nodes[0], self.job.node)

        job2 = MockJob()
        job2.node = self.job.node
        job2.name = self.job.name + "2"
       
        self.mcp.add_job(job2)

        assert_equal(len(self.mcp.jobs), 2)
        assert_equal(self.mcp.jobs[job2.name], job2)
        assert_equal(len(self.mcp.nodes), 1)
        assert_equal(self.mcp.nodes[0], self.job.node)

        try:
            self.mcp.add_job(self.job)
            assert False
        except mcp.JobExistsError:
            pass

    def test_store_data(self):
        pass

    def test_load_data(self):
        pass

    def test_sleep_time(self):
        assert_equals(self.mcp._sleep_time(timeutils.current_time()), 0)
        
        seconds = 5
        time = self.mcp._sleep_time(timeutils.current_time() + datetime.timedelta(seconds=seconds)) 
        assert equals_with_delta(time, seconds, .01)
    
    def test_schedule_next_run(self):
        jo = job.Job()
        jo.command = "Test command"
        jo.scheduler = scheduler.ConstantScheduler()

        def call_now(time, func, next):
            next.succeed()

        callLater = mcp.reactor.callLater
        mcp.reactor.callLater = call_now
        next = self.mcp._schedule_next_run(jo)
        assert_equals(len(jo.scheduled), 1)
        assert_equals(jo, next.job)
        assert next.is_done

        mcp.reactor.callLater = callLater


