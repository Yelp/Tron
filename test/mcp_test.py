import datetime

from testify import *
from testify.utils import turtle

from tron.utils import timeutils
from tron import mcp, job, job_flow, scheduler

def equals_with_delta(val, check, delta):
    return val <= check + delta and val >= check - delta

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

class MockNode(object):
    def run(self, run):
        return run

class TestGlobalFunctions(TestCase):
    def test_sleep_time(self):
        assert_equal(mcp.sleep_time(timeutils.current_time()), 0)
        assert_equal(mcp.sleep_time(timeutils.current_time() - datetime.timedelta(seconds=5)), 0)
        
        seconds = 5
        time = mcp.sleep_time(timeutils.current_time() + datetime.timedelta(seconds=seconds)) 
        assert equals_with_delta(time, seconds, .01)

class TestStateHandler(TestCase):
    @setup
    def setup(self):
        self.mcp = mcp.MasterControlProgram(".")
        self.state_handler = self.mcp.state_handler
        self.job = job.Job("Test Job")
        self.flow = job_flow.JobFlow("Test Flow", self.job)
        
        self.flow.scheduler = scheduler.IntervalScheduler(datetime.timedelta(seconds=0))
        self.job.command = "Test command"
        self.flow.queueing = True
        self.job.node = turtle.Turtle()
        
    def test_state_changed(self):
        pass

    def test_store_data(self):
        pass

    def test_load_data(self):
        pass

class TestMasterControlProgram(TestCase):
    @setup
    def build_jobs(self):
        self.job = MockJob()
        self.flow = job_flow.JobFlow("Test Flow", self.job)
        self.mcp = mcp.MasterControlProgram(".")
        
    def test_add_flow(self):
        assert_equal(len(self.mcp.flows), 0)
        assert_equal(len(self.mcp.nodes), 0)
        assert_equal(len(self.mcp.jobs), 0)

        self.mcp.add_flow(self.flow)
        
        assert_equal(len(self.mcp.jobs), 1)
        assert_equal(self.mcp.jobs[self.job.name], self.job)
        assert_equal(len(self.mcp.nodes), 1)
        assert_equal(self.mcp.nodes[0], self.job.node)

        job2 = MockJob()
        job2.node = self.job.node
        job2.name = self.job.name + "2"
        flow2 = job_flow.JobFlow("Test Flow2", job2)

        self.mcp.add_flow(flow2)

        assert_equal(len(self.mcp.jobs), 2)
        assert_equal(self.mcp.jobs[job2.name], job2)
        assert_equal(len(self.mcp.nodes), 1)
        assert_equal(self.mcp.nodes[0], self.job.node)

        try:
            self.mcp.add_flow(self.flow)
            assert False
        except mcp.FlowExistsError:
            pass

    def test_schedule_next_run(self):
        jo = job.Job()
        jo.command = "Test command"
        jo.node = turtle.Turtle()
        flo = job_flow.JobFlow("jo flo", jo)
        flo.scheduler = scheduler.ConstantScheduler()

        def call_now(time, func, next):
            next.start()
            next.runs[0].succeed()

        callLater = mcp.reactor.callLater
        mcp.reactor.callLater = call_now
        next = self.mcp._schedule_next_run(flo)
        
        assert_equals(len(filter(lambda r:r.state == job.JOB_RUN_SUCCEEDED, jo.runs)), 1)
        assert_equals(jo, next.runs[0].job)
        assert next.is_success

        mcp.reactor.callLater = callLater


