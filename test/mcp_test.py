import datetime

from testify import *
from testify.utils import turtle

from tron.utils import timeutils
from tron import mcp, node, job, action, scheduler

def equals_with_delta(val, check, delta):
    return val <= check + delta and val >= check - delta

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
        self.action = action.Action("Test Action")
        self.action = action.Action("Test Action", self.action)
        
        self.action.scheduler = scheduler.IntervalScheduler(datetime.timedelta(seconds=0))
        self.action.command = "Test command"
        self.action.queueing = True
        self.action.node = turtle.Turtle()
        
    def test_state_changed(self):
        pass

    def test_store_data(self):
        pass

    def test_load_data(self):
        pass

class TestMasterControlProgram(TestCase):
    @setup
    def build_actions(self):
        self.action = action.Action("Test Action")
        self.job = job.Job("Test Job", self.action)
        self.mcp = mcp.MasterControlProgram(".")
        self.job.node_pool = node.NodePool('test hostname')

    def test_add_action(self):
        assert_equal(len(self.mcp.actions), 0)
        assert_equal(len(self.mcp.nodes), 0)
        assert_equal(len(self.mcp.actions), 0)

        self.mcp.add_job(self.job)
        
        assert_equal(len(self.mcp.actions), 1)
        assert_equal(self.mcp.actions[self.action.name], self.action)
        assert_equal(len(self.mcp.nodes), 1)
        assert_equal(self.mcp.nodes[0], self.job.node_pool.nodes[0])

        action2 = action.Action("Test Action2")
        job2 = job.Job("Test Job2", action2)
        action2.node_pool = self.action.node_pool

        self.mcp.add_job(job2)

        assert_equal(len(self.mcp.actions), 2)
        assert_equal(self.mcp.actions[action2.name], action2)
        assert_equal(len(self.mcp.nodes), 1)
        assert_equal(self.mcp.nodes[0], self.job.node_pool.nodes[0])

        try:
            self.mcp.add_job(self.job)
            assert False
        except mcp.JobExistsError:
            pass

    def test_schedule_next_run(self):
        act = action.Action("Test Action")
        jo = job.Job("Test Job", act)
        jo.node_pool = turtle.Turtle()
        jo.scheduler = scheduler.ConstantScheduler()

        act.job = jo
        act.command = "Test command"
        act.node = turtle.Turtle()

        def call_now(time, func, next):
            next.start()
            next.runs[0].succeed()

        callLater = mcp.reactor.callLater
        mcp.reactor.callLater = call_now
        next = self.mcp._schedule_next_run(jo)
        
        assert_equals(len(filter(lambda r:r.is_success, jo.runs)), 1)
        assert_equals(jo.topo_actions[0], next.runs[0].action)
        assert next.is_success

        mcp.reactor.callLater = callLater


