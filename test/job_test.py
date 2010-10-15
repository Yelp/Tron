
import tempfile
import shutil

from testify import *
from testify.utils import turtle
from tron import job, action, scheduler
from tron.utils import timeutils

class TestEnableDisableRuns(TestCase):
    @setup
    def setup(self):
        self.test_dir = tempfile.mkdtemp()
        self.action1 = action.Action(name="Test Act1")
        self.action1.command = "Test Command"
        
        self.job = job.Job("Test Job", self.action1)
        self.job.output_dir = self.test_dir
        self.job.scheduler = scheduler.DailyScheduler()
        self.job.node_pool = turtle.Turtle()
        self.action1.job = self.job
    
        self.job.enable_act = action.Action(name='Enable Act')
        self.job.enable_act.command = 'Enable Command'
        self.job.enable_act.job = self.job
        self.job.disable_act = action.Action(name='Disable Act')
        self.job.disable_act.command = 'Disable Command'
        self.job.disable_act.job = self.job

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)

    def test_enable(self):
        assert_equal(len(self.job.enable_runs), 0)
        self.job.enable()
        
        assert_equal(len(self.job.enable_runs), 1)
        er = self.job.enable_runs[0]
        assert er.is_running
        assert_equal(len(er.runs), 1)
        assert_equal(er.runs[0].action, self.job.enable_act)
        assert er.runs[0].is_running

        self.job.enable()
        assert_equal(len(self.job.enable_runs), 2)

    def test_disable(self):
        assert_equal(len(self.job.disable_runs), 0)
        self.job.disable()
        
        assert_equal(len(self.job.disable_runs), 1)
        assert self.job.disable_runs[0].is_running
        dr = self.job.disable_runs[0]
        assert_equal(len(dr.runs), 1)
        assert_equal(dr.runs[0].action, self.job.disable_act)
        assert dr.runs[0].is_running

        self.job.disable()
        assert_equal(len(self.job.disable_runs), 2)

class TestJobRun(TestCase):
    @setup
    def setup(self):
        self.test_dir = tempfile.mkdtemp()
        self.action1 = action.Action(name="Test Act1")
        self.action2 = action.Action(name="Test Act1")
        self.action2.required_actions.append(self.action1)
        self.action1.command = "Test Command"
        self.action2.command = "Test Command"

        self.job = job.Job("Test Job", self.action1)
        self.job.output_dir = self.test_dir
        self.job.topo_actions.append(self.action2)
        self.job.scheduler = scheduler.DailyScheduler()
        self.job.node_pool = turtle.Turtle()
        self.action1.job = self.job
        self.action2.job = self.job
     
    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)

    def test_set_run_time(self):
        jr = self.job.next_runs()[0]
        time = timeutils.current_time()
        jr.set_run_time(time)

        assert_equal(jr.run_time, time)
        assert_equal(jr.runs[0].run_time, time)
        assert not jr.runs[1].run_time

    def test_start(self):
        jr = self.job.next_runs()[0]
        jr.start()

        assert jr.runs[0].is_running
        assert not jr.runs[1].is_running

    def test_schedule(self):
        jr = self.job.next_runs()[0]
        assert jr.runs[0].is_scheduled
        assert jr.runs[1].is_queued

        jr.succeed()

        assert jr.runs[0].is_success
        assert jr.runs[1].is_success

        jr.schedule()

        assert jr.runs[0].is_scheduled
        assert jr.runs[1].is_queued

    def test_scheduled_start(self):
        self.job.queueing = True
        jr1 = self.job.next_runs()[0]
        jr2 = self.job.next_runs()[0]

        jr2.scheduled_start()
        assert jr2.is_queued

        self.job.queueing = False
        jr2.schedule()
        jr2.scheduled_start()
        assert jr2.is_cancelled

        jr1.scheduled_start()
        assert jr1.is_running

    def test_last_success_check(self):
        jr1 = self.job.next_runs()[0]
        jr2 = self.job.next_runs()[0]
        jr3 = self.job.next_runs()[0]

        jr2.last_success_check()
        assert_equal(self.job.last_success, jr2)
        
        jr1.last_success_check()
        assert_equal(self.job.last_success, jr2)

        jr3.last_success_check()
        assert_equal(self.job.last_success, jr3)

    def test_manual_start(self):
        jr1 = self.job.next_runs()[0]
        jr2 = self.job.next_runs()[0]

        jr2.manual_start()
        assert jr2.is_queued

        jr1.manual_start()
        assert jr1.is_running
    

class TestJob(TestCase):
    """Unit testing for Job class"""
    @setup
    def setup(self):
        self.test_dir = tempfile.mkdtemp()
        self.action = action.Action(name="Test Action")
        self.action.command = "Test Command"

        self.job = job.Job("Test Job", self.action)
        self.job.output_dir = self.test_dir
        self.job.node_pool = turtle.Turtle()
        self.job.scheduler = scheduler.DailyScheduler()
        self.action.job = self.job
      
    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)       

    def test_all_nodes_build_run(self):
        self.job.all_nodes = True
        self.job.node_pool = turtle.Turtle(nodes=[turtle.Turtle(), turtle.Turtle(), turtle.Turtle()])
        runs = self.job.build_runs()

        assert_equals(len(runs), 3)

        assert_equals(runs[0].node, self.job.node_pool.nodes[0])
        assert_equals(runs[1].node, self.job.node_pool.nodes[1])
        assert_equals(runs[2].node, self.job.node_pool.nodes[2])


    def test_remove_old_runs(self):
        self.job.run_limit = 3
        runs = []
        for i in range(6):
            runs.append(self.job.next_runs()[0])
            runs[i].runs[0].node = turtle.Turtle()

        self.job.remove_old_runs()
        assert_equals(len(self.job.runs), 6)
        
        runs[0].queue()
        runs[1].cancel()
        runs[2].succeed()
        self.job.remove_old_runs()
        assert_equals(len(self.job.runs), 6)

        runs[0].succeed()
        self.job.remove_old_runs()
        assert_equals(len(self.job.runs), 4)

        runs[4].succeed()
        self.job.remove_old_runs()
        assert_equals(len(self.job.runs), 3)
        
        runs[5].succeed()
        self.job.remove_old_runs()
        assert_equals(len(self.job.runs), 3)

        runs[3].cancel()
        for i in range(5):
            self.job.next_runs()[0]

        self.job.remove_old_runs()
        assert_equals(len(self.job.runs), 6)
 
    def test_next_to_finish(self):
        runs = []
        for i in range(5):
            runs.append(self.job.next_runs()[0])
            runs[i].runs[0].node = turtle.Turtle()

        assert_equals(self.job.next_to_finish(), runs[0])
        runs[0].succeed()
       
        assert_equals(self.job.next_to_finish(), runs[1])
        runs[1].queue()

        assert_equals(self.job.next_to_finish(), runs[1])
    
        runs[3].start()
        assert_equals(self.job.next_to_finish(), runs[3])

    def test_disable(self):
        runs = []
        for i in range(10):
            runs.append(self.job.next_runs()[0])
            runs[i].runs[0].node = turtle.Turtle()

        runs[0].start()
        runs[4].start()

        self.job.disable()
        assert not self.job.enabled
       
        for r in runs:
            assert r.is_running or r.job is None

    def test_enable(self):
        self.job.enable()
        assert self.job.enabled

    def test_next_num(self):
        job2 = job.Job("New Job")

        for i in range(10):
            assert_equals(self.job.next_num(), i)
            assert_equals(job2.next_num(), i)

    def test_get_run_by_num(self):
        runs = []
        
        for i in range(10):
            runs.append(self.job.next_runs()[0])

        for i in range(10):
            assert_equals(self.job.get_run_by_num(runs[i].run_num), runs[i])

    def test_build_run(self):
        self.job.node_pool = turtle.Turtle()
        act = action.Action("Action Test2")
        act.command = "test"
        act.job = self.job
        
        self.job.topo_actions.append(act)
        run1 = self.job.next_runs()[0]
        assert_equals(run1.runs[0].action, self.action)
        assert_equals(run1.runs[1].action, act)
        
        run2 = self.job.next_runs()[0]
        assert_equals(self.job.runs[1], run1)
        
        assert_equals(run2.runs[0].action, self.action)
        assert_equals(run2.runs[1].action, act)
        
    def test_manual_start_no_scheduled(self):
        r1 = self.job.build_run()
        r1.succeed()

        mr1 = self.job.manual_start()
        assert_equal(len(self.job.runs), 2)
        assert mr1.is_running

        mr2 = self.job.manual_start()
        assert_equal(len(self.job.runs), 3)
        assert mr2.is_queued

    def test_manual_start_scheduled_run(self):
        r1 = self.job.next_runs()[0]
        r1.succeed()
        r2 = self.job.next_runs()[0]

        mr1 = self.job.manual_start()
        assert_equal(len(self.job.runs), 3)

        assert_equal(self.job.runs[0], r2)
        assert_equal(self.job.runs[1], mr1)
        assert_equal(self.job.runs[2], r1)

        assert mr1.is_running

        mr2 = self.job.manual_start()
        assert_equal(len(self.job.runs), 4)
        assert_equal(self.job.runs[1], mr2)
        assert_equal(self.job.runs[2], mr1)

        assert mr2.is_queued

        
