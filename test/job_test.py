import tempfile
import shutil
import datetime

from testify import *
from testify.utils import turtle
from tron import job, action, scheduler
from tron.utils import timeutils, testingutils


class TestJobRun(TestCase):
    @setup
    def setup(self):
        self.test_dir = tempfile.mkdtemp()
        self.action1 = action.Action(name="Test Act1")
        self.action2 = action.Action(name="Test Act2")
        self.action2.required_actions.append(self.action1)
        self.action1.command = "Test Command"
        self.action2.command = "Test Command"

        self.job = job.Job("Test Job", self.action1)
        self.job.output_path = self.test_dir
        self.job.topo_actions.append(self.action2)
        self.job.scheduler = scheduler.DailyScheduler()
        self.job.node_pool = testingutils.TestPool()
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
        assert_equal(jr.action_runs[0].run_time, time)
        assert_equal(jr.action_runs[1].run_time, time)

    def test_start(self):
        jr = self.job.next_runs()[0]
        jr.start()

        assert jr.action_runs[0].is_running
        assert not jr.action_runs[1].is_running

    def test_schedule(self):
        jr = self.job.next_runs()[0]
        assert jr.action_runs[0].is_scheduled
        assert jr.action_runs[1].is_scheduled

        jr.succeed()

        assert jr.action_runs[0].is_success
        assert jr.action_runs[1].is_success

        # This shouldn't do anything
        jr.schedule()

        assert jr.action_runs[0].is_success
        assert jr.action_runs[1].is_success

    def test_scheduled_start(self):
        self.job.queueing = True
        jr1 = self.job.next_runs()[0]
        jr2 = self.job.next_runs()[0]

        jr2.scheduled_start()
        assert jr2.is_queued

        self.job.queueing = False
        jr2.schedule()
        assert jr2.is_scheduled
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
        self.job.output_path = self.test_dir
        self.job.node_pool = testingutils.TestPool()
        self.job.scheduler = scheduler.DailyScheduler()
        self.action.job = self.job

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)

    def test_all_nodes_build_run(self):
        self.job.all_nodes = True
        self.job.node_pool = turtle.Turtle(nodes=[turtle.Turtle(),
                                                  turtle.Turtle(),
                                                  turtle.Turtle()])
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
            runs[i].action_runs[0].node = testingutils.TestNode()

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

    def test_newest(self):
        runs = []
        for i in range(5):
            runs.append(self.job.next_runs()[0])
            runs[i].action_runs[0].node = turtle.Turtle()

        assert_equals(self.job.newest(), runs[-1])
        runs[0].succeed()

        assert_equals(self.job.newest(), runs[-1])
        runs[1].queue()

        assert_equals(self.job.newest(), runs[-1])

    def test_next_to_finish(self):
        runs = []
        for i in range(5):
            runs.append(self.job.next_runs()[0])

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
        act = action.Action("Action Test2")
        act.command = "test"
        act.job = self.job

        self.job.topo_actions.append(act)
        run1 = self.job.next_runs()[0]
        assert_equals(run1.action_runs[0].action, self.action)
        assert_equals(run1.action_runs[1].action, act)

        run2 = self.job.next_runs()[0]
        assert_equals(self.job.runs[1], run1)

        assert_equals(run2.action_runs[0].action, self.action)
        assert_equals(run2.action_runs[1].action, act)

    def test_manual_start_no_scheduled(self):
        r1 = self.job.build_run()
        r1.succeed()

        mr1 = self.job.manual_start()[0]
        assert_equal(len(self.job.runs), 2)
        assert mr1.is_running
        assert mr1.run_time

        mr2 = self.job.manual_start()[0]
        assert_equal(len(self.job.runs), 3)
        assert mr2.is_queued

    def test_manual_start_scheduled_run(self):
        r1 = self.job.next_runs()[0]
        r1.succeed()
        r2 = self.job.next_runs()[0]

        mr1 = self.job.manual_start()[0]
        assert_equal(len(self.job.runs), 3)

        assert_equal(self.job.runs[0], r2)
        assert_equal(self.job.runs[1], mr1)
        assert_equal(self.job.runs[2], r1)

        assert mr1.is_running

        mr2 = self.job.manual_start()[0]
        assert_equal(len(self.job.runs), 4)
        assert_equal(self.job.runs[1], mr2)
        assert_equal(self.job.runs[2], mr1)

        assert mr2.is_queued

    def test_restore_run(self):
        self.job.topo_actions = []
        run_num = 4

        act1 = action.Action("Action Test1")
        act1.command = "test1"
        act1.job = self.job
        act1_id = ''.join([self.job.name, '.', str(run_num), '.', act1.name])

        act2 = action.Action("Action Test2")
        act2.command = "test2"
        act2.job = self.job
        act2_id = ''.join([self.job.name, '.', str(run_num), '.', act2.name])

        act3 = action.Action("Action Test3")
        act3.command = "test3"
        act3.job = self.job
        act3_id = ''.join([self.job.name, '.', str(run_num), '.', act3.name])

        cact = action.Action(name="Test Cleanup Action")
        cact.command = "Test Cleanup Command"
        cact.job = self.job
        cact_id = ''.join([self.job.name, '.', str(run_num), '.', cact.name])

        self.job.topo_actions.append(act1)
        self.job.topo_actions.append(act2)
        self.job.topo_actions.append(act3)
        self.job.cleanup_action = cact

        # filter out Action Test2 from restored state. upon trond restart
        #   restore_main_run(state_data) should filter out actions for runs
        #   before the new action was introduced
        state_data = \
        {'end_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 234159),
         'run_num': run_num,
         'run_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 125149),
         'runs': [{'command': act1.command,
               'end_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 232693),
               'id': act1_id,
               'run_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 125149),
               'start_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 128291),
               'state': 'scheduled'},
              {'command': act3.command,
               'end_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 234116),
               'id': act3_id,
               'run_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 125149),
               'start_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 133002),
               'state': 'scheduled'}],
         'cleanup_run': {'command': cact.command,
               'end_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 234116),
               'id': cact_id,
               'run_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 125149),
               'start_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 133002),
               'state': 'scheduled'},
         'start_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 128152)}


        job_run = self.job.restore_run(state_data)

        # act2 was filtered
        assert_equal(len(job_run.action_runs), 2)
        assert_equal(job_run.action_runs[0].id, act1_id)
        assert_equal(job_run.action_runs[1].id, act3_id)
        assert_equal(job_run.action_runs_with_cleanup[2].id, cact_id)



if __name__ == '__main__':
    run()
