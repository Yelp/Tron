import datetime

from testify import setup, teardown, TestCase, run, assert_equal
from testify.utils import turtle
from tron.core import action, job
from tests import testingutils


class JobTestCase(TestCase):

    @setup
    def setup_job(self):
        pass

    @teardown
    def teardown_job(self):
        pass

    def test_all_nodes_build_run(self):
        self.job.all_nodes = True
        self.job.node_pool = turtle.Turtle(nodes=[turtle.Turtle(),
                                                  turtle.Turtle(),
                                                  turtle.Turtle()])
        runs = self.job.build_runs(None)

        assert_equal(len(runs), 3)

        assert_equal(runs[0].node, self.job.node_pool.nodes[0])
        assert_equal(runs[1].node, self.job.node_pool.nodes[1])
        assert_equal(runs[2].node, self.job.node_pool.nodes[2])


    def test_remove_old_runs(self):
        self.job.run_limit = 3
        runs = []
        for i in range(6):
            runs.append(self.job.next_runs()[0])
            runs[i].action_runs[0].node = testingutils.TestNode()

        self.job.remove_old_runs()
        assert_equal(len(self.job.runs), 6)

        runs[0].queue()
        runs[1].cancel()
        runs[2].succeed()
        self.job.remove_old_runs()
        assert_equal(len(self.job.runs), 6)

        runs[0].succeed()
        self.job.remove_old_runs()
        assert_equal(len(self.job.runs), 4)

        runs[4].succeed()
        self.job.remove_old_runs()
        assert_equal(len(self.job.runs), 3)

        runs[5].succeed()
        self.job.remove_old_runs()
        assert_equal(len(self.job.runs), 3)

        runs[3].cancel()
        for i in range(5):
            self.job.next_runs()[0]

        self.job.remove_old_runs()
        assert_equal(len(self.job.runs), 6)

    def test_update_last_success(self):
        jr1 = self.job.next_runs()[0]
        jr2 = self.job.next_runs()[0]
        jr3 = self.job.next_runs()[0]

        self.job.update_last_success(jr2)
        assert_equal(self.job.last_success, jr2)

        self.job.update_last_success(jr1)
        assert_equal(self.job.last_success, jr2)

        self.job.update_last_success(jr3)
        assert_equal(self.job.last_success, jr3)

    def test_newest(self):
        runs = []
        for i in range(5):
            runs.append(self.job.next_runs()[0])
            runs[i].action_runs[0].node = turtle.Turtle()

        assert_equal(self.job.newest(), runs[-1])
        runs[0].succeed()

        assert_equal(self.job.newest(), runs[-1])
        runs[1].queue()

        assert_equal(self.job.newest(), runs[-1])

    def test_next_to_finish(self):
        runs = []
        for i in range(5):
            runs.append(self.job.next_runs()[0])

        assert_equal(self.job.next_to_finish(), runs[0])
        runs[0].succeed()

        assert_equal(self.job.next_to_finish(), runs[1])
        runs[1].queue()

        assert_equal(self.job.next_to_finish(), runs[1])

        runs[3].start()
        assert_equal(self.job.next_to_finish(), runs[3])

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
            assert_equal(self.job.next_num(), i)
            assert_equal(job2.next_num(), i)

    def test_get_run_by_num(self):
        runs = []

        for i in range(10):
            runs.append(self.job.next_runs()[0])

        for i in range(10):
            assert_equal(self.job.get_run_by_num(runs[i].run_num), runs[i])

    def test_build_run(self):
        act = action.Action("Action Test2")
        act.command = "test"
        act.job = self.job

        self.job.topo_actions.append(act)
        run1 = self.job.next_runs()[0]
        assert_equal(run1.action_runs[0].action, self.action)
        assert_equal(run1.action_runs[1].action, act)

        run2 = self.job.next_runs()[0]
        assert_equal(self.job.runs[1], run1)

        assert_equal(run2.action_runs[0].action, self.action)
        assert_equal(run2.action_runs[1].action, act)

    def test_manual_start_no_scheduled(self):
        r1 = self.job.build_and_add_runs(None)[0]
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

    def test_build_action_dag(self):
        """Test that a required action can appear after the action requiring it
        in the all_actions list.  This is important because this input comes
        from a dict (so order is undefined).
        """
        dependent_action = action.Action(name="dep_act")
        required_action = action.Action(name="req_act",
            required_actions=[dependent_action])

        job_run = job.JobRun(self.job, run_num=1)
        self.job.build_action_dag(job_run, [
            required_action,
            dependent_action
        ])
        assert_equal(len(job_run.action_runs), 2)


class JobSchedulerTestCase(TestCase):

    @setup
    def setup_job(self):
        self.scheduler = turtle.Turtle()
        self.job = job.Job(
                "jobname",
                self.scheduler,
        )
        self.job_scheduler = job.JobScheduler(self.job)

    def test_enable(self):
        self.job_scheduler.enable()
        assert self.job.enabled
        # TODO:



if __name__ == '__main__':
    run()
