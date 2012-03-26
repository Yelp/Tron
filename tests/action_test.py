import os
import tempfile
import shutil

from testify import setup, run, teardown
from testify import TestCase, assert_equal, assert_raises
from testify.utils import turtle

from tron import  action, job, scheduler
from tron.serialize.filehandler import FileHandleManager
from tron.utils import  testingutils

def get_num_runs_by_state(job, state):
    count = 0
    for run in job.runs:
        count += len(filter(lambda r: r.state == state, run.action_runs))
    return count


class TestAction(TestCase):
    """Unit testing for Action class"""
    @setup
    def setup(self):
        self.test_dir = tempfile.mkdtemp()
        self.action = action.Action(name="Test Action")
        self.action.command = "Test command"
        self.job = job.Job("Test Job", self.action)
        self.job.node_pool = turtle.Turtle()
        self.job.output_path = self.test_dir
        self.action.job = self.job

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)
        FileHandleManager.reset()

    def test_next_run(self):
        assert_equal(self.job.next_runs(), [])

        self.action.scheduler = turtle.Turtle()
        self.action.scheduler.next_run = lambda j:None

        assert_equal(self.job.next_runs(), [])
        assert_equal(get_num_runs_by_state(self.job, action.ActionRun.STATE_SCHEDULED), 0)

        self.job.scheduler = scheduler.ConstantScheduler()
        assert self.job.next_runs()[0]
        assert_equal(get_num_runs_by_state(self.job, action.ActionRun.STATE_SCHEDULED), 1)





class TestRunDependency(TestCase):
    @setup
    def build_job(self):
        self.test_dir = tempfile.mkdtemp()
        self.action = action.Action(name="Test Action1")
        self.action.command = "Test command1"

        self.dep_action = action.Action(name="Test Action2")
        self.dep_action.command = "Test command2"
        self.dep_action.required_actions.append(self.action)

        self.job = job.Job("Test Job", self.action)
        self.job.node_pool = testingutils.TestPool()
        self.job.output_path = self.test_dir
        self.action.job = self.job
        self.dep_action.job = self.job

        self.job.topo_actions.append(self.dep_action)
        self.job.scheduler = scheduler.DailyScheduler()
        self.job_run = self.job.next_runs()[0]
        self.run = self.job_run.action_runs[0]
        self.dep_run = self.job_run.action_runs[1]

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)
        FileHandleManager.reset()

    def test_success(self):
        assert self.run.is_scheduled
        assert self.dep_run.is_scheduled, self.dep_run.state
        self.job_run.start()

        assert self.dep_run.is_queued
        self.run.succeed()

        # Make it look like we started successfully
        #self.dep_run.action_command.machine.transition('start')

        assert self.dep_run.is_running
        assert not self.dep_run.is_done
        assert self.dep_run.start_time
        assert not self.dep_run.end_time

        self.dep_run.succeed()

        assert not self.dep_run.is_running
        assert self.dep_run.is_done
        assert self.dep_run.start_time
        assert self.dep_run.end_time

    def test_fail(self):
        self.job_run.start()
        self.run.fail(1)

        assert self.dep_run.is_queued, self.dep_run.state


class ActionRunBuildingTest(TestCase):
    """Check hat we can create and manage action runs"""
    @setup
    def build_job(self):
        self.test_dir = tempfile.mkdtemp()
        self.action = action.Action(name="Test Action")
        self.job = job.Job(self.action)
        self.job.node_pool = turtle.Turtle()
        self.job.output_path = self.test_dir
        self.action.job = self.job
        self.action.command = "Test Action Command"

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)
        FileHandleManager.reset()

    def test_build_run(self):
        run = self.job.build_run(None)
        self.action.build_run(run)
        assert run.id

    def test_no_schedule(self):
        runs = self.job.next_runs()
        assert_equal(runs, [])


class TestBuildingActionRun(TestCase):

    @setup
    def setup_action(self):
        node = turtle.Turtle()
        job = turtle.Turtle()
        self.action = action.Action("name", "do things", node, job=job)

    def test_build_action(self):
        job_run = turtle.Turtle(notify_action_run_completed=turtle.Turtle())
        action_run = self.action.build_run(job_run)
        assert_equal(action_run.state, action.ActionRun.STATE_SCHEDULED)

        action_run.machine.transition('queue')
        action_run.machine.transition('success')
        assert_equal(len(job_run.notify_action_run_completed.calls), 1)
        assert_equal(len(job_run.job.notify_state_changed.calls), 2)
        assert not job_run.cleanup_completed.calls

    def test_build_cleanup_action(self):
        job_run = turtle.Turtle()
        action_run = self.action.build_run(job_run, True)
        assert_equal(action_run.state, action.ActionRun.STATE_SCHEDULED)

        action_run.machine.transition('queue')
        action_run.machine.transition('success')
        assert_equal(len(job_run.job.notify_state_changed.calls), 2)
        assert_equal(len(job_run.notify_cleanup_action_run_completed.calls), 1)
        assert not job_run.run_completed.calls


if __name__ == '__main__':
    run()
