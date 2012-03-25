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


class TestActionRun(TestCase):
    """Unit testing for ActionRun class"""
    @setup
    def setup(self):
        self.test_dir = tempfile.mkdtemp()
        self.action = action.Action(name="Test Action")
        self.job = job.Job("Test Job", self.action)
        self.job.node_pool = testingutils.TestPool()
        self.job.output_path = self.test_dir
        self.job.scheduler = scheduler.DailyScheduler()
        self.job.queueing = True
        self.action.job = self.job
        self.action.command = "Test command"

        self.job_run = self.job.next_runs()[0]
        self.run = self.job_run.action_runs[0]

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)
        FileHandleManager.reset()

    def test_scheduled_start_succeed(self):
        self.job_run.scheduled_start()

        assert self.run.is_running
        assert_equal(get_num_runs_by_state(self.job, action.ActionRun.STATE_SCHEDULED), 0)
        assert_equal(get_num_runs_by_state(self.job, action.ActionRun.STATE_RUNNING), 1)
        assert_equal(self.run.state, action.ActionRun.STATE_RUNNING)

    def test_scheduled_start_wait(self):
        job_run2 = self.job.next_runs()[0]

        assert_equal(get_num_runs_by_state(self.job, action.ActionRun.STATE_SCHEDULED), 2)
        job_run2.scheduled_start()
        assert job_run2.is_queued
        assert_equal(get_num_runs_by_state(self.job, action.ActionRun.STATE_SCHEDULED), 1)

        self.job_run.scheduled_start()
        self.run.action_command.started()
        assert self.run.is_running

        self.run.succeed()
        assert self.run.is_success

        assert job_run2.action_runs[0].is_running

    def test_scheduled_start_cancel(self):
        self.job.queueing = False
        job_run2 = self.job.next_runs()[0]

        assert_equal(get_num_runs_by_state(self.job, action.ActionRun.STATE_SCHEDULED), 2)
        job_run2.scheduled_start()
        assert job_run2.is_cancelled

        self.job_run.scheduled_start()
        assert self.run.is_running

        self.run.succeed()
        assert self.run.is_success
        assert job_run2.is_cancelled


class ActionRunState(TestCase):
    """Check that our action runs can start/stop and manage their state"""
    @setup
    def build_job(self):
        self.test_dir = tempfile.mkdtemp()
        self.action = action.Action(name="Test Action")
        self.action.command = "Test command"

        self.action.job = turtle.Turtle()
        self.action.job.output_path = None
        self.run = self.action.build_run(turtle.Turtle(output_path=self.test_dir))

        self.run.job_run = turtle.Turtle()
        self.run.node = testingutils.TestNode()

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)
        FileHandleManager.reset()

    def test_success(self):
        assert not self.run.is_running
        assert not self.run.is_done

        assert self.run.attempt_start()

        assert self.run.is_running
        assert not self.run.is_done
        assert self.run.start_time

        assert self.run.succeed()

        assert not self.run.is_running
        assert self.run.is_done
        assert self.run.end_time
        assert_equal(self.run.exit_status, 0)

    def test_failure(self):
        self.run.attempt_start()

        self.run.fail(1)
        assert not self.run.is_running
        assert self.run.is_done
        assert self.run.end_time
        assert_equal(self.run.exit_status, 1)

        assert_raises(action.Error, self.run.start)

    def test_skip(self):
        assert not self.run.is_running
        assert self.run.attempt_start()

        assert self.run.fail(-1)
        assert self.run.skip()
        assert self.run.is_skipped


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


class ActionRunLogFileTest(TestCase):
    @setup
    def build_job(self):
        self.test_dir = tempfile.mkdtemp()
        self.action = action.Action(name="Test Action")

        self.job = job.Job("Test Job", self.action)
        self.job.node_pool = turtle.Turtle()
        self.job.output_path = self.test_dir

        self.action.job = self.job
        self.action.command = "Test command"

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)
        FileHandleManager.reset()

    def test_no_logging(self):
        run = self.action.build_run(turtle.Turtle(output_path=self.test_dir))
        run.node = testingutils.TestNode()
        run.attempt_start()

    def test_file_log(self):
        run = self.action.build_run(turtle.Turtle(output_path=self.test_dir))
        run.node = testingutils.TestNode()
        run.attempt_start()
        # Write some output to the files so they are created
        run.stdout_file.write("stdout")
        run.stderr_file.write("stderr")
        assert os.path.isfile(run.stdout_path)
        assert os.path.isfile(run.stderr_path)
        os.remove(run.stdout_path)
        os.remove(run.stderr_path)


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
