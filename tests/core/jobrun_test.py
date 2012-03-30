import shutil
from testify import TestCase, setup, teardown
from tron.serialize.filehandler import FileHandleManager


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
    def teardown_job(self):
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