import datetime
import shutil
import StringIO
import tempfile

from testify import TestCase, class_setup, class_teardown, setup, teardown
from testify import assert_raises, assert_equal, suite, run
from testify.utils import turtle
from tron.config import config_parse

from tron.core import job, actionrun
from tron import mcp, scheduler, event, node, service
from tron.utils import timeutils
from tests.testingutils import Turtle


@suite('integration')
class StateHandlerIntegrationTestCase(TestCase):
    @class_setup
    def class_setup_time(self):
        timeutils.override_current_time(datetime.datetime.now())
        self.now = timeutils.current_time()

    @class_teardown
    def class_teardown_time(self):
        timeutils.override_current_time(None)

    def _advance_run(self, job_run, state):
        for action_run in job_run.action_runs:
            action_run.machine.state = state

    def _create_runs(self, job_sched):
        # Advance first run to succeeded
        run0 = job_sched.get_runs_to_schedule().next()
        self._advance_run(run0, actionrun.ActionRun.STATE_SUCCEEDED)

        # Advance second run to failure
        run1 = job_sched.get_runs_to_schedule().next()
        self._advance_run(run1, actionrun.ActionRun.STATE_FAILED)

        job_sched.get_runs_to_schedule().next()

    def _build_job_sched(self, name):
        """Create a JobScheduler with a name."""
        sched = scheduler.IntervalScheduler(interval=datetime.timedelta(30))
        config = {
            'name': name,
            'node': 'localhost',
            'schedule': 'interval 13s',
            'actions': [
                {
                    'name': 'task0',
                    'command': "echo do",
                },
            ]
        }
        job0 = job.Job.from_config(config_parse.valid_job(config), sched, {}, [])
        job_sched = job.JobScheduler(job0)
        return job_sched

    def _build_service(self, name):
        return service.Service(name)

    @setup
    def setup_mcp(self):
        self.test_dir = tempfile.mkdtemp()
        node.NodePoolStore.get_instance().put(Turtle(name="localhost"))
        self.mcp = mcp.MasterControlProgram(self.test_dir, "config")
        self.state_handler = self.mcp.state_handler
        for job_name in ['job1', 'job2']:
            self.mcp.jobs[job_name] = self._build_job_sched(job_name)
        self.mcp.services['service1'] = self._build_service('service1')

    @teardown
    def teardown_mcp(self):
        shutil.rmtree(self.test_dir)
        event.EventManager.get_instance().clear()

    @suite('integration', 'reactor')
    def test_store_data(self):
        self._create_runs(self.mcp.jobs['job1'])
        self._create_runs(self.mcp.jobs['job2'])
        self.mcp.state_handler.store_state()
        # TODO

    def test_restore_data_no_state_file(self):
        pass
        # TODO

    def test_restore_data(self):
        pass
        # TODO


class TestNoVersionState(TestCase):
    @setup
    def build_files(self):
        self.state_data = """
sample_job:
    disable_runs: []
    enable_runs: []
    enabled: true
    runs:
    -   end_time: null
        run_num: 68801
        run_time: &id001 2011-01-25 18:21:12.614273
        runs:
        -   command: do_stuff
            end_time: null
            id: batch_email_sender.68801.check
            run_time: *id001
            start_time: null
            state: 0
        start_time: null
"""
        self.data_file = StringIO.StringIO(self.state_data)

    @teardown
    def teardown_mcp(self):
        event.EventManager.get_instance().clear()

    def test(self):
        handler = mcp.StateHandler(turtle.Turtle(), "/tmp")
        assert_raises(mcp.UnsupportedVersionError, handler._load_data_file, self.data_file)

class FutureVersionTest(TestCase):
    @setup
    def build_files(self):
        self.state_data = """
version: [99, 0, 0]
jobs:
    sample_job:
        disable_runs: []
        enable_runs: []
        enabled: true
        runs:
        -   end_time: null
            run_num: 68801
            run_time: &id001 2011-01-25 18:21:12.614273
            runs:
            -   command: do_stuff
                end_time: null
                id: batch_email_sender.68801.check
                run_time: *id001
                start_time: null
                state: 0
            start_time: null
"""
        self.data_file = StringIO.StringIO(self.state_data)

    @teardown
    def teardown_mcp(self):
        event.EventManager.get_instance().clear()

    def test(self):
        handler = mcp.StateHandler(turtle.Turtle(), "/tmp")
        assert_raises(mcp.StateFileVersionError, handler._load_data_file, self.data_file)


class MasterControlProgramTestCase(TestCase):

    @setup
    def setup_mcp(self):
        self.working_dir = tempfile.mkdtemp()
        config_file = tempfile.NamedTemporaryFile(dir=self.working_dir)
        self.mcp = mcp.MasterControlProgram(self.working_dir, config_file.name)

    @teardown
    def teardown_mcp(self):
        self.mcp.nodes.clear()
        self.mcp.event_manager.clear()

    def test_live_reconfig(self):
        pass
        # TODO: some of these tests are in tests.config.reconfig_test

    def test_load_config(self):
        pass
        # TODO

    def config_lines(self):
        # TODO:
        pass

    def test_rewrite_config(self):
        pass
        # TODO:

    def test_apply_config(self):
        pass
        # TODO:

    def test_apply_working_directory(self):
        pass
        # TODO

    def test_ssh_options_from_config(self):
        ssh_conf = turtle.Turtle(agent=False, identities=[])
        ssh_options = self.mcp._ssh_options_from_config(ssh_conf)

        assert_equal(ssh_options['agent'], False)
        assert_equal(ssh_options.identitys, [])
        # TODO: tests with agent and identities

    def test_add_job(self):
        pass

    def test_add_job_already_exists(self):
        pass

    def test_remove_job(self):
        pass

    def test_disable_all(self):
        pass

    def test_enable_all(self):
        pass




if __name__ == '__main__':
    run()
