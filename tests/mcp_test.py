import datetime
import os
import shutil
import StringIO
import tempfile

from testify import TestCase, class_setup, class_teardown, setup, teardown
from testify import assert_raises, assert_equal, suite, run
from testify.utils import turtle
import time
import yaml
from tests.assertions import assert_length
from tests.mocks import MockNode
import tron
from tron.config import config_parse

from tron.core import job, actionrun
from tron import mcp, scheduler, event, node, service
from tron.utils import timeutils


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

    def _load_state(self):
        # This is sloppy, but necessary for now, until state writing is fixed
        time.sleep(1)
        with open(self.state_handler.get_state_file_path(), 'r') as fh:
            return yaml.load(fh)

    @setup
    def setup_mcp(self):
        self.test_dir = tempfile.mkdtemp()
        node.NodePoolStore.get_instance().put(MockNode("localhost"))
        self.mcp = mcp.MasterControlProgram(self.test_dir, "config")
        self.state_handler = self.mcp.state_handler
        for job_name in ['job1', 'job2']:
            self.mcp.jobs[job_name] = self._build_job_sched(job_name)
        self.mcp.services['service1'] = self._build_service('service1')
        self.state_handler.writing_enabled = True

    @teardown
    def teardown_mcp(self):
        shutil.rmtree(self.test_dir)
        event.EventManager.get_instance().clear()

    @suite('integration')
    def test_store_data(self):
        self._create_runs(self.mcp.jobs['job1'])
        self._create_runs(self.mcp.jobs['job2'])
        self.mcp.state_handler.store_state()

        state_data = self._load_state()
        tstamp = time.mktime(self.now.timetuple())
        assert_equal(state_data['create_time'], tstamp)

        assert_length(state_data['jobs'], 2)
        assert_length(state_data['jobs']['job2']['runs'], 3)
        run_states = [
            r['runs'][0]['state'] for r in state_data['jobs']['job2']['runs']]
        assert_equal(run_states, ['scheduled', 'failed', 'succeeded'])

    @suite('integration')
    def test_load_data_no_state_file(self):
        assert not os.path.exists(self.state_handler.get_state_file_path())
        assert not self.state_handler.load_data()

    @suite('integration')
    def test_load_data(self):
        state_data = {
            'version': tron.__version_info__,
            'jobs': {'one': 1, 'two': 2},
            'services': {'one': 'ONE', 'two': 'TWO'}
        }

        with open(self.state_handler.get_state_file_path(), 'w') as fh:
            fh.write(yaml.dump(state_data))

        loaded_data = self.state_handler.load_data()
        assert_equal(loaded_data, state_data)


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
        shutil.rmtree(self.working_dir)

    def test_ssh_options_from_config(self):
        ssh_conf = turtle.Turtle(agent=False, identities=[])
        ssh_options = self.mcp._ssh_options_from_config(ssh_conf)

        assert_equal(ssh_options['agent'], False)
        assert_equal(ssh_options.identitys, [])
        # TODO: tests with agent and identities

if __name__ == '__main__':
    run()
