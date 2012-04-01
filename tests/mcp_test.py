import datetime
import shutil
import StringIO
import tempfile

from testify import TestCase, class_setup, class_teardown, setup, teardown
from testify import assert_raises, assert_equal, suite, run
from testify.utils import turtle

from tron.core import action, job
from tron import mcp, scheduler, event, node
from tests import testingutils
from tron.utils import timeutils


# TODO: This does not test anything
class TestStateHandler(TestCase):
    @class_setup
    def class_setup_time(self):
        timeutils.override_current_time(datetime.datetime.now())
        self.now = timeutils.current_time()

    @class_teardown
    def class_teardown_time(self):
        timeutils.override_current_time(None)

    @setup
    def setup_mcp(self):
        nodes = turtle.Turtle()
        self.test_dir = tempfile.mkdtemp()
        self.mcp = mcp.MasterControlProgram(self.test_dir, "config")
        self.state_handler = self.mcp.state_handler
        self.action = action.Action("Test Action", "doit", nodes)

        self.action.command = "Test command"
        self.action.queueing = True
        self.action.node = turtle.Turtle()
        self.job = job.Job("Test Job", self.action)
        self.job.output_path = self.test_dir

        self.job.node_pool = turtle.Turtle()
        self.job.scheduler = scheduler.IntervalScheduler(datetime.timedelta(seconds=5))
        self.action.job = self.job

    @teardown
    def teardown_mcp(self):
        shutil.rmtree(self.test_dir)
        event.EventManager.get_instance().clear()


    @suite('integration')
    def test_reschedule(self):
        def callNow(sleep, func, run):
            raise NotImplementedError(sleep)

        #self.mcp.job_scheduler.next_runs(self.job)
        #callLate = reactor.callLater
        #reactor.callLater = callNow

        #try:
        #    self.state_handler._reschedule(run)
        #    assert False
        #except NotImplementedError as sleep:
        #    assert_equals(sleep, 0)
#
        #try:
        #    self.state_handler._reschedule(run)
        #    assert False
        #except NotImplementedError as sleep:
        #    assert_equals(sleep, 5)
#
        #reactor.callLater = callLate

    def test_store_data(self):
        pass

    def test_load_data(self):
        pass


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
        job_conf = {

        }
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
