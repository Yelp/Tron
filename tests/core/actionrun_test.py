import datetime

from testify import run, setup, TestCase, assert_equal, turtle
from testify.test_case import class_setup, class_teardown

from tron.core.actionrun import ActionCommand, ActionRunContext
from tron.utils import timeutils

class ActionCommandTestCase(TestCase):

    @setup
    def setup_command(self):
        self.ac = ActionCommand("action.1.do", "do", None)

    def test_init(self):
        assert_equal(self.ac.state, ActionCommand.PENDING)

    def test_started(self):
        assert self.ac.started()
        assert self.ac.start_time is not None
        assert_equal(self.ac.state, ActionCommand.RUNNING)

    def test_started_already_started(self):
        self.ac.started()
        assert not self.ac.started()

    def test_exited(self):
        self.ac.started()
        assert self.ac.exited(123)
        assert_equal(self.ac.exit_status, 123)
        assert self.ac.end_time is not None

    def test_exited_from_pending(self):
        assert self.ac.exited(123)
        assert_equal(self.ac.state, ActionCommand.FAILSTART)

    def test_exited_bad_state(self):
        self.ac.started()
        self.ac.exited(123)
        assert not self.ac.exited(1)

    def test_write_stderr(self):
        message = "this is the message"
        # Test without a stderr
        self.ac.write_stderr(message)

        self.ac.serializer = turtle.Turtle()
        self.ac.write_stderr(message)
        assert_equal(len(self.ac.serializer.open.calls), 1)

    def test_done(self):
        self.ac.started()
        self.ac.exited(123)
        assert self.ac.done()

    def test_bad_state(self):
        assert not self.ac.done()


class ActionRunContextTestCase(TestCase):

    @class_setup
    def freeze_time(self):
        timeutils.override_current_time(datetime.datetime.now())
        self.now = timeutils.current_time()

    @class_teardown
    def unfreeze_time(self):
        timeutils.override_current_time(None)

    @setup
    def build_context(self):
        action_run = turtle.Turtle(
            id="runid",
            node=turtle.Turtle(hostname="nodename"),
            run_time=self.now
        )
        self.context = ActionRunContext(action_run)

    def test_runid(self):
        assert_equal(self.context.runid, 'runid')

    def test_daynumber(self):
        daynum = self.now.toordinal()
        assert_equal(self.context['daynumber'], daynum)

    def test_node_hostname(self):
        assert_equal(self.context.node, 'nodename')


if __name__ == "__main__":
    run()