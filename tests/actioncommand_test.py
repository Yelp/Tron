from testify import TestCase, assert_equal, setup, turtle

from tron.actioncommand import ActionCommand
from tron.serialize import filehandler

class ActionCommandTestCase(TestCase):

    @setup
    def setup_command(self):
        null_fh = filehandler.NullFileHandle
        self.serializer = turtle.Turtle(open=lambda fn: null_fh)
        self.ac = ActionCommand("action.1.do", "do", self.serializer)

    def test_init(self):
        assert_equal(self.ac.state, ActionCommand.PENDING)

    def test_init_no_serializer(self):
        ac = ActionCommand("action.1.do", "do")
        ac.write_stdout("something")
        ac.write_stderr("else")
        assert_equal(ac.stdout, filehandler.NullFileHandle)
        ac.done()

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

    def test_write_stderr_no_fh(self):
        message = "this is the message"
        # Test without a stderr
        self.ac.write_stderr(message)

    def test_write_stderr(self):
        message = "this is the message"
        fh = turtle.Turtle()
        serializer = turtle.Turtle(open=lambda fn: fh)
        ac = ActionCommand("action.1.do", "do", serializer)

        ac.write_stderr(message)
        assert_equal(fh.write.calls, [((message,), {})])

    def test_done(self):
        self.ac.started()
        self.ac.exited(123)
        assert self.ac.done()

    def test_done_bad_state(self):
        assert not self.ac.done()

    def test_handle_errback(self):
        message = "something went wrong"
        self.ac.handle_errback(message)
        assert_equal(self.ac.state, ActionCommand.FAILSTART)
        assert self.ac.end_time