import datetime

from testify import run, setup, TestCase, assert_equal, turtle
from testify.assertions import assert_raises
from testify.test_case import class_setup, class_teardown

from tron import node
from tron.core.actionrun import ActionCommand, ActionRunContext, ActionRun, ActionRunCollection
from tron.core.actionrun import InvalidStartStateError
from tron.utils import timeutils

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


class ActionRunTestCase(TestCase):

    @setup
    def setup_action_run(self):
        anode = turtle.Turtle()
        output_path = ["random_dir"]
        self.command = "do command"
        self.action_run = ActionRun(
                "id",
                "action_name",
                anode,
                timeutils.current_time(),
                self.command,
                output_path=output_path)

    def test_init_state(self):
        assert_equal(self.action_run.state, ActionRun.STATE_SCHEDULED)

    def test_start(self):
        self.action_run.machine.transition('ready')
        assert self.action_run.start()
        assert self.action_run.is_starting
        assert self.action_run.start_time

    def test_start_bad_state(self):
        self.action_run.fail()
        assert_raises(InvalidStartStateError, self.action_run.start)

    def test_start_invalid_command(self):
        self.action_run.bare_command = "%(notfound)s"
        self.action_run.machine.transition('ready')
        assert self.action_run.start()
        assert self.action_run.is_failed
        assert_equal(self.action_run.exit_status, -1)

    def test_start_node_error(self):
        def raise_error(c):
            raise node.Error("The error")
        self.action_run.node = turtle.Turtle(submit_command=raise_error)
        self.action_run.machine.transition('ready')
        assert self.action_run.start()
        assert_equal(self.action_run.exit_status, -2)
        assert self.action_run.is_failed

    def test_build_action_command(self):
        self.action_run.watcher = watcher = turtle.Turtle()
        action_command = self.action_run.build_action_command()
        assert_equal(action_command.id, self.action_run.id)
        assert_equal(action_command.command, self.action_run.rendered_command)
        action_command.started()
        assert_equal(watcher.calls,
            [((action_command, action_command.RUNNING), {})])

    def test_watcher_running(self):
        self.action_run.build_action_command()
        self.action_run.machine.transition('start')
        assert self.action_run.watcher(
                self.action_run.action_command, ActionCommand.RUNNING)
        assert self.action_run.is_running

    def test_watcher_failstart(self):
        self.action_run.build_action_command()
        assert self.action_run.watcher(
                self.action_run.action_command, ActionCommand.FAILSTART)
        assert self.action_run.is_failed

    def test_watcher_exiting_fail(self):
        self.action_run.build_action_command()
        self.action_run.action_command.exit_status = -1
        self.action_run.machine.transition('start')
        assert self.action_run.watcher(
            self.action_run.action_command, ActionCommand.EXITING)
        assert self.action_run.is_failed
        assert_equal(self.action_run.exit_status, -1)

    def test_watcher_exiting_success(self):
        self.action_run.build_action_command()
        self.action_run.action_command.exit_status = 0
        self.action_run.machine.transition('start')
        self.action_run.machine.transition('started')
        assert self.action_run.watcher(
            self.action_run.action_command, ActionCommand.EXITING)
        assert self.action_run.is_succeeded
        assert_equal(self.action_run.exit_status, 0)

    def test_watcher_exiting_failunknown(self):
        self.action_run.build_action_command()
        self.action_run.machine.transition('start')
        self.action_run.machine.transition('started')
        assert self.action_run.watcher(
            self.action_run.action_command, ActionCommand.EXITING)
        assert self.action_run.is_unknown
        assert_equal(self.action_run.exit_status, None)

    def test_watcher_unhandled(self):
        self.action_run.build_action_command()
        assert self.action_run.watcher(
            self.action_run.action_command, ActionCommand.PENDING) is None
        assert self.action_run.is_scheduled

    def test_success(self):
        assert self.action_run.ready()
        self.action_run.machine.transition('start')
        self.action_run.machine.transition('started')

        assert self.action_run.is_running
        assert self.action_run.success()
        assert not self.action_run.is_running
        assert self.action_run.is_done
        assert self.action_run.end_time
        assert_equal(self.action_run.exit_status, 0)

    def test_success_bad_state(self):
        self.action_run.cancel()
        assert not self.action_run.success()

    def test_failure(self):
        self.action_run.fail(1)
        assert not self.action_run.is_running
        assert self.action_run.is_done
        assert self.action_run.end_time
        assert_equal(self.action_run.exit_status, 1)

    def test_failure_bad_state(self):
        self.action_run.fail(444)
        assert not self.action_run.fail(123)
        assert_equal(self.action_run.exit_status, 444)

    def test_skip(self):
        assert not self.action_run.is_running
        self.action_run.ready()
        assert self.action_run.start()

        assert self.action_run.fail(-1)
        assert self.action_run.skip()
        assert self.action_run.is_skipped

    def test_skip_bad_state(self):
        assert not self.action_run.skip()

    def test_render_command(self):
        self.action_run.context = {'stars': 'bright'}
        self.action_run.bare_command = "%(stars)s"
        assert_equal(self.action_run.render_command(), 'bright')

    def test_command_not_yet_rendered(self):
        assert_equal(self.action_run.command, self.command)

    def test_command_already_rendered(self):
        assert self.action_run.command
        self.action_run.bare_command = "new command"
        assert_equal(self.action_run.command, self.command)

    def test_command_failed_render(self):
        self.action_run.bare_command = "%(this_is_missing)s"
        assert_equal(self.action_run.command, ActionRun.FAILED_RENDER)

    def test__getattr__(self):
        assert self.action_run.is_succeeded is not None
        assert self.action_run.cancel()

    def test__getattr__missing_attribute(self):
        assert_raises(AttributeError,
            self.action_run.__getattr__, 'is_not_a_real_state')


class ActionRunStateRestoreTestCase(TestCase):

    @setup
    def setup_action_run(self):
        self.parent_context = {}
        self.output_path = ['one', 'two']
        self.state_data = {
            'job_run_id':       'theid',
            'action_name':      'theaction',
            'node_name':        'anode',
            'run_time':         'run_time',
            'command':          'do things',
            'start_time':       'start_time',
            'end_time':         'end_time',
            'state':            'succeeded'
        }

    def test_from_state(self):
        state_data = self.state_data
        action_run = ActionRun.from_state(
            state_data, self.parent_context, self.output_path)

        for key, value in self.state_data.iteritems():
            if key in ['state', 'node_name']:
                continue
            assert_equal(getattr(action_run, key), value)

        assert action_run.is_succeeded
        assert not action_run.is_cleanup

    def test_from_state_running(self):
        self.state_data['state'] = 'running'
        action_run = ActionRun.from_state(
            self.state_data, self.parent_context, self.output_path)
        assert action_run.is_unknown

    def test_from_state_no_node_name(self):
        del self.state_data['node_name']
        action_run = ActionRun.from_state(
            self.state_data, self.parent_context, self.output_path)
        assert action_run.node is None

    def test_from_state_with_node_exists(self):
        anode = turtle.Turtle(name="anode", hostname="box")
        node_store = node.NodePoolStore.get_instance()
        node_store.put(anode)

        action_run = ActionRun.from_state(
            self.state_data, self.parent_context, self.output_path)

        assert_equal(action_run.node, anode)
        node_store.clear()


class ActionRunCollectionTestCase(TestCase):

    @setup
    def setup_runs(self):
        self.run_map = {}
        self.collection = ActionRunCollection(self.run_map)

if __name__ == "__main__":
    run()