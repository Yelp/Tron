import datetime
import shutil
import tempfile
import mock

from testify import run, setup, TestCase, assert_equal, turtle, teardown
from testify.assertions import assert_raises, assert_in
from tests import testingutils
from tests.assertions import assert_length
from tests.testingutils import Turtle, autospec_method

from tron import node, actioncommand
from tron.core import jobrun, actiongraph
from tron.core.actionrun import ActionCommand, ActionRun
from tron.core.actionrun import ActionRunCollection, ActionRunFactory
from tron.serialize import filehandler


class ActionRunFactoryTestCase(TestCase):

    @setup
    def setup_action_runs(self):
        self.run_time = datetime.datetime(2012, 3, 14, 15, 9 ,26)
        actions = [Turtle(name='act1'), Turtle(name='act2')]
        self.action_graph = actiongraph.ActionGraph(
                actions, dict((a.name, a) for a in actions))

        mock_node = mock.create_autospec(node.Node)
        self.job_run = jobrun.JobRun('jobname', 7, self.run_time, mock_node,
                action_graph=self.action_graph)

        self.action_state_data = {
            'job_run_id':       'job_run_id',
            'action_name':      'act1',
            'state':            'succeeded',
            'run_time':         'the_run_time',
            'start_time':       None,
            'end_time':         None,
            'command':          'do action1',
            'node_name':        'anode'
        }
        self.action_runner = mock.create_autospec(actioncommand.SubprocessActionRunnerFactory)

    def test_build_action_run_collection(self):
        collection = ActionRunFactory.build_action_run_collection(
            self.job_run, self.action_runner)
        assert_equal(collection.action_graph, self.action_graph)
        assert_in('act1', collection.run_map)
        assert_in('act2', collection.run_map)
        assert_length(collection.run_map, 2)
        assert_equal(collection.run_map['act1'].action_name, 'act1')

    def test_action_run_collection_from_state(self):
        state_data = [self.action_state_data]
        cleanup_action_state_data = {
            'job_run_id':       'job_run_id',
            'action_name':      'cleanup',
            'state':            'succeeded',
            'run_time':         self.run_time,
            'start_time':       None,
            'end_time':         None,
            'command':          'do cleanup',
            'node_name':        'anode'
        }
        collection = ActionRunFactory.action_run_collection_from_state(
            self.job_run, state_data, cleanup_action_state_data)

        assert_equal(collection.action_graph, self.action_graph)
        assert_length(collection.run_map, 2)
        assert_equal(collection.run_map['act1'].action_name, 'act1')
        assert_equal(collection.run_map['cleanup'].action_name, 'cleanup')

    def test_build_run_for_action(self):
        action = Turtle(
            name='theaction', node_pool=None, is_cleanup=False, command="doit")
        action_run = ActionRunFactory.build_run_for_action(
            self.job_run, action, self.action_runner)

        assert_equal(action_run.job_run_id, self.job_run.id)
        assert_equal(action_run.node, self.job_run.node)
        assert_equal(action_run.action_name, action.name)
        assert not action_run.is_cleanup
        assert_equal(action_run.command, action.command)

    def test_build_run_for_action_with_node(self):
        action = Turtle(name='theaction', is_cleanup=True, command="doit")
        action_run = ActionRunFactory.build_run_for_action(
            self.job_run, action, self.action_runner)

        assert_equal(action_run.job_run_id, self.job_run.id)
        assert_equal(action_run.node, action.node_pool.next.returns[0])
        assert action_run.is_cleanup
        assert_equal(action_run.action_name, action.name)
        assert_equal(action_run.command, action.command)

    def test_action_run_from_state(self):
        state_data = self.action_state_data
        action_run = ActionRunFactory.action_run_from_state(
                self.job_run, state_data)

        assert_equal(action_run.job_run_id, state_data['job_run_id'])
        assert not action_run.is_cleanup


class ActionRunTestCase(TestCase):

    @setup
    def setup_action_run(self):
        self.output_path = filehandler.OutputPath(tempfile.mkdtemp())
        self.action_runner = mock.create_autospec(
            actioncommand.NoActionRunnerFactory)
        self.command = "do command %(actionname)s"
        self.rendered_command = "do command action_name"
        self.action_run = ActionRun(
                "id",
                "action_name",
                mock.create_autospec(node.Node),
                self.command,
                output_path=self.output_path,
                action_runner=self.action_runner)

    @teardown
    def teardown_action_run(self):
        shutil.rmtree(self.output_path.base, ignore_errors=True)

    def test_init_state(self):
        assert_equal(self.action_run.state, ActionRun.STATE_SCHEDULED)

    def test_start(self):
        self.action_run.machine.transition('ready')
        assert self.action_run.start()
        assert self.action_run.is_starting
        assert self.action_run.start_time

    def test_start_bad_state(self):
        self.action_run.fail()
        assert not self.action_run.start()

    def test_start_invalid_command(self):
        self.action_run.bare_command = "%(notfound)s"
        self.action_run.machine.transition('ready')
        assert not self.action_run.start()
        assert self.action_run.is_failed
        assert_equal(self.action_run.exit_status, -1)

    def test_start_node_error(self):
        def raise_error(c):
            raise node.Error("The error")
        self.action_run.node = turtle.Turtle(submit_command=raise_error)
        self.action_run.machine.transition('ready')
        assert not self.action_run.start()
        assert_equal(self.action_run.exit_status, -2)
        assert self.action_run.is_failed

    @mock.patch('tron.core.actionrun.filehandler', autospec=True)
    def test_build_action_command(self, mock_filehandler):
        autospec_method(self.action_run.watch)
        serializer = mock_filehandler.OutputStreamSerializer.return_value
        action_command = self.action_run.build_action_command()
        assert_equal(action_command, self.action_run.action_command)
        assert_equal(action_command, self.action_runner.create.return_value)
        self.action_runner.create.assert_called_with(
            self.action_run.id, self.action_run.command, serializer)
        mock_filehandler.OutputStreamSerializer.assert_called_with(
            self.action_run.output_path)
        self.action_run.watch.assert_called_with(action_command)

    def test_handler_running(self):
        self.action_run.build_action_command()
        self.action_run.machine.transition('start')
        assert self.action_run.handler(
                self.action_run.action_command, ActionCommand.RUNNING)
        assert self.action_run.is_running

    def test_handler_failstart(self):
        self.action_run.build_action_command()
        assert self.action_run.handler(
                self.action_run.action_command, ActionCommand.FAILSTART)
        assert self.action_run.is_failed

    def test_handler_exiting_fail(self):
        self.action_run.build_action_command()
        self.action_run.action_command.exit_status = -1
        self.action_run.machine.transition('start')
        assert self.action_run.handler(
            self.action_run.action_command, ActionCommand.EXITING)
        assert self.action_run.is_failed
        assert_equal(self.action_run.exit_status, -1)

    def test_handler_exiting_success(self):
        self.action_run.build_action_command()
        self.action_run.action_command.exit_status = 0
        self.action_run.machine.transition('start')
        self.action_run.machine.transition('started')
        assert self.action_run.handler(
            self.action_run.action_command, ActionCommand.EXITING)
        assert self.action_run.is_succeeded
        assert_equal(self.action_run.exit_status, 0)

    def test_handler_exiting_failunknown(self):
        self.action_run.action_command = mock.create_autospec(
            actioncommand.ActionCommand, exit_status=None)
        self.action_run.machine.transition('start')
        self.action_run.machine.transition('started')
        assert self.action_run.handler(
            self.action_run.action_command, ActionCommand.EXITING)
        assert self.action_run.is_unknown
        assert_equal(self.action_run.exit_status, None)

    def test_handler_unhandled(self):
        self.action_run.build_action_command()
        assert self.action_run.handler(
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

    def test_state_data(self):
        state_data = self.action_run.state_data
        assert_equal(state_data['command'], self.action_run.bare_command)
        assert not self.action_run.rendered_command
        assert not state_data['rendered_command']

    def test_state_data_after_rendered(self):
        command = self.action_run.command
        state_data = self.action_run.state_data
        assert_equal(state_data['command'], command)
        assert_equal(state_data['rendered_command'], command)

    def test_render_command(self):
        self.action_run.context = {'stars': 'bright'}
        self.action_run.bare_command = "%(stars)s"
        assert_equal(self.action_run.render_command(), 'bright')

    def test_command_not_yet_rendered(self):
        assert_equal(self.action_run.command, self.rendered_command)

    def test_command_already_rendered(self):
        assert self.action_run.command
        self.action_run.bare_command = "new command"
        assert_equal(self.action_run.command, self.rendered_command)

    def test_command_failed_render(self):
        self.action_run.bare_command = "%(this_is_missing)s"
        assert_equal(self.action_run.command, ActionRun.FAILED_RENDER)

    def test_is_complete(self):
        self.action_run.machine.state = ActionRun.STATE_SUCCEEDED
        assert self.action_run.is_complete
        self.action_run.machine.state = ActionRun.STATE_SKIPPED
        assert self.action_run.is_complete
        self.action_run.machine.state = ActionRun.STATE_RUNNING
        assert not self.action_run.is_complete

    def test_is_broken(self):
        self.action_run.machine.state = ActionRun.STATE_UNKNOWN
        assert self.action_run.is_broken
        self.action_run.machine.state = ActionRun.STATE_FAILED
        assert self.action_run.is_broken
        self.action_run.machine.state = ActionRun.STATE_QUEUED
        assert not self.action_run.is_broken

    def test__getattr__(self):
        assert not self.action_run.is_succeeded
        assert not self.action_run.is_failed
        assert not self.action_run.is_queued
        assert self.action_run.is_scheduled
        assert self.action_run.cancel()
        assert self.action_run.is_cancelled

    def test__getattr__missing_attribute(self):
        assert_raises(AttributeError,
            self.action_run.__getattr__, 'is_not_a_real_state')


class ActionRunStateRestoreTestCase(testingutils.MockTimeTestCase):

    now = datetime.datetime(2012, 3, 14, 15, 19)

    @setup
    def setup_action_run(self):
        self.parent_context = {}
        self.output_path = ['one', 'two']
        self.state_data = {
            'job_run_id':       'theid',
            'action_name':      'theaction',
            'node_name':        'anode',
            'command':          'do things',
            'start_time':       'start_time',
            'end_time':         'end_time',
            'state':            'succeeded'
        }
        self.run_node = Turtle()

    def test_from_state(self):
        state_data = self.state_data
        action_run = ActionRun.from_state(state_data, self.parent_context,
                list(self.output_path), self.run_node)

        for key, value in self.state_data.iteritems():
            if key in ['state', 'node_name']:
                continue
            assert_equal(getattr(action_run, key), value)

        assert action_run.is_succeeded
        assert not action_run.is_cleanup
        assert_equal(action_run.output_path[:2], self.output_path)

    def test_from_state_running(self):
        self.state_data['state'] = 'running'
        action_run = ActionRun.from_state(self.state_data,
                self.parent_context, self.output_path, self.run_node)
        assert action_run.is_unknown
        assert_equal(action_run.exit_status, 0)
        assert_equal(action_run.end_time, self.now)

    def test_from_state_queued(self):
        self.state_data['state'] = 'queued'
        action_run = ActionRun.from_state(self.state_data, self.parent_context,
                self.output_path, self.run_node)
        assert action_run.is_queued

    def test_from_state_no_node_name(self):
        del self.state_data['node_name']
        action_run = ActionRun.from_state(self.state_data,
                self.parent_context, self.output_path, self.run_node)
        assert_equal(action_run.node, self.run_node)

    @mock.patch('tron.core.actionrun.node.NodePoolRepository')
    def test_from_state_with_node_exists(self, mock_store):
        ActionRun.from_state(self.state_data,
                self.parent_context, self.output_path, self.run_node)
        mock_store.get_instance().get_node.assert_called_with(
            self.state_data['node_name'], self.run_node)

    def test_from_state_before_rendered_command(self):
        self.state_data['command'] = 'do things %(actionname)s'
        self.state_data['rendered_command'] = None
        action_run = ActionRun.from_state(self.state_data,
                self.parent_context, self.output_path, self.run_node)
        assert_equal(action_run.bare_command, self.state_data['command'])
        assert not action_run.rendered_command

    def test_from_state_old_state(self):
        self.state_data['command'] = 'do things %(actionname)s'
        action_run = ActionRun.from_state(self.state_data,
                self.parent_context, self.output_path, self.run_node)
        assert_equal(action_run.bare_command, self.state_data['command'])
        assert not action_run.rendered_command

    def test_from_state_after_rendered_command(self):
        self.state_data['command'] = 'do things theaction'
        self.state_data['rendered_command'] = self.state_data['command']
        action_run = ActionRun.from_state(self.state_data,
                self.parent_context, self.output_path, self.run_node)
        assert_equal(action_run.bare_command, self.state_data['command'])
        assert_equal(action_run.rendered_command, self.state_data['command'])


class ActionRunCollectionTestCase(TestCase):

    def _build_run(self, name):
        mock_node = mock.create_autospec(node.Node)
        return ActionRun("id", name, mock_node, self.command,
            output_path=self.output_path)

    @setup
    def setup_runs(self):
        action_names = ['action_name', 'second_name', 'cleanup']

        action_graph = [
            mock.Mock(name=name, required_actions=[])
            for name in action_names
        ]
        self.action_graph = actiongraph.ActionGraph(
            action_graph, dict((a.name, a) for a in action_graph))
        self.output_path = filehandler.OutputPath(tempfile.mkdtemp())
        self.command = "do command"
        self.action_runs = [self._build_run(name) for name in action_names]
        self.run_map = dict((a.action_name, a) for a in self.action_runs)
        self.run_map['cleanup'].is_cleanup = True
        self.collection = ActionRunCollection(self.action_graph, self.run_map)

    @teardown
    def teardown_action_run(self):
        shutil.rmtree(self.output_path.base, ignore_errors=True)

    def test__init__(self):
        assert_equal(self.collection.action_graph, self.action_graph)
        assert_equal(self.collection.run_map, self.run_map)
        assert self.collection.proxy_action_runs_with_cleanup

    def test_action_runs_for_actions(self):
        actions = [Turtle(name='action_name')]
        action_runs = self.collection.action_runs_for_actions(actions)
        assert_equal(list(action_runs), self.action_runs[:1])

    def test_get_action_runs_with_cleanup(self):
        runs = self.collection.get_action_runs_with_cleanup()
        assert_equal(set(runs), set(self.action_runs))

    def test_get_action_runs(self):
        runs = self.collection.get_action_runs()
        assert_equal(set(runs), set(self.action_runs[:2]))

    def test_cleanup_action_run(self):
        assert_equal(self.action_runs[2], self.collection.cleanup_action_run)

    def test_state_data(self):
        state_data = self.collection.state_data
        assert_length(state_data, len(self.action_runs[:2]))

    def test_cleanup_action_state_data(self):
        state_data = self.collection.cleanup_action_state_data
        assert_equal(state_data['action_name'], 'cleanup')

    def test_cleanup_action_state_data_no_cleanup_action(self):
        del self.collection.run_map['cleanup']
        assert not self.collection.cleanup_action_state_data

    def test_get_startable_action_runs(self):
        action_runs = self.collection.get_startable_action_runs()
        assert_equal(set(action_runs), set(self.action_runs[:2]))

    def test_get_startable_action_runs_none(self):
        self.collection.run_map.clear()
        action_runs = self.collection.get_startable_action_runs()
        assert_equal(set(action_runs), set())

    def test_has_startable_action_runs(self):
        assert self.collection.has_startable_action_runs

    def test_has_startable_action_runs_false(self):
        self.collection.run_map.clear()
        assert not self.collection.has_startable_action_runs

    def test_is_complete_false(self):
        assert not self.collection.is_complete

    def test_is_complete_true(self):
        for action_run in self.collection.action_runs_with_cleanup:
            action_run.machine.state = ActionRun.STATE_SKIPPED
        assert self.collection.is_complete

    def test_is_done_false(self):
        assert not self.collection.is_done

    def test_is_done_false_because_of_running(self):
        action_run = self.collection.run_map['action_name']
        action_run.machine.state = ActionRun.STATE_RUNNING
        assert not self.collection.is_done

    def test_is_done_true_because_blocked(self):
        self.run_map['action_name'].machine.state = ActionRun.STATE_FAILED
        self.run_map['second_name'].machine.state = ActionRun.STATE_QUEUED
        autospec_method(self.collection._is_run_blocked)
        blocked_second_action_run = lambda ar: ar == self.run_map['second_name']
        self.collection._is_run_blocked.side_effect = blocked_second_action_run
        assert self.collection.is_done
        assert self.collection.is_failed

    def test_is_done_true(self):
        for action_run in self.collection.action_runs_with_cleanup:
            action_run.machine.state = ActionRun.STATE_FAILED
        assert self.collection.is_done

    def test_is_failed_false_not_done(self):
        self.run_map['action_name'].machine.state = ActionRun.STATE_FAILED
        assert not self.collection.is_failed

    def test_is_failed_false_no_failed(self):
        for action_run in self.collection.action_runs_with_cleanup:
            action_run.machine.state = ActionRun.STATE_SUCCEEDED
        assert not self.collection.is_failed

    def test_is_failed_true(self):
        for action_run in self.collection.action_runs_with_cleanup:
            action_run.machine.state = ActionRun.STATE_FAILED
        assert self.collection.is_failed

    def test__getattr__(self):
        assert self.collection.is_scheduled
        assert not self.collection.is_cancelled
        assert not self.collection.is_running
        assert self.collection.ready()

    def test__str__(self):
        self.collection._is_run_blocked = lambda r: r.action_name != 'cleanup'
        expected = [
            "ActionRunCollection",
            "second_name(scheduled:blocked)",
            "action_name(scheduled:blocked)",
            "cleanup(scheduled)"
        ]
        for expectation in expected:
            assert_in(expectation, str(self.collection))

    def test_end_time(self):
        max_end_time = datetime.datetime(2013, 6, 15)
        self.run_map['action_name'].machine.state = ActionRun.STATE_FAILED
        self.run_map['action_name'].end_time = datetime.datetime(2013, 5, 12)
        self.run_map['second_name'].machine.state = ActionRun.STATE_SUCCEEDED
        self.run_map['second_name'].end_time = max_end_time
        assert_equal(self.collection.end_time, max_end_time)

    def test_end_time_not_done(self):
        self.run_map['action_name'].end_time = datetime.datetime(2013, 5, 12)
        self.run_map['action_name'].machine.state = ActionRun.STATE_FAILED
        self.run_map['second_name'].end_time = None
        self.run_map['second_name'].machine.state = ActionRun.STATE_RUNNING
        assert_equal(self.collection.end_time, None)

    def test_end_time_not_started(self):
        assert_equal(self.collection.end_time, None)


class ActionRunCollectionIsRunBlockedTestCase(TestCase):

    def _build_run(self, name):
        mock_node = mock.create_autospec(node.Node)
        return ActionRun("id", name, mock_node, self.command,
            output_path=self.output_path)

    @setup
    def setup_collection(self):
        action_names = ['action_name', 'second_name', 'cleanup']

        action_graph = [
            Turtle(name=name, required_actions=[])
            for name in action_names
        ]
        self.second_act = second_act = action_graph.pop(1)
        second_act.required_actions.append(action_graph[0])
        action_map = dict((a.name, a) for a in action_graph)
        action_map['second_name'] = second_act
        self.action_graph = actiongraph.ActionGraph(action_graph, action_map)

        self.output_path = filehandler.OutputPath(tempfile.mkdtemp())
        self.command = "do command"
        self.action_runs = [self._build_run(name) for name in action_names]
        self.run_map = dict((a.action_name, a) for a in self.action_runs)
        self.run_map['cleanup'].is_cleanup = True
        self.collection = ActionRunCollection(self.action_graph, self.run_map)

    @teardown
    def teardown_action_run(self):
        shutil.rmtree(self.output_path.base, ignore_errors=True)

    def test_is_run_blocked_no_required_actions(self):
        assert not self.collection._is_run_blocked(self.run_map['action_name'])

    def test_is_run_blocked_completed_run(self):
        self.run_map['second_name'].machine.state = ActionRun.STATE_FAILED
        assert not self.collection._is_run_blocked(self.run_map['second_name'])

        self.run_map['second_name'].machine.state = ActionRun.STATE_RUNNING
        assert not self.collection._is_run_blocked(self.run_map['second_name'])

    def test_is_run_blocked_required_actions_completed(self):
        self.run_map['action_name'].machine.state = ActionRun.STATE_SKIPPED
        assert not self.collection._is_run_blocked(self.run_map['second_name'])

    def test_is_run_blocked_required_actions_blocked(self):
        third_act = Turtle(name='third_act', required_actions=[self.second_act])
        self.action_graph.action_map['third_act'] = third_act
        self.run_map['third_act'] = self._build_run('third_act')

        self.run_map['action_name'].machine.state = ActionRun.STATE_FAILED
        assert self.collection._is_run_blocked(self.run_map['third_act'])

    def test_is_run_blocked_required_actions_scheduled(self):
        self.run_map['action_name'].machine.state = ActionRun.STATE_SCHEDULED
        assert self.collection._is_run_blocked(self.run_map['second_name'])

    def test_is_run_blocked_required_actions_starting(self):
        self.run_map['action_name'].machine.state = ActionRun.STATE_STARTING
        assert self.collection._is_run_blocked(self.run_map['second_name'])

    def test_is_run_blocked_required_actions_queued(self):
        self.run_map['action_name'].machine.state = ActionRun.STATE_QUEUED
        assert self.collection._is_run_blocked(self.run_map['second_name'])

    def test_is_run_blocked_required_actions_failed(self):
        self.run_map['action_name'].machine.state = ActionRun.STATE_FAILED
        assert self.collection._is_run_blocked(self.run_map['second_name'])


if __name__ == "__main__":
    run()
