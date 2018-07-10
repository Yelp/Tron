from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import shutil
import tempfile

import mock
import six
from testify import assert_equal
from testify import run
from testify import setup
from testify import teardown
from testify import TestCase
from testify import turtle
from testify.assertions import assert_in
from testify.assertions import assert_raises

from tests import testingutils
from tests.assertions import assert_length
from tests.testingutils import autospec_method
from tests.testingutils import Turtle
from tron import actioncommand
from tron import node
from tron.core import action
from tron.core import actiongraph
from tron.core import jobrun
from tron.core.action import ExecutorTypes
from tron.core.actionrun import ActionCommand
from tron.core.actionrun import ActionRun
from tron.core.actionrun import ActionRunCollection
from tron.core.actionrun import ActionRunFactory
from tron.core.actionrun import MesosActionRun
from tron.core.actionrun import SSHActionRun
from tron.serialize import filehandler


class ActionRunFactoryTestCase(TestCase):
    @setup
    def setup_action_runs(self):
        self.run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        actions = [Turtle(name='act1'), Turtle(name='act2')]
        self.action_graph = actiongraph.ActionGraph(
            actions,
            {a.name: a
             for a in actions},
        )

        mock_node = mock.create_autospec(node.Node)
        self.job_run = jobrun.JobRun(
            'jobname',
            7,
            self.run_time,
            mock_node,
            action_graph=self.action_graph,
        )

        self.action_state_data = {
            'job_run_id': 'job_run_id',
            'action_name': 'act1',
            'state': 'succeeded',
            'run_time': 'the_run_time',
            'start_time': None,
            'end_time': None,
            'command': 'do action1',
            'node_name': 'anode',
        }
        self.action_runner = mock.create_autospec(
            actioncommand.SubprocessActionRunnerFactory,
        )

    def test_build_action_run_collection(self):
        collection = ActionRunFactory.build_action_run_collection(
            self.job_run,
            self.action_runner,
        )
        assert_equal(collection.action_graph, self.action_graph)
        assert_in('act1', collection.run_map)
        assert_in('act2', collection.run_map)
        assert_length(collection.run_map, 2)
        assert_equal(collection.run_map['act1'].action_name, 'act1')

    def test_action_run_collection_from_state(self):
        state_data = [self.action_state_data]
        cleanup_action_state_data = {
            'job_run_id': 'job_run_id',
            'action_name': 'cleanup',
            'state': 'succeeded',
            'run_time': self.run_time,
            'start_time': None,
            'end_time': None,
            'command': 'do cleanup',
            'node_name': 'anode',
            'action_runner': {
                'status_path': '/tmp/foo',
                'exec_path': '/bin/foo'
            }
        }
        collection = ActionRunFactory.action_run_collection_from_state(
            self.job_run,
            state_data,
            cleanup_action_state_data,
        )

        assert_equal(collection.action_graph, self.action_graph)
        assert_length(collection.run_map, 2)
        assert_equal(collection.run_map['act1'].action_name, 'act1')
        assert_equal(collection.run_map['cleanup'].action_name, 'cleanup')

    def test_build_run_for_action(self):
        action = Turtle(
            name='theaction',
            node_pool=None,
            is_cleanup=False,
            command="doit",
        )
        action_run = ActionRunFactory.build_run_for_action(
            self.job_run,
            action,
            self.action_runner,
        )

        assert_equal(action_run.job_run_id, self.job_run.id)
        assert_equal(action_run.node, self.job_run.node)
        assert_equal(action_run.action_name, action.name)
        assert not action_run.is_cleanup
        assert_equal(action_run.command, action.command)

    def test_build_run_for_action_with_node(self):
        action = Turtle(name='theaction', is_cleanup=True, command="doit")
        action_run = ActionRunFactory.build_run_for_action(
            self.job_run,
            action,
            self.action_runner,
        )

        assert_equal(action_run.job_run_id, self.job_run.id)
        assert_equal(action_run.node, action.node_pool.next.returns[0])
        assert action_run.is_cleanup
        assert_equal(action_run.action_name, action.name)
        assert_equal(action_run.command, action.command)

    def test_build_run_for_ssh_action(self):
        action = Turtle(
            name='theaction',
            command="doit",
            executor=ExecutorTypes.ssh,
        )
        action_run = ActionRunFactory.build_run_for_action(
            self.job_run,
            action,
            self.action_runner,
        )
        assert_equal(action_run.__class__, SSHActionRun)

    def test_build_run_for_mesos_action(self):
        action = Turtle(
            name='theaction',
            command="doit",
            executor=ExecutorTypes.mesos,
            cpus=10,
            mem=500,
            constraints=[['pool', 'LIKE', 'default']],
            docker_image='fake-docker.com:400/image',
            docker_parameters=[{
                'key': 'test',
                'value': 123
            }],
            env={'TESTING': 'true'},
            extra_volumes=[{
                'path': '/tmp'
            }],
            mesos_address='fake-mesos-master.com',
        )
        action_run = ActionRunFactory.build_run_for_action(
            self.job_run,
            action,
            self.action_runner,
        )
        assert_equal(action_run.__class__, MesosActionRun)
        assert_equal(action_run.cpus, action.cpus)
        assert_equal(action_run.mem, action.mem)
        assert_equal(action_run.constraints, action.constraints)
        assert_equal(action_run.docker_image, action.docker_image)
        assert_equal(action_run.docker_parameters, action.docker_parameters)
        assert_equal(action_run.env, action.env)
        assert_equal(action_run.extra_volumes, action.extra_volumes)
        assert_equal(action_run.mesos_address, action.mesos_address)

    def test_action_run_from_state_default(self):
        state_data = self.action_state_data
        action_run = ActionRunFactory.action_run_from_state(
            self.job_run,
            state_data,
        )

        assert_equal(action_run.job_run_id, state_data['job_run_id'])
        assert not action_run.is_cleanup
        assert_equal(action_run.__class__, SSHActionRun)

    def test_action_run_from_state_mesos(self):
        state_data = self.action_state_data
        state_data['executor'] = ExecutorTypes.mesos
        state_data['cpus'] = 2
        state_data['mem'] = 200
        state_data['constraints'] = [['pool', 'LIKE', 'default']]
        state_data['docker_image'] = 'fake-docker.com:400/image'
        state_data['docker_parameters'] = [{'key': 'test', 'value': 123}]
        state_data['env'] = {'TESTING': 'true'}
        state_data['extra_volumes'] = [{'path': '/tmp'}]
        state_data['mesos_address'] = 'fake-mesos-master.com'
        action_run = ActionRunFactory.action_run_from_state(
            self.job_run,
            state_data,
        )

        assert_equal(action_run.job_run_id, state_data['job_run_id'])
        assert_equal(action_run.cpus, state_data['cpus'])
        assert_equal(action_run.mem, state_data['mem'])
        assert_equal(action_run.constraints, state_data['constraints'])
        assert_equal(action_run.docker_image, state_data['docker_image'])
        assert_equal(
            action_run.docker_parameters, state_data['docker_parameters']
        )
        assert_equal(action_run.env, state_data['env'])
        assert_equal(action_run.extra_volumes, state_data['extra_volumes'])
        assert_equal(action_run.mesos_address, state_data['mesos_address'])

        assert not action_run.is_cleanup
        assert_equal(action_run.__class__, MesosActionRun)


class ActionRunTestCase(TestCase):
    @setup
    def setup_action_run(self):
        self.output_path = filehandler.OutputPath(tempfile.mkdtemp())
        self.action_runner = actioncommand.NoActionRunnerFactory()
        self.command = "do command %(actionname)s"
        self.rendered_command = "do command action_name"
        self.action_run = ActionRun(
            "id",
            "action_name",
            mock.create_autospec(node.Node),
            self.command,
            output_path=self.output_path,
            action_runner=self.action_runner,
        )
        # These should be implemented in subclasses, we don't care here
        self.action_run.submit_command = mock.Mock()
        self.action_run.stop = mock.Mock()
        self.action_run.kill = mock.Mock()

    def test_init_state(self):
        assert_equal(self.action_run.state, ActionRun.STATE_SCHEDULED)

    def test_start(self):
        self.action_run.machine.transition('ready')
        assert self.action_run.start()
        assert_equal(self.action_run.submit_command.call_count, 1)
        assert self.action_run.is_starting
        assert self.action_run.start_time

    def test_start_bad_state(self):
        self.action_run.fail()
        assert not self.action_run.start()

    @mock.patch('tron.core.actionrun.log')
    def test_start_invalid_command(self, _log):
        self.action_run.bare_command = "%(notfound)s"
        self.action_run.machine.transition('ready')
        assert not self.action_run.start()
        assert self.action_run.is_failed
        assert_equal(self.action_run.exit_status, -1)

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

    @mock.patch('tron.core.actionrun.log')
    def test_command_failed_render(self, _log):
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
        assert_raises(
            AttributeError,
            self.action_run.__getattr__,
            'is_not_a_real_state',
        )


class SSHActionRunTestCase(TestCase):
    @setup
    def setup_action_run(self):
        self.output_path = filehandler.OutputPath(tempfile.mkdtemp())
        self.action_runner = mock.create_autospec(
            actioncommand.NoActionRunnerFactory,
        )
        self.command = "do command %(actionname)s"
        self.action_run = SSHActionRun(
            "id",
            "action_name",
            mock.create_autospec(node.Node),
            self.command,
            output_path=self.output_path,
            action_runner=self.action_runner,
        )

    @teardown
    def teardown_action_run(self):
        shutil.rmtree(self.output_path.base, ignore_errors=True)

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
            self.action_run.id,
            self.action_run.command,
            serializer,
        )
        mock_filehandler.OutputStreamSerializer.assert_called_with(
            self.action_run.output_path,
        )
        self.action_run.watch.assert_called_with(action_command)

    def test_auto_retry(self):
        self.action_run.retries_remaining = 2
        self.action_run.exit_statuses = []
        self.action_run.build_action_command()
        self.action_run.action_command.exit_status = -1
        self.action_run.machine.transition('start')

        self.action_run.fail(-1)
        assert self.action_run.retries_remaining == 1
        assert not self.action_run.is_failed

        self.action_run.fail(-1)
        assert self.action_run.retries_remaining == 0
        assert not self.action_run.is_failed

        self.action_run.fail(-1)
        assert self.action_run.retries_remaining == 0
        assert self.action_run.is_failed

        assert_equal(self.action_run.exit_statuses, [-1, -1])

    def test_manual_retry(self):
        self.action_run.retries_remaining = None
        self.action_run.exit_statuses = []
        self.action_run.build_action_command()
        self.action_run.action_command.exit_status = -1
        self.action_run.machine.transition('start')
        self.action_run.fail(-1)
        self.action_run.retry()
        self.action_run.is_running
        assert_equal(self.action_run.exit_statuses, [-1])
        assert_equal(self.action_run.retries_remaining, 0)

    def test_handler_running(self):
        self.action_run.build_action_command()
        self.action_run.machine.transition('start')
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.RUNNING,
        )
        assert self.action_run.is_running

    def test_handler_failstart(self):
        self.action_run.build_action_command()
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.FAILSTART,
        )
        assert self.action_run.is_failed

    def test_handler_exiting_fail(self):
        self.action_run.build_action_command()
        self.action_run.action_command.exit_status = -1
        self.action_run.machine.transition('start')
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.EXITING,
        )
        assert self.action_run.is_failed
        assert_equal(self.action_run.exit_status, -1)

    def test_handler_exiting_success(self):
        self.action_run.build_action_command()
        self.action_run.action_command.exit_status = 0
        self.action_run.machine.transition('start')
        self.action_run.machine.transition('started')
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.EXITING,
        )
        assert self.action_run.is_succeeded
        assert_equal(self.action_run.exit_status, 0)

    def test_handler_exiting_failunknown(self):
        self.action_run.action_command = mock.create_autospec(
            actioncommand.ActionCommand,
            exit_status=None,
        )
        self.action_run.machine.transition('start')
        self.action_run.machine.transition('started')
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.EXITING,
        )
        assert self.action_run.is_unknown
        assert_equal(self.action_run.exit_status, None)

    def test_handler_unhandled(self):
        self.action_run.build_action_command()
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.PENDING,
        ) is None
        assert self.action_run.is_scheduled


class ActionRunStateRestoreTestCase(testingutils.MockTimeTestCase):

    now = datetime.datetime(2012, 3, 14, 15, 19)

    @setup
    def setup_action_run(self):
        self.parent_context = {}
        self.output_path = ['one', 'two']
        self.state_data = {
            'job_run_id': 'theid',
            'action_name': 'theaction',
            'node_name': 'anode',
            'command': 'do things',
            'start_time': 'start_time',
            'end_time': 'end_time',
            'state': 'succeeded',
        }
        self.run_node = Turtle()

    def test_from_state(self):
        state_data = self.state_data
        action_run = ActionRun.from_state(
            state_data,
            self.parent_context,
            list(self.output_path),
            self.run_node,
        )

        for key, value in six.iteritems(self.state_data):
            if key in ['state', 'node_name']:
                continue
            assert_equal(getattr(action_run, key), value)

        assert action_run.is_succeeded
        assert not action_run.is_cleanup
        assert_equal(action_run.output_path[:2], self.output_path)

    def test_from_state_running(self):
        self.state_data['state'] = 'running'
        action_run = ActionRun.from_state(
            self.state_data,
            self.parent_context,
            self.output_path,
            self.run_node,
        )
        assert action_run.is_unknown
        assert_equal(action_run.exit_status, 0)
        assert_equal(action_run.end_time, self.now)

    def test_from_state_queued(self):
        self.state_data['state'] = 'queued'
        action_run = ActionRun.from_state(
            self.state_data,
            self.parent_context,
            self.output_path,
            self.run_node,
        )
        assert action_run.is_queued

    def test_from_state_no_node_name(self):
        del self.state_data['node_name']
        action_run = ActionRun.from_state(
            self.state_data,
            self.parent_context,
            self.output_path,
            self.run_node,
        )
        assert_equal(action_run.node, self.run_node)

    @mock.patch('tron.core.actionrun.node.NodePoolRepository')
    def test_from_state_with_node_exists(self, mock_store):
        ActionRun.from_state(
            self.state_data,
            self.parent_context,
            self.output_path,
            self.run_node,
        )
        mock_store.get_instance().get_node.assert_called_with(
            self.state_data['node_name'],
            self.run_node,
        )

    def test_from_state_before_rendered_command(self):
        self.state_data['command'] = 'do things %(actionname)s'
        self.state_data['rendered_command'] = None
        action_run = ActionRun.from_state(
            self.state_data,
            self.parent_context,
            self.output_path,
            self.run_node,
        )
        assert_equal(action_run.bare_command, self.state_data['command'])
        assert not action_run.rendered_command

    def test_from_state_old_state(self):
        self.state_data['command'] = 'do things %(actionname)s'
        action_run = ActionRun.from_state(
            self.state_data,
            self.parent_context,
            self.output_path,
            self.run_node,
        )
        assert_equal(action_run.bare_command, self.state_data['command'])
        assert not action_run.rendered_command

    def test_from_state_after_rendered_command(self):
        self.state_data['command'] = 'do things theaction'
        self.state_data['rendered_command'] = self.state_data['command']
        action_run = ActionRun.from_state(
            self.state_data,
            self.parent_context,
            self.output_path,
            self.run_node,
        )
        assert_equal(action_run.bare_command, self.state_data['command'])
        assert_equal(action_run.rendered_command, self.state_data['command'])


class ActionRunCollectionTestCase(TestCase):
    def _build_run(self, name):
        mock_node = mock.create_autospec(node.Node)
        return ActionRun(
            "id",
            name,
            mock_node,
            self.command,
            output_path=self.output_path,
        )

    @setup
    def setup_runs(self):
        action_names = ['action_name', 'second_name', 'cleanup']
        self.action_graph = actiongraph.ActionGraph.from_config(
            action.ActionMap.from_config([
                dict(name=name, command='test') for name in action_names
            ])
        )
        self.output_path = filehandler.OutputPath(tempfile.mkdtemp())
        self.command = "do command"
        self.action_runs = [self._build_run(name) for name in action_names]
        self.run_map = {a.action_name: a for a in self.action_runs}
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
        actions = ['action_name']
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

        def blocked_second_action_run(ar, ):
            return ar == self.run_map['second_name']

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
            "cleanup(scheduled)",
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
        return ActionRun(
            "id",
            name,
            mock_node,
            self.command,
            output_path=self.output_path,
        )

    @setup
    def setup_collection(self):
        action_names = ['action_name', 'second_name', 'cleanup']
        self.action_graph = actiongraph.ActionGraph.from_config(
            action.ActionMap.from_config([
                {
                    'name': 'action_name',
                    'command': 'test'
                },
                {
                    'name': 'second_name',
                    'command': 'test',
                    'requires': ['action_name'],
                },
                {
                    'name': 'cleanup',
                    'command': 'test'
                },
            ])
        )

        self.output_path = filehandler.OutputPath(tempfile.mkdtemp())
        self.command = "do command"
        self.action_runs = [self._build_run(name) for name in action_names]
        self.run_map = {a.action_name: a for a in self.action_runs}
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
        third_act = action.Action(
            name='third_act',
            command='test',
            required_actions=['second_name'],
            node_pool='testpool',
        )
        self.action_graph.action_map = self.action_graph.action_map.update({
            'third_act': third_act
        }, )
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

    def test_is_run_blocked_required_actions_missing(self):
        del self.run_map['action_name']
        assert not self.collection._is_run_blocked(self.run_map['second_name'])


class MesosActionRunTestCase(TestCase):
    @setup
    def setup_action_run(self):
        self.output_path = mock.MagicMock()
        self.command = "do the command"
        self.mesos_address = 'mesos-master.com'
        self.other_task_kwargs = {
            'cpus': 1,
            'mem': 50,
            'docker_image': 'container:v2',
            'constraints': [],
            'env': {
                'TESTING': 'true'
            },
            'docker_parameters': [],
            'extra_volumes': [],
        }
        self.action_run = MesosActionRun(
            "job_run_id",
            "action_name",
            mock.create_autospec(node.Node),
            rendered_command=self.command,
            output_path=self.output_path,
            executor=ExecutorTypes.mesos,
            mesos_address=self.mesos_address,
            **self.other_task_kwargs
        )

    @mock.patch('tron.core.actionrun.filehandler', autospec=True)
    @mock.patch('tron.core.actionrun.MesosClusterRepository', autospec=True)
    def test_submit_command(self, mock_cluster_repo, mock_filehandler):
        serializer = mock_filehandler.OutputStreamSerializer.return_value
        with mock.patch.object(
            self.action_run,
            'watch',
            autospec=True,
        ) as mock_watch:
            self.action_run.submit_command()

            mock_get_cluster = mock_cluster_repo.get_cluster
            mock_get_cluster.assert_called_once_with(self.mesos_address)
            mock_get_cluster.return_value.create_task.assert_called_once_with(
                action_run_id=self.action_run.id,
                command=self.command,
                serializer=serializer,
                **self.other_task_kwargs
            )
            task = mock_get_cluster.return_value.create_task.return_value
            mock_get_cluster.return_value.submit.assert_called_once_with(task)
            mock_watch.assert_called_once_with(task)

        mock_filehandler.OutputStreamSerializer.assert_called_with(
            self.action_run.output_path,
        )

    @mock.patch('tron.core.actionrun.filehandler', autospec=True)
    @mock.patch('tron.core.actionrun.MesosClusterRepository', autospec=True)
    def test_submit_command_task_none(
        self, mock_cluster_repo, mock_filehandler
    ):
        # Task is None if Mesos is disabled
        mock_cluster_repo.get_cluster.return_value.create_task.return_value = None
        self.action_run.submit_command()

        mock_get_cluster = mock_cluster_repo.get_cluster
        mock_get_cluster.assert_called_once_with(self.mesos_address)
        assert self.action_run.is_failed

    @mock.patch('tron.core.actionrun.MesosClusterRepository', autospec=True)
    def test_kill_task(self, mock_cluster_repo):
        mock_get_cluster = mock_cluster_repo.get_cluster
        self.action_run.task_id = 'fake_task_id'
        error_message = self.action_run.kill()
        mock_get_cluster.return_value.kill.assert_called_once_with(
            self.action_run.task_id
        )
        assert_equal(
            error_message,
            "Warning: It might take up to docker_stop_timeout (current setting is 2 mins) for killing."
        )

    @mock.patch('tron.core.actionrun.MesosClusterRepository', autospec=True)
    def test_kill_task_not_running(self, mock_cluster_repo):
        error_message = self.action_run.kill()
        assert_equal(
            error_message, "Error: Can't find task id for the action."
        )

    @mock.patch('tron.core.actionrun.MesosClusterRepository', autospec=True)
    def test_stop_task(self, mock_cluster_repo):
        mock_get_cluster = mock_cluster_repo.get_cluster
        self.action_run.task_id = 'fake_task_id'
        self.action_run.stop()
        mock_get_cluster.return_value.kill.assert_called_once_with(
            self.action_run.task_id
        )

    @mock.patch('tron.core.actionrun.MesosClusterRepository', autospec=True)
    def test_stop_task_not_running(self, mock_cluster_repo):
        error_message = self.action_run.stop()
        assert_equal(
            error_message, "Error: Can't find task id for the action."
        )


if __name__ == "__main__":
    run()
