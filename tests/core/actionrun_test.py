import datetime
import shutil
import tempfile
from unittest import mock
from unittest.mock import MagicMock

import pytest

from tests.assertions import assert_length
from tests.testingutils import autospec_method
from tron import actioncommand
from tron import node
from tron.actioncommand import SubprocessActionRunnerFactory
from tron.config.schema import ConfigConstraint
from tron.config.schema import ConfigParameter
from tron.config.schema import ConfigVolume
from tron.config.schema import ExecutorTypes
from tron.core import actiongraph
from tron.core import jobrun
from tron.core.action import ActionCommandConfig
from tron.core.actionrun import ActionCommand
from tron.core.actionrun import ActionRun
from tron.core.actionrun import ActionRunAttempt
from tron.core.actionrun import ActionRunCollection
from tron.core.actionrun import ActionRunFactory
from tron.core.actionrun import eager_all
from tron.core.actionrun import INITIAL_RECOVER_DELAY
from tron.core.actionrun import KubernetesActionRun
from tron.core.actionrun import MAX_RECOVER_TRIES
from tron.core.actionrun import MesosActionRun
from tron.core.actionrun import min_filter
from tron.core.actionrun import SSHActionRun
from tron.serialize import filehandler


@pytest.fixture
def output_path():
    output_path = filehandler.OutputPath(tempfile.mkdtemp())
    yield output_path
    shutil.rmtree(output_path.base, ignore_errors=True)


@pytest.fixture
def mock_current_time():
    with mock.patch("tron.core.actionrun.timeutils.current_time", autospec=True,) as mock_current_time:
        yield mock_current_time


class TestMinFilter:
    def test_min_filter(self):
        seq = [None, 2, None, 7, None, 9, 10, 12, 1]
        assert min_filter(seq) == 1


class TestEagerAll:
    def test_all_true(self):
        assert eager_all(range(1, 5))

    def test_all_false(self):
        assert not eager_all(0 for _ in range(7))

    def test_full_iteration(self):
        seq = iter([1, 0, 3, 0, 5])
        assert not eager_all(seq)
        with pytest.raises(StopIteration):
            next(seq)


class TestActionRunFactory:
    @pytest.fixture(autouse=True)
    def setup_action_runs(self):
        self.run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        a1 = MagicMock()
        a1.name = "act1"
        a1.command_config = ActionCommandConfig(command="do action1")
        a2 = MagicMock()
        a2.name = "act2"
        actions = [a1, a2]
        self.action_graph = actiongraph.ActionGraph(
            {a.name: a for a in actions}, {"act1": set(), "act2": set()}, {"act1": set(), "act2": set()},
        )

        mock_node = mock.create_autospec(node.Node)
        self.job_run = jobrun.JobRun("jobname", 7, self.run_time, mock_node, action_graph=self.action_graph,)

        self.action_runner = mock.create_autospec(actioncommand.SubprocessActionRunnerFactory,)

    @pytest.fixture
    def state_data(self):
        command_config = self.action_graph.action_map["act1"].command_config.state_data
        # State data with command config and retries.
        yield {
            "job_run_id": "job_run_id",
            "action_name": "act1",
            "state": "succeeded",
            "run_time": "the_run_time",
            "start_time": None,
            "end_time": None,
            "attempts": [dict(command_config=command_config, start_time="start")],
            "node_name": "anode",
        }

    def test_build_action_run_collection(self):
        collection = ActionRunFactory.build_action_run_collection(self.job_run, self.action_runner,)
        assert collection.action_graph == self.action_graph
        assert "act1" in collection.run_map
        assert "act2" in collection.run_map
        assert len(collection.run_map) == 2
        assert collection.run_map["act1"].action_name == "act1"

    def test_action_run_collection_from_state(self, state_data):
        state_data = [state_data]
        cleanup_command_config = dict(command="do action1")
        cleanup_action_state_data = {
            "job_run_id": "job_run_id",
            "action_name": "cleanup",
            "state": "succeeded",
            "run_time": self.run_time,
            "start_time": None,
            "end_time": None,
            "attempts": [
                dict(
                    command_config=cleanup_command_config,
                    rendered_command="do action1",
                    start_time="start",
                    end_time="end",
                    exit_status=0,
                ),
            ],
            "node_name": "anode",
            "action_runner": {"status_path": "/tmp/foo", "exec_path": "/bin/foo",},
        }
        collection = ActionRunFactory.action_run_collection_from_state(
            self.job_run, state_data, cleanup_action_state_data,
        )

        assert collection.action_graph == self.action_graph
        assert_length(collection.run_map, 2)
        assert collection.run_map["act1"].action_name == "act1"
        assert collection.run_map["cleanup"].action_name == "cleanup"

    def test_build_run_for_action(self):
        expected_command = "doit"
        action = MagicMock(
            node_pool=None, is_cleanup=False, command_config=ActionCommandConfig(command=expected_command),
        )
        action.name = "theaction"
        action_run = ActionRunFactory.build_run_for_action(self.job_run, action, self.action_runner,)

        assert action_run.job_run_id == self.job_run.id
        assert action_run.node == self.job_run.node
        assert action_run.action_name == action.name
        assert not action_run.is_cleanup
        assert action_run.command == expected_command

    def test_build_run_for_action_with_node(self):
        expected_command = "doit"
        action = MagicMock(
            node_pool=None, is_cleanup=True, command_config=ActionCommandConfig(command=expected_command),
        )
        action.node_pool = mock.create_autospec(node.NodePool)
        action_run = ActionRunFactory.build_run_for_action(self.job_run, action, self.action_runner,)

        assert action_run.job_run_id == self.job_run.id
        assert action_run.node == action.node_pool.next()
        assert action_run.is_cleanup
        assert action_run.action_name == action.name
        assert action_run.command == expected_command

    def test_build_run_for_ssh_action(self):
        action = MagicMock(name="theaction", command="doit", executor=ExecutorTypes.ssh.value,)
        action_run = ActionRunFactory.build_run_for_action(self.job_run, action, self.action_runner,)
        assert action_run.__class__ == SSHActionRun

    def test_build_run_for_mesos_action(self):
        command_config = MagicMock(
            cpus=10,
            mem=500,
            disk=600,
            constraints=[["pool", "LIKE", "default"]],
            docker_image="fake-docker.com:400/image",
            docker_parameters=[{"key": "test", "value": 123,}],
            env={"TESTING": "true"},
            extra_volumes=[{"path": "/tmp",}],
        )
        action = MagicMock(
            name="theaction", command="doit", executor=ExecutorTypes.mesos.value, command_config=command_config,
        )
        action_run = ActionRunFactory.build_run_for_action(self.job_run, action, self.action_runner,)
        assert action_run.__class__ == MesosActionRun
        assert action_run.command_config.cpus == command_config.cpus
        assert action_run.command_config.mem == command_config.mem
        assert action_run.command_config.disk == command_config.disk
        assert action_run.command_config.constraints == command_config.constraints
        assert action_run.command_config.docker_image == command_config.docker_image
        assert action_run.command_config.docker_parameters == command_config.docker_parameters
        assert action_run.command_config.env == command_config.env
        assert action_run.command_config.extra_volumes == command_config.extra_volumes

    def test_action_run_from_state_ssh(self, state_data):
        action_run = ActionRunFactory.action_run_from_state(self.job_run, state_data,)

        assert action_run.job_run_id == state_data["job_run_id"]
        assert not action_run.is_cleanup
        assert action_run.__class__ == SSHActionRun

    def test_action_run_from_state_mesos(self, state_data):
        state_data["executor"] = ExecutorTypes.mesos.value
        action_run = ActionRunFactory.action_run_from_state(self.job_run, state_data,)

        assert action_run.job_run_id == state_data["job_run_id"]
        action_name = state_data["action_name"]
        assert action_run.command_config == self.action_graph.action_map[action_name].command_config

        assert not action_run.is_cleanup
        assert action_run.__class__ == MesosActionRun

    def test_action_run_from_state_kubernetes(self, state_data):
        state_data["executor"] = ExecutorTypes.kubernetes.value
        action_run = ActionRunFactory.action_run_from_state(self.job_run, state_data,)

        assert action_run.job_run_id == state_data["job_run_id"]
        action_name = state_data["action_name"]
        assert action_run.command_config == self.action_graph.action_map[action_name].command_config

        assert not action_run.is_cleanup
        assert action_run.__class__ == KubernetesActionRun

    def test_action_run_from_state_spark(self, state_data):
        state_data["executor"] = ExecutorTypes.spark.value
        action_run = ActionRunFactory.action_run_from_state(self.job_run, state_data,)

        assert action_run.job_run_id == state_data["job_run_id"]
        action_name = state_data["action_name"]
        assert action_run.command_config == self.action_graph.action_map[action_name].command_config

        assert not action_run.is_cleanup
        assert action_run.__class__ == KubernetesActionRun


class TestActionRun:
    @pytest.fixture(autouse=True)
    def setup_action_run(self, output_path):
        self.action_runner = actioncommand.NoActionRunnerFactory()
        self.command = "do command {actionname}"
        self.rendered_command = "do command action_name"
        self.action_run = ActionRun(
            job_run_id="ns.id.0",
            name="action_name",
            node=mock.create_autospec(node.Node),
            command_config=ActionCommandConfig(command=self.command),
            output_path=output_path,
            action_runner=self.action_runner,
        )
        # These should be implemented in subclasses, we don't care here
        self.action_run.submit_command = mock.Mock()
        self.action_run.stop = mock.Mock()
        self.action_run.kill = mock.Mock()

    def test_init_state(self):
        assert self.action_run.state == ActionRun.SCHEDULED

    def test_ready_state(self):
        self.action_run.ready()
        assert self.action_run.state == ActionRun.WAITING

    def test_start(self):
        self.action_run.machine.transition("ready")
        assert self.action_run.start()
        assert self.action_run.submit_command.call_count == 1
        assert self.action_run.is_starting
        assert self.action_run.start_time

    def test_start_bad_state(self):
        self.action_run.fail()
        assert not self.action_run.start()

    @mock.patch("tron.core.actionrun.log", autospec=True)
    def test_start_invalid_command(self, _log):
        self.action_run.original_command = "{notfound}"
        self.action_run.machine.transition("ready")
        assert not self.action_run.start()
        assert self.action_run.is_failed
        assert self.action_run.exit_status == -1

    def test_success(self):
        assert self.action_run.ready()
        self.action_run.machine.transition("start")
        self.action_run.machine.transition("started")

        assert self.action_run.is_running
        assert self.action_run.success()
        assert not self.action_run.is_running
        assert self.action_run.is_done
        assert self.action_run.end_time
        assert self.action_run.exit_status == 0

    def test_success_emits_not(self):
        self.action_run.machine.transition("start")
        self.action_run.machine.transition("started")
        self.action_run.trigger_downstreams = None
        self.action_run.emit_triggers = mock.Mock()
        assert self.action_run.success()
        assert self.action_run.emit_triggers.call_count == 0

    def test_sucess_emits_not_invalid_transition(self):
        self.action_run.trigger_downstreams = True
        self.action_run.machine.check = mock.Mock(return_value=False)
        self.action_run.emit_triggers = mock.Mock()

        assert not self.action_run.success()
        assert self.action_run.emit_triggers.call_count == 0

    def test_success_emits_on_true(self):
        self.action_run.machine.transition("start")
        self.action_run.machine.transition("started")
        self.action_run.trigger_downstreams = True
        self.action_run.emit_triggers = mock.Mock()
        assert self.action_run.success()
        assert self.action_run.emit_triggers.call_count == 1

    def test_success_emits_on_dict(self):
        self.action_run.machine.transition("start")
        self.action_run.machine.transition("started")
        self.action_run.trigger_downstreams = dict(foo="bar")
        self.action_run.emit_triggers = mock.Mock()
        assert self.action_run.success()
        assert self.action_run.emit_triggers.call_count == 1

    @mock.patch("tron.core.actionrun.EventBus", autospec=True)
    def test_emit_triggers(self, eventbus):
        self.action_run.context = {"shortdate": "foo"}

        self.action_run.trigger_downstreams = True
        self.action_run.emit_triggers()

        self.action_run.trigger_downstreams = dict(foo="bar")
        self.action_run.emit_triggers()

        assert eventbus.publish.mock_calls == [
            mock.call("ns.id.action_name.shortdate.foo"),
            mock.call("ns.id.action_name.foo.bar"),
        ]

    def test_success_bad_state(self):
        self.action_run.cancel()
        assert not self.action_run.success()

    def test_failure(self):
        self.action_run._exit_unsuccessful(1)
        assert not self.action_run.is_running
        assert self.action_run.is_done
        assert self.action_run.end_time
        assert self.action_run.exit_status == 1

    def test_failure_bad_state(self):
        self.action_run.fail(444)
        assert not self.action_run.fail(123)
        assert self.action_run.exit_status == 444

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
        self.action_run.context = {"stars": "bright"}
        bare_command = "{stars}"
        assert self.action_run.render_command(bare_command) == "bright"

    def test_command_not_yet_rendered(self):
        assert self.action_run.command == self.action_run.command_config.command

    def test_command_already_rendered(self):
        last_attempt = self.action_run.create_attempt()
        assert self.action_run.command == last_attempt.rendered_command

    @mock.patch("tron.core.actionrun.log", autospec=True)
    def test_command_failed_render(self, _log):
        bare_command = "{this_is_missing}"
        assert self.action_run.render_command(bare_command) == ActionRun.FAILED_RENDER

    def test_is_complete(self):
        self.action_run.machine.state = ActionRun.SUCCEEDED
        assert self.action_run.is_complete
        self.action_run.machine.state = ActionRun.SKIPPED
        assert self.action_run.is_complete
        self.action_run.machine.state = ActionRun.RUNNING
        assert not self.action_run.is_complete

    def test_is_broken(self):
        self.action_run.machine.state = ActionRun.UNKNOWN
        assert self.action_run.is_broken
        self.action_run.machine.state = ActionRun.FAILED
        assert self.action_run.is_broken
        self.action_run.machine.state = ActionRun.WAITING
        assert not self.action_run.is_broken

    def test__getattr__(self):
        assert not self.action_run.is_succeeded
        assert not self.action_run.is_failed
        assert not self.action_run.is_queued
        assert self.action_run.is_scheduled
        assert self.action_run.cancel()
        assert self.action_run.is_cancelled

    def test__getattr__missing_attribute(self):
        with pytest.raises(AttributeError):
            self.action_run.__getattr__("is_not_a_real_state")

    def test_auto_retry(self, mock_current_time):
        # One timestamp for start and end of each attempt, plus final end time
        mock_current_time.side_effect = [1, 2, 3, 4, 5, 6, 7]
        self.action_run.retries_remaining = 2
        self.action_run.create_attempt()
        self.action_run.machine.transition("start")

        assert self.action_run._exit_unsuccessful(-1)
        assert self.action_run.is_starting
        assert self.action_run.retries_remaining == 1

        assert self.action_run._exit_unsuccessful(-1)
        assert self.action_run.retries_remaining == 0
        assert not self.action_run.is_failed

        assert self.action_run._exit_unsuccessful(-2)
        assert self.action_run.retries_remaining == 0
        assert self.action_run.is_failed

        assert self.action_run.exit_statuses == [-1, -1, -2]
        assert len(self.action_run.attempts) == 3
        for i, attempt in enumerate(self.action_run.attempts):
            assert attempt.start_time == i * 2 + 1
            assert attempt.end_time == (i + 1) * 2

    def test_auto_retry_command_config_change(self, mock_current_time):
        self.action_run.retries_remaining = 1
        self.action_run.create_attempt()
        self.action_run.machine.transition("start")

        # If the command_config gets reconfigured later, auto retry
        # still uses the original command by default.
        self.action_run.command_config = ActionCommandConfig(command="new")

        assert self.action_run._exit_unsuccessful(-1)
        assert self.action_run._exit_unsuccessful(-1)
        assert len(self.action_run.attempts) == 2

        for i, attempt in enumerate(self.action_run.attempts):
            assert attempt.rendered_command == self.rendered_command

    def test_no_auto_retry_on_fail_not_running(self):
        self.action_run.retries_remaining = 2

        self.action_run.fail()
        assert self.action_run.retries_remaining == -1
        assert self.action_run.is_failed

        assert self.action_run.exit_statuses == []
        assert self.action_run.exit_status is None

    def test_no_auto_retry_on_fail_running(self):
        self.action_run.retries_remaining = 2
        self.action_run.create_attempt()
        self.action_run.machine.transition("start")

        self.action_run.fail()
        assert self.action_run.retries_remaining == -1
        assert self.action_run.is_failed

        assert self.action_run.exit_statuses == [None]
        assert self.action_run.exit_status is None

    def test_auto_retry_already_done(self):
        # If someone transitions the action before it
        # is done to success/fail, the action
        # should not automatically retry when the command
        # completes.
        self.action_run.retries_remaining = 2
        self.action_run.create_attempt()
        self.action_run.machine.transition("start")
        self.action_run.machine.transition("started")

        # Action gets manually transitioned to success with tronctl
        self.action_run.machine.transition("success")
        assert self.action_run.is_succeeded

        # Command later fails
        # Does not start a command
        assert not self.action_run._exit_unsuccessful(-1)
        # Still succeeded, not starting
        assert self.action_run.is_succeeded

    def test_manual_retry(self, mock_current_time):
        mock_current_time.side_effect = [1, 2, 3, 4]
        self.action_run.retries_remaining = None
        failed_attempt = self.action_run.create_attempt()
        self.action_run.machine.transition("start")
        self.action_run.fail(-1)
        assert failed_attempt.end_time == 2
        assert failed_attempt.exit_status == -1

        self.action_run.retry()
        assert self.action_run.is_starting
        assert self.action_run.exit_statuses == [-1]
        assert self.action_run.retries_remaining == 0
        # Last attempt should be unchanged
        assert failed_attempt.end_time == 2
        assert failed_attempt.exit_status == -1

    def test_manual_retry_use_new_command(self, mock_current_time):
        mock_current_time.side_effect = [1, 2, 3, 4]
        self.action_run.retries_remaining = None
        self.action_run.create_attempt()
        self.action_run.machine.transition("start")
        self.action_run.fail(-1)

        # Change the command config
        self.action_run.command_config = ActionCommandConfig(command="new")
        self.action_run.retry(original_command=False)
        assert self.action_run.is_starting
        assert self.action_run.last_attempt.rendered_command == "new"

    @mock.patch("twisted.internet.reactor.callLater", autospec=True)
    def test_retries_delay(self, callLater):
        self.action_run.retries_delay = datetime.timedelta()
        self.action_run.retries_remaining = 2
        self.action_run.machine.transition("start")
        callLater.return_value = "delayed call"
        assert self.action_run._exit_unsuccessful(-1)
        assert self.action_run.in_delay == "delayed call"


class TestActionRunFactoryTriggerTimeout:
    def test_trigger_timeout_default(self):
        today = datetime.datetime.today()
        day = datetime.timedelta(days=1)
        tomorrow = today + day
        action_run = ActionRunFactory.build_run_for_action(
            mock.Mock(run_time=today), mock.Mock(trigger_timeout=None), mock.Mock(),
        )
        assert action_run.trigger_timeout_timestamp == tomorrow.timestamp()

    def test_trigger_timeout_custom(self):
        today = datetime.datetime.today()
        hour = datetime.timedelta(hours=1)
        target = today + hour
        action_run = ActionRunFactory.build_run_for_action(
            mock.Mock(run_time=today), mock.Mock(trigger_timeout=hour), mock.Mock(),
        )
        assert action_run.trigger_timeout_timestamp == target.timestamp()


class TestActionRunTriggerTimeout:
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        self.command = mock.Mock()
        self.rendered_command = "do command action_name"
        self.action_run = ActionRun(
            job_run_id="ns.id.0",
            name="action_name",
            command_config=ActionCommandConfig(command=self.command),
            triggered_by=["hello"],
            node=mock.Mock(),
            output_path=mock.Mock(),
            action_runner=mock.Mock(),
            trigger_timeout_timestamp=mock.Mock(),
        )
        self.action_run.submit_command = mock.Mock()
        self.action_run.stop = mock.Mock()
        self.action_run.kill = mock.Mock()

    def test_cleanup_clears_trigger_timeout(self):
        self.action_run.clear_trigger_timeout = MagicMock()
        self.action_run.cleanup()
        self.action_run.clear_trigger_timeout.assert_called_with()

    def test_clear_trigger_timeout(self):
        timeout_call = MagicMock()
        self.action_run.trigger_timeout_call = timeout_call
        self.action_run.clear_trigger_timeout()
        assert self.action_run.trigger_timeout_call is None
        timeout_call.cancel.assert_called_with()

    @mock.patch("tron.core.actionrun.EventBus", autospec=True)
    @mock.patch("tron.core.actionrun.reactor", autospec=True)
    def test_setup_subscriptions_no_triggers(self, reactor, eventbus):
        self.action_run.triggered_by = []
        self.action_run.setup_subscriptions()
        assert not reactor.callLater.called
        assert not eventbus.subscribe.called

    @mock.patch("tron.core.actionrun.EventBus", autospec=True)
    @mock.patch("tron.core.actionrun.reactor", autospec=True)
    def test_setup_subscriptions_no_remaining(self, reactor, eventbus):
        self.action_run.triggered_by = ["hello"]
        self.action_run.trigger_timeout_timestamp = None
        eventbus.has_event.return_value = True
        self.action_run.setup_subscriptions()
        assert not reactor.callLater.called
        assert not eventbus.subscribe.called
        assert eventbus.has_event.call_args_list == [mock.call("hello")]

    @mock.patch("tron.core.actionrun.reactor", autospec=True)
    def test_setup_subscriptions_timeout_in_future(self, reactor, mock_current_time):
        now = datetime.datetime.now()
        mock_current_time.return_value = now
        self.action_run.trigger_timeout_timestamp = now.timestamp() + 10
        self.action_run.setup_subscriptions()
        reactor.callLater.assert_called_once_with(
            10.0, self.action_run.trigger_timeout_reached,
        )

    @mock.patch("tron.core.actionrun.reactor", autospec=True)
    def test_setup_subscriptions_timeout_in_past(self, reactor, mock_current_time):
        now = datetime.datetime.now()
        mock_current_time.return_value = now
        self.action_run.trigger_timeout_timestamp = now.timestamp() - 10
        self.action_run.setup_subscriptions()
        reactor.callLater.assert_called_once_with(
            1, self.action_run.trigger_timeout_reached,
        )

    @mock.patch("tron.core.actionrun.EventBus", autospec=True)
    def test_trigger_timeout_reached_no_remaining_notifies(self, eventbus):
        self.action_run.notify = MagicMock()
        self.action_run.triggered_by = ["hello"]
        eventbus.has_event.return_value = True
        self.action_run.trigger_timeout_reached()
        assert self.action_run.notify.called

    @mock.patch("tron.core.actionrun.EventBus", autospec=True)
    def test_trigger_timeout_reached_with_remaining_fails(self, eventbus):
        self.action_run.fail = MagicMock()
        self.action_run.triggered_by = ["hello"]
        eventbus.has_event.return_value = False
        self.action_run.trigger_timeout_reached()
        assert self.action_run.fail.called

    def test_done_clears_trigger_timeout_call(self):
        self.action_run.machine.check = mock.Mock(return_value=True)
        self.action_run.transition_and_notify = MagicMock()
        self.action_run.triggered_by = []
        self.action_run.clear_trigger_timeout = MagicMock()
        self.action_run._done(ActionRun.SUCCEEDED)
        assert self.action_run.clear_trigger_timeout.called

    def test_trigger_notify_clears_trigger_timeout(self):
        self.action_run.notify = MagicMock()
        self.action_run.triggered_by = []
        self.action_run.clear_trigger_timeout = MagicMock()
        self.action_run.trigger_notify()
        assert self.action_run.clear_trigger_timeout.called


class TestSSHActionRun:
    @pytest.fixture(autouse=True)
    def setup_action_run(self, output_path):
        self.action_runner = mock.create_autospec(actioncommand.NoActionRunnerFactory,)
        self.command = "do command {actionname}"
        self.action_run = SSHActionRun(
            job_run_id="job_name.5",
            name="action_name",
            command_config=ActionCommandConfig(command=self.command),
            node=mock.create_autospec(node.Node),
            output_path=output_path,
            action_runner=self.action_runner,
        )

    def test_start_node_error(self):
        def raise_error(c):
            raise node.Error("The error")

        self.action_run.node = mock.MagicMock()
        self.action_run.node.submit_command.side_effect = raise_error
        self.action_run.machine.transition("ready")
        assert not self.action_run.start()
        assert self.action_run.exit_status == -2
        assert self.action_run.is_failed

    @mock.patch("tron.core.actionrun.filehandler", autospec=True)
    def test_build_action_command(self, mock_filehandler):
        self.action_run.watch = mock.MagicMock()
        attempt = self.action_run.create_attempt()
        serializer = mock_filehandler.OutputStreamSerializer.return_value
        action_command = self.action_run.build_action_command(attempt)
        assert action_command == self.action_run.action_command
        assert action_command == self.action_runner.create.return_value
        self.action_runner.create.assert_called_with(
            self.action_run.id, attempt.rendered_command, serializer,
        )
        mock_filehandler.OutputStreamSerializer.assert_called_with(self.action_run.output_path,)
        self.action_run.watch.assert_called_with(action_command)

    def test_handler_running(self):
        attempt = self.action_run.create_attempt()
        self.action_run.build_action_command(attempt)
        self.action_run.machine.transition("start")
        assert self.action_run.handler(self.action_run.action_command, ActionCommand.RUNNING,)
        assert self.action_run.is_running

    def test_handler_failstart(self):
        attempt = self.action_run.create_attempt()
        self.action_run.build_action_command(attempt)
        assert self.action_run.handler(self.action_run.action_command, ActionCommand.FAILSTART,)
        assert self.action_run.is_failed

    def test_handler_exiting_fail(self):
        attempt = self.action_run.create_attempt()
        self.action_run.build_action_command(attempt)
        self.action_run.action_command.exit_status = -1
        self.action_run.machine.transition("start")
        assert self.action_run.handler(self.action_run.action_command, ActionCommand.EXITING,)
        assert self.action_run.is_failed
        assert self.action_run.exit_status == -1

    def test_handler_exiting_success(self):
        attempt = self.action_run.create_attempt()
        self.action_run.build_action_command(attempt)
        self.action_run.action_command.exit_status = 0
        self.action_run.machine.transition("start")
        self.action_run.machine.transition("started")
        assert self.action_run.handler(self.action_run.action_command, ActionCommand.EXITING,)
        assert self.action_run.is_succeeded
        assert self.action_run.exit_status == 0

    def test_handler_exiting_failunknown(self):
        self.action_run.action_command = mock.create_autospec(actioncommand.ActionCommand, exit_status=None,)
        self.action_run.machine.transition("start")
        self.action_run.machine.transition("started")
        assert self.action_run.handler(self.action_run.action_command, ActionCommand.EXITING,)
        assert self.action_run.is_unknown
        assert self.action_run.exit_status is None
        assert self.action_run.end_time is not None

    def test_handler_unhandled(self):
        attempt = self.action_run.create_attempt()
        self.action_run.build_action_command(attempt)
        assert self.action_run.handler(self.action_run.action_command, ActionCommand.PENDING,) is None
        assert self.action_run.is_scheduled

    def test_recover_no_action_runner(self):
        # Default setup has no action runner
        assert not self.action_run.recover()


class TestSSHActionRunRecover:
    @pytest.fixture(autouse=True)
    def setup_action_run(self, output_path):
        self.action_runner = SubprocessActionRunnerFactory(status_path="/tmp/foo", exec_path="/bin/foo",)
        self.command = "do command {actionname}"
        self.action_run = SSHActionRun(
            job_run_id="job_name.5",
            name="action_name",
            command_config=ActionCommandConfig(self.command),
            node=mock.create_autospec(node.Node),
            output_path=output_path,
            action_runner=self.action_runner,
        )

    def test_recover_incorrect_state(self):
        # Should return falsy if not UNKNOWN.
        self.action_run.machine.state = ActionRun.FAILED
        assert not self.action_run.recover()

    def test_recover_action_runner(self):
        self.action_run.end_time = 1000
        self.action_run.exit_status = 0
        self.action_run.machine.state = ActionRun.UNKNOWN
        last_attempt = self.action_run.create_attempt()
        last_attempt.end_time = 1000
        last_attempt.exit_status = 0
        assert self.action_run.recover()
        assert self.action_run.machine.state == ActionRun.RUNNING
        assert self.action_run.end_time is None
        assert self.action_run.exit_status is None
        assert last_attempt.end_time is None
        assert last_attempt.exit_status is None
        self.action_run.node.submit_command.assert_called_once()

        # Check recovery command
        submit_args = self.action_run.node.submit_command.call_args[0]
        assert len(submit_args) == 1
        recovery_command = submit_args[0]
        assert recovery_command.command == "/bin/foo/recover_batch.py /tmp/foo/job_name.5.action_name/status"
        assert recovery_command.start_time is not None  # already started

    @mock.patch("tron.core.actionrun.reactor", autospec=True)
    def test_handler_exiting_failunknown(self, mock_reactor):
        self.action_run.action_command = mock.create_autospec(actioncommand.ActionCommand, exit_status=None,)
        self.action_run.machine.transition("start")
        self.action_run.machine.transition("started")
        delay_deferred = self.action_run.handler(self.action_run.action_command, ActionCommand.EXITING,)
        assert delay_deferred == mock_reactor.callLater.return_value
        assert self.action_run.is_running
        assert self.action_run.exit_status is None
        assert self.action_run.end_time is None

        call_args = mock_reactor.callLater.call_args[0]
        assert call_args[0] == INITIAL_RECOVER_DELAY
        assert call_args[1] == self.action_run.submit_recovery_command

        # Check recovery run
        recovery_run = call_args[2]
        assert "recovery" in recovery_run.name
        assert isinstance(recovery_run, SSHActionRun)
        # Recovery run should not be recovering itself, parent run handles its unknown status
        assert recovery_run.recover() is None

        # Check command
        recovery_command = call_args[3]
        assert recovery_command.command == "/bin/foo/recover_batch.py /tmp/foo/job_name.5.action_name/status"
        assert recovery_command.start_time is not None  # already started

    @mock.patch("tron.core.actionrun.SSHActionRun.do_recover", autospec=True)
    @mock.patch("tron.core.actionrun.reactor", autospec=True)
    def test_handler_exiting_failunknown_max_retries(self, mock_reactor, mock_do_recover):
        self.action_run.action_command = mock.create_autospec(actioncommand.ActionCommand, exit_status=None,)
        self.action_run.machine.transition("start")
        self.action_run.machine.transition("started")

        def exit_unknown(*args, **kwargs):
            self.action_run.handler(
                self.action_run.action_command, ActionCommand.EXITING,
            )

        # Each time do_recover is called, end up exiting unknown again
        mock_do_recover.side_effect = exit_unknown

        # Start the cycle
        exit_unknown()

        assert mock_do_recover.call_count == MAX_RECOVER_TRIES
        last_call = mock_do_recover.call_args
        expected_delay = INITIAL_RECOVER_DELAY * (3 ** (MAX_RECOVER_TRIES - 1))
        assert last_call == mock.call(self.action_run, delay=expected_delay)

        assert self.action_run.is_unknown
        assert self.action_run.exit_status is None
        assert self.action_run.end_time is not None


class TestActionRunStateRestore:

    now = datetime.datetime(2012, 3, 14, 15, 19)

    @pytest.fixture(autouse=True)
    def setup_action_run(self, mock_current_time):
        self.parent_context = {}
        self.output_path = ["one", "two"]
        self.run_node = MagicMock()
        mock_current_time.return_value = self.now
        self.command_config = ActionCommandConfig(command="do {actionname}", cpus=1,)
        self.action_config = mock.Mock(command_config=self.command_config)
        self.action_graph = actiongraph.ActionGraph(
            {"theaction": self.action_config}, {"theaction": set()}, {"theaction": set()},
        )

    @pytest.fixture
    def state_data(self):
        # State data with command config and retries.
        yield {
            "job_run_id": "theid",
            "action_name": "theaction",
            "node_name": "anode",
            "run_time": "the_run_time",
            "start_time": "start_time",
            "end_time": "end",
            "exit_status": 0,
            "attempts": [
                dict(
                    command_config=self.command_config.state_data,
                    rendered_command="do theaction",
                    start_time="start",
                    end_time="end",
                    exit_status=0,
                ),
            ],
            "state": "succeeded",
        }

    @pytest.fixture
    def state_data_old(self):
        # State data before command config and retries are separate.
        yield {
            "job_run_id": "theid",
            "action_name": "theaction",
            "node_name": "anode",
            "command": "do {actionname}",
            "start_time": "start_time",
            "end_time": "end",
            "state": "succeeded",
        }

    def test_from_state_old(self, state_data_old):
        state_data = state_data_old
        action_run = ActionRun.from_state(
            state_data, self.parent_context, list(self.output_path), self.run_node, self.action_graph,
        )

        for key, value in state_data.items():
            if key in ["state", "node_name"]:
                continue
            assert getattr(action_run, key) == value

        assert action_run.is_succeeded
        assert not action_run.is_cleanup
        assert action_run.output_path[:2] == self.output_path
        assert action_run.command_config.command == state_data["command"]
        assert action_run.command == state_data["command"]

    def test_from_state_old_with_mesos_task_id(self, state_data_old):
        state_data = state_data_old
        state_data["mesos_task_id"] = "task"
        action_run = ActionRun.from_state(
            state_data, self.parent_context, list(self.output_path), self.run_node, self.action_graph,
        )

        for key, value in state_data.items():
            if key in ["state", "node_name", "mesos_task_id"]:
                continue
            assert getattr(action_run, key) == value

        assert action_run.is_succeeded
        assert action_run.last_attempt.mesos_task_id == state_data["mesos_task_id"]

    def test_from_state_old_not_started(self, state_data_old):
        state_data = state_data_old
        state_data["start_time"] = None
        state_data["state"] = "scheduled"
        action_run = ActionRun.from_state(
            state_data, self.parent_context, list(self.output_path), self.run_node, self.action_graph,
        )

        for key, value in state_data.items():
            if key in ["state", "node_name"]:
                continue
            assert getattr(action_run, key) == value

        assert action_run.is_scheduled
        assert action_run.exit_statuses == []
        assert len(action_run.attempts) == 0

    def test_from_state_old_rendered_and_exited(self, state_data_old):
        state_data = state_data_old
        state_data["rendered_command"] = "do things theaction"
        state_data["exit_status"] = 0
        action_run = ActionRun.from_state(
            state_data, self.parent_context, list(self.output_path), self.run_node, self.action_graph,
        )

        for key, value in state_data.items():
            if key in ["state", "node_name", "command", "rendered_command"]:
                continue
            assert getattr(action_run, key) == value

        assert action_run.is_succeeded
        assert action_run.exit_statuses == [0]
        assert action_run.command_config.command == state_data["command"]
        assert action_run.command == state_data["rendered_command"]

    def test_from_state_old_retries(self, state_data_old):
        state_data = state_data_old
        state_data["rendered_command"] = "do things theaction"
        state_data["exit_status"] = 0
        state_data["exit_statuses"] = [1]
        action_run = ActionRun.from_state(
            state_data, self.parent_context, list(self.output_path), self.run_node, self.action_graph,
        )

        for key, value in state_data.items():
            if key in [
                "state",
                "node_name",
                "command",
                "rendered_command",
                "exit_statuses",
            ]:
                continue
            assert getattr(action_run, key) == value

        assert action_run.is_succeeded
        assert action_run.exit_statuses == [1, 0]
        assert len(action_run.attempts) == 2

    def test_from_state_running(self, state_data):
        state_data["state"] = "running"
        action_run = ActionRun.from_state(
            state_data, self.parent_context, self.output_path, self.run_node, self.action_graph, lambda: None,
        )
        assert action_run.is_unknown

    def test_from_state_starting(self, state_data):
        state_data["state"] = "starting"
        action_run = ActionRun.from_state(
            state_data, self.parent_context, self.output_path, self.run_node, self.action_graph, lambda: None,
        )
        assert action_run.is_unknown

    def test_from_state_queued(self, state_data):
        state_data["state"] = "queued"
        action_run = ActionRun.from_state(
            state_data, self.parent_context, self.output_path, self.run_node, self.action_graph, lambda: None,
        )
        assert action_run.is_queued

    def test_from_state_no_node_name(self, state_data):
        del state_data["node_name"]
        action_run = ActionRun.from_state(
            state_data, self.parent_context, self.output_path, self.run_node, self.action_graph, lambda: None,
        )
        assert action_run.node == self.run_node

    @mock.patch("tron.core.actionrun.node.NodePoolRepository", autospec=True)
    def test_from_state_with_node_exists(self, mock_store, state_data):
        ActionRun.from_state(
            state_data, self.parent_context, self.output_path, self.run_node, self.action_graph, lambda: None,
        )
        mock_store.get_instance().get_node.assert_called_with(
            state_data["node_name"], self.run_node,
        )

    def test_from_state_after_rendered_command(self, state_data):
        action_run = ActionRun.from_state(
            state_data, self.parent_context, self.output_path, self.run_node, self.action_graph, lambda: None,
        )
        assert action_run.command_config == self.command_config
        assert len(action_run.attempts) == len(state_data["attempts"])
        assert action_run.exit_statuses == [0]
        assert action_run.command == state_data["attempts"][-1]["rendered_command"]

    def test_from_state_action_config_gone(self, state_data):
        state_data["action_name"] = "old_action"
        action_run = ActionRun.from_state(
            state_data, self.parent_context, self.output_path, self.run_node, self.action_graph, lambda: None,
        )
        assert action_run.command_config.command == ""
        assert action_run.command == state_data["attempts"][-1]["rendered_command"]


class TestActionRunCollection:
    def _build_run(self, action):
        mock_node = mock.create_autospec(node.Node)
        return ActionRun(
            "id", action.name, mock_node, command_config=action.command_config, output_path=self.output_path,
        )

    @pytest.fixture(autouse=True)
    def setup_runs(self, output_path):
        action_names = ["action_name", "second_name", "cleanup"]

        actions = []
        for name in action_names:
            m = mock.Mock(name=name, required_actions=[], command_config=ActionCommandConfig(command="old"),)
            m.name = name
            actions.append(m)

        self.action_graph = actiongraph.ActionGraph(
            {a.name: a for a in actions},
            {"action_name": set(), "second_name": set(), "cleanup": set()},
            {"action_name": set(), "second_name": set(), "cleanup": set()},
        )
        self.output_path = output_path
        self.command = "do command"
        self.action_runs = [self._build_run(action) for action in actions]
        self.run_map = {a.action_name: a for a in self.action_runs}
        self.run_map["cleanup"].is_cleanup = True
        self.collection = ActionRunCollection(self.action_graph, self.run_map)

    def test__init__(self):
        assert self.collection.action_graph == self.action_graph
        assert self.collection.run_map == self.run_map
        assert self.collection.proxy_action_runs_with_cleanup

    def test_action_runs_for_actions(self):
        m = MagicMock()
        m.name = "action_name"
        actions = [m]
        action_runs = self.collection.action_runs_for_actions(actions)
        assert list(action_runs) == self.action_runs[:1]

    def test_get_action_runs_with_cleanup(self):
        runs = self.collection.get_action_runs_with_cleanup()
        assert set(runs) == set(self.action_runs)

    def test_get_action_runs(self):
        runs = self.collection.get_action_runs()
        assert set(runs) == set(self.action_runs[:2])

    def test_cleanup_action_run(self):
        assert self.action_runs[2] == self.collection.cleanup_action_run

    def test_update_action_config_no_changes(self):
        assert self.collection.update_action_config(self.action_graph) is False

    def test_update_action_config(self):
        # Latest config has 'new_name' instead of 'action_name'
        new_action_names = ["new_name", "second_name", "cleanup"]
        new_actions = []
        for name in new_action_names:
            action = mock.Mock(name=name, required_actions=[], command_config=ActionCommandConfig(command="new"),)
            action.name = name
            new_actions.append(action)

        new_action_graph = actiongraph.ActionGraph(
            {a.name: a for a in new_actions},
            {"new_name": set(), "second_name": set(), "cleanup": set()},
            {"new_name": set(), "second_name": set(), "cleanup": set()},
        )
        assert self.collection.update_action_config(new_action_graph) is True
        assert self.collection.action_graph != new_action_graph

        updated_action_runs = self.collection.action_runs_with_cleanup
        # Action names should be unchanged
        assert sorted(run.name for run in updated_action_runs) == sorted(run.name for run in self.action_runs)

        for run in updated_action_runs:
            if run.name == "action_name":
                assert run.command_config.command == "old"
            else:
                assert run.command_config.command == "new"

    def test_state_data(self):
        state_data = self.collection.state_data
        assert_length(state_data, len(self.action_runs[:2]))

    def test_cleanup_action_state_data(self):
        state_data = self.collection.cleanup_action_state_data
        assert state_data["action_name"] == "cleanup"

    def test_cleanup_action_state_data_no_cleanup_action(self):
        del self.collection.run_map["cleanup"]
        assert not self.collection.cleanup_action_state_data

    def test_get_startable_action_runs(self):
        action_runs = self.collection.get_startable_action_runs()
        assert set(action_runs) == set(self.action_runs[:2])

    def test_get_startable_action_runs_none(self):
        self.collection.run_map.clear()
        action_runs = self.collection.get_startable_action_runs()
        assert set(action_runs) == set()

    def test_has_startable_action_runs(self):
        assert self.collection.has_startable_action_runs

    def test_has_startable_action_runs_false(self):
        self.collection.run_map.clear()
        assert not self.collection.has_startable_action_runs

    def test_is_complete_false(self):
        assert not self.collection.is_complete

    def test_is_complete_true(self):
        for action_run in self.collection.action_runs_with_cleanup:
            action_run.machine.state = ActionRun.SKIPPED
        assert self.collection.is_complete

    def test_is_done_false(self):
        assert not self.collection.is_done

    def test_is_done_false_because_of_running(self):
        action_run = self.collection.run_map["action_name"]
        action_run.machine.state = ActionRun.RUNNING
        assert not self.collection.is_done

    def test_is_done_true_because_blocked(self):
        self.run_map["action_name"].machine.state = ActionRun.FAILED
        self.run_map["second_name"].machine.state = ActionRun.WAITING
        autospec_method(self.collection._is_run_blocked)

        self.collection._is_run_blocked.return_value = True
        assert self.collection.is_done
        assert self.collection.is_failed
        self.collection._is_run_blocked.assert_called_with(
            self.run_map["second_name"], in_job_only=True,
        )

    def test_is_done_true(self):
        for action_run in self.collection.action_runs_with_cleanup:
            action_run.machine.state = ActionRun.FAILED
        assert self.collection.is_done

    def test_is_failed_false_not_done(self):
        self.run_map["action_name"].machine.state = ActionRun.FAILED
        assert not self.collection.is_failed

    def test_is_failed_false_no_failed(self):
        for action_run in self.collection.action_runs_with_cleanup:
            action_run.machine.state = ActionRun.SUCCEEDED
        assert not self.collection.is_failed

    def test_is_failed_true(self):
        for action_run in self.collection.action_runs_with_cleanup:
            action_run.machine.state = ActionRun.FAILED
        assert self.collection.is_failed

    def test__getattr__(self):
        assert self.collection.is_scheduled
        assert not self.collection.is_cancelled
        assert not self.collection.is_running
        assert self.collection.ready()

    def test__str__(self):
        self.collection._is_run_blocked = lambda r: r.action_name != "cleanup"
        expected = [
            "ActionRunCollection",
            "second_name(scheduled:blocked)",
            "action_name(scheduled:blocked)",
            "cleanup(scheduled)",
        ]
        for expectation in expected:
            assert expectation in str(self.collection)

    def test_end_time(self):
        max_end_time = datetime.datetime(2013, 6, 15)
        self.run_map["action_name"].machine.state = ActionRun.FAILED
        self.run_map["action_name"].end_time = datetime.datetime(2013, 5, 12)
        self.run_map["second_name"].machine.state = ActionRun.SUCCEEDED
        self.run_map["second_name"].end_time = max_end_time
        assert self.collection.end_time == max_end_time

    def test_end_time_not_done(self):
        self.run_map["action_name"].end_time = datetime.datetime(2013, 5, 12)
        self.run_map["action_name"].machine.state = ActionRun.FAILED
        self.run_map["second_name"].end_time = None
        self.run_map["second_name"].machine.state = ActionRun.RUNNING
        assert self.collection.end_time is None

    def test_end_time_not_started(self):
        assert self.collection.end_time is None


class TestActionRunCollectionIsRunBlocked:
    def _build_run(self, name):
        mock_node = mock.create_autospec(node.Node)
        return ActionRun("id", name, mock_node, self.command_config, output_path=self.output_path,)

    @pytest.fixture(autouse=True)
    def setup_collection(self, output_path):
        action_names = ["action_name", "second_name", "cleanup"]

        actions = []
        for name in action_names:
            m = MagicMock()
            m.name = name
            actions.append(m)

        self.second_act = actions[1]
        action_map = {a.name: a for a in actions}
        self.action_graph = actiongraph.ActionGraph(
            action_map,
            {"action_name": set(), "second_name": {"action_name"}, "cleanup": set()},
            {"action_name": set(), "second_name": set(), "cleanup": set()},
        )

        self.output_path = output_path
        self.command_config = ActionCommandConfig(command="do command")
        self.action_runs = [self._build_run(name) for name in action_names]
        self.run_map = {a.action_name: a for a in self.action_runs}
        self.run_map["cleanup"].is_cleanup = True
        self.collection = ActionRunCollection(self.action_graph, self.run_map)

    def test_is_run_blocked_no_required_actions(self):
        assert not self.collection._is_run_blocked(self.run_map["action_name"])

    def test_is_run_blocked_completed_run(self):
        self.run_map["second_name"].machine.state = ActionRun.FAILED
        assert not self.collection._is_run_blocked(self.run_map["second_name"])

        self.run_map["second_name"].machine.state = ActionRun.RUNNING
        assert not self.collection._is_run_blocked(self.run_map["second_name"])

    def test_is_run_blocked_required_actions_completed(self):
        self.run_map["action_name"].machine.state = ActionRun.SKIPPED
        assert not self.collection._is_run_blocked(self.run_map["second_name"])

    def test_is_run_blocked_required_actions_blocked(self):
        third_act = MagicMock()
        third_act.name = "third_act"
        self.action_graph.action_map["third_act"] = third_act
        self.action_graph.required_actions["third_act"] = {self.second_act.name}
        self.run_map["third_act"] = self._build_run("third_act")

        self.run_map["action_name"].machine.state = ActionRun.FAILED
        assert self.collection._is_run_blocked(self.run_map["third_act"])

    def test_is_run_blocked_required_actions_scheduled(self):
        self.run_map["action_name"].machine.state = ActionRun.SCHEDULED
        assert self.collection._is_run_blocked(self.run_map["second_name"])

    def test_is_run_blocked_required_actions_starting(self):
        self.run_map["action_name"].machine.state = ActionRun.STARTING
        assert self.collection._is_run_blocked(self.run_map["second_name"])

    def test_is_run_blocked_required_actions_waiting(self):
        self.run_map["action_name"].machine.state = ActionRun.WAITING
        assert self.collection._is_run_blocked(self.run_map["second_name"])

    def test_is_run_blocked_required_actions_failed(self):
        self.run_map["action_name"].machine.state = ActionRun.FAILED
        assert self.collection._is_run_blocked(self.run_map["second_name"])

    def test_is_run_blocked_required_actions_missing(self):
        del self.run_map["action_name"]
        assert not self.collection._is_run_blocked(self.run_map["second_name"])

    def test_is_run_blocked_in_job_only(self):
        self.run_map["action_name"].machine.state = ActionRun.SKIPPED
        self.run_map["second_name"].triggered_by = ["trigger"]
        assert not self.collection._is_run_blocked(self.run_map["second_name"], in_job_only=True)
        assert self.collection._is_run_blocked(self.run_map["second_name"], in_job_only=False)


class TestMesosActionRun:
    @pytest.fixture(autouse=True)
    def setup_action_run(self):
        self.output_path = mock.MagicMock()
        self.command = "do the command"
        self.extra_volumes = [ConfigVolume("/mnt/foo", "/mnt/foo", "RO")]
        self.constraints = [ConfigConstraint("an attr", "an op", "a val")]
        self.docker_parameters = [ConfigParameter("init", "true")]
        self.other_task_kwargs = {
            "cpus": 1,
            "mem": 50,
            "disk": 42,
            "docker_image": "container:v2",
            "env": {
                "TESTING": "true",
                "TRON_JOB_NAMESPACE": "mynamespace",
                "TRON_JOB_NAME": "myjob",
                "TRON_RUN_NUM": "42",
                "TRON_ACTION": "action_name",
            },
        }
        command_config = ActionCommandConfig(
            command=self.command,
            extra_volumes=self.extra_volumes,
            constraints=self.constraints,
            docker_parameters=self.docker_parameters,
            **self.other_task_kwargs,
        )
        self.action_run = MesosActionRun(
            job_run_id="mynamespace.myjob.42",
            name="action_name",
            command_config=command_config,
            node=mock.create_autospec(node.Node),
            output_path=self.output_path,
            executor=ExecutorTypes.mesos.value,
        )

    @mock.patch("tron.core.actionrun.filehandler", autospec=True)
    @mock.patch("tron.core.actionrun.MesosClusterRepository", autospec=True)
    def test_submit_command(self, mock_cluster_repo, mock_filehandler):
        serializer = mock_filehandler.OutputStreamSerializer.return_value
        # submit_command should add a new attempt
        self.action_run.attempts = [
            ActionRunAttempt(
                command_config=self.action_run.command_config,
                rendered_command=self.command,
                mesos_task_id="last_attempt",
            ),
        ]
        with mock.patch.object(self.action_run, "watch", autospec=True,) as mock_watch:
            new_attempt = self.action_run.create_attempt()
            self.action_run.submit_command(new_attempt)

            mock_get_cluster = mock_cluster_repo.get_cluster
            mock_get_cluster.assert_called_once_with()

            mock_get_cluster.return_value.create_task.assert_called_once_with(
                action_run_id=self.action_run.id,
                command=self.command,
                serializer=serializer,
                task_id=None,
                extra_volumes=[e._asdict() for e in self.extra_volumes],
                constraints=[["an attr", "an op", "a val"]],
                docker_parameters=[{"key": "init", "value": "true"}],
                **self.other_task_kwargs,
            )
            task = mock_get_cluster.return_value.create_task.return_value
            mock_get_cluster.return_value.submit.assert_called_once_with(task)
            mock_watch.assert_called_once_with(task)
            assert self.action_run.last_attempt.mesos_task_id == task.get_mesos_id.return_value

        mock_filehandler.OutputStreamSerializer.assert_called_with(self.action_run.output_path,)

    @mock.patch("tron.core.actionrun.filehandler", autospec=True)
    @mock.patch("tron.core.actionrun.MesosClusterRepository", autospec=True)
    def test_submit_command_task_none(
        self, mock_cluster_repo, mock_filehandler,
    ):
        # Task is None if Mesos is disabled
        mock_get_cluster = mock_cluster_repo.get_cluster
        mock_get_cluster.return_value.create_task.return_value = None
        new_attempt = self.action_run.create_attempt()
        self.action_run.submit_command(new_attempt)

        mock_get_cluster.assert_called_once_with()
        assert mock_get_cluster.return_value.submit.call_count == 0
        assert self.action_run.is_failed

    @mock.patch("tron.core.actionrun.filehandler", autospec=True)
    @mock.patch("tron.core.actionrun.MesosClusterRepository", autospec=True)
    def test_recover(self, mock_cluster_repo, mock_filehandler):
        self.action_run.machine.state = ActionRun.UNKNOWN
        self.action_run.end_time = 1000
        self.action_run.exit_status = 0
        last_attempt = self.action_run.create_attempt()
        last_attempt.mesos_task_id = "my_mesos_id"
        last_attempt.end_time = 1000
        last_attempt.exit_status = 0
        serializer = mock_filehandler.OutputStreamSerializer.return_value
        with mock.patch.object(self.action_run, "watch", autospec=True,) as mock_watch:
            assert self.action_run.recover()

            mock_get_cluster = mock_cluster_repo.get_cluster
            mock_get_cluster.assert_called_once_with()
            mock_get_cluster.return_value.create_task.assert_called_once_with(
                action_run_id=self.action_run.id,
                command=self.command,
                serializer=serializer,
                task_id="my_mesos_id",
                extra_volumes=[e._asdict() for e in self.extra_volumes],
                constraints=[["an attr", "an op", "a val"]],
                docker_parameters=[{"key": "init", "value": "true"}],
                **self.other_task_kwargs,
            ), mock_get_cluster.return_value.create_task.calls
            task = mock_get_cluster.return_value.create_task.return_value
            mock_get_cluster.return_value.recover.assert_called_once_with(task)
            mock_watch.assert_called_once_with(task)

        assert self.action_run.is_running
        assert self.action_run.end_time is None
        assert self.action_run.exit_status is None
        assert last_attempt.end_time is None
        assert last_attempt.exit_status is None
        mock_filehandler.OutputStreamSerializer.assert_called_with(self.action_run.output_path,)

    @mock.patch("tron.core.actionrun.filehandler", autospec=True)
    @mock.patch("tron.core.actionrun.MesosClusterRepository", autospec=True)
    def test_recover_done_no_change(self, mock_cluster_repo, mock_filehandler):
        self.action_run.machine.state = ActionRun.SUCCEEDED
        last_attempt = self.action_run.create_attempt()
        last_attempt.mesos_task_id = "my_mesos_id"

        assert not self.action_run.recover()
        assert mock_cluster_repo.get_cluster.call_count == 0
        assert self.action_run.is_succeeded

    @mock.patch("tron.core.actionrun.filehandler", autospec=True)
    @mock.patch("tron.core.actionrun.MesosClusterRepository", autospec=True)
    def test_recover_no_mesos_task_id(
        self, mock_cluster_repo, mock_filehandler,
    ):
        self.action_run.machine.state = ActionRun.UNKNOWN
        last_attempt = self.action_run.create_attempt()
        last_attempt.mesos_task_id = None

        assert not self.action_run.recover()
        assert mock_cluster_repo.get_cluster.call_count == 0
        assert self.action_run.is_unknown
        assert self.action_run.end_time is not None

    @mock.patch("tron.core.actionrun.filehandler", autospec=True)
    @mock.patch("tron.core.actionrun.MesosClusterRepository", autospec=True)
    def test_recover_task_none(self, mock_cluster_repo, mock_filehandler):
        self.action_run.machine.state = ActionRun.UNKNOWN
        last_attempt = self.action_run.create_attempt()
        last_attempt.mesos_task_id = "my_mesos_id"
        # Task is None if Mesos is disabled
        mock_get_cluster = mock_cluster_repo.get_cluster
        mock_get_cluster.return_value.create_task.return_value = None
        assert not self.action_run.recover()

        mock_get_cluster.assert_called_once_with()
        assert self.action_run.is_unknown
        assert mock_get_cluster.return_value.recover.call_count == 0
        assert self.action_run.end_time is not None

    @mock.patch("tron.core.actionrun.MesosClusterRepository", autospec=True)
    def test_kill_task(self, mock_cluster_repo):
        mock_get_cluster = mock_cluster_repo.get_cluster
        last_attempt = self.action_run.create_attempt()
        last_attempt.mesos_task_id = "fake_task_id"
        self.action_run.machine.state = ActionRun.RUNNING

        self.action_run.kill()
        mock_get_cluster.return_value.kill.assert_called_once_with(last_attempt.mesos_task_id,)

    @mock.patch("tron.core.actionrun.MesosClusterRepository", autospec=True)
    def test_kill_task_no_task_id(self, mock_cluster_repo):
        self.action_run.machine.state = ActionRun.RUNNING
        self.action_run.create_attempt()
        error_message = self.action_run.kill()
        assert error_message == "Error: Can't find task id for the action."

    @mock.patch("tron.core.actionrun.MesosClusterRepository", autospec=True)
    def test_stop_task(self, mock_cluster_repo):
        mock_get_cluster = mock_cluster_repo.get_cluster
        last_attempt = self.action_run.create_attempt()
        last_attempt.mesos_task_id = "fake_task_id"
        self.action_run.machine.state = ActionRun.RUNNING

        self.action_run.stop()
        mock_get_cluster.return_value.kill.assert_called_once_with(last_attempt.mesos_task_id,)

    @mock.patch("tron.core.actionrun.MesosClusterRepository", autospec=True)
    def test_stop_task_no_task_id(self, mock_cluster_repo):
        self.action_run.machine.state = ActionRun.RUNNING
        self.action_run.create_attempt()
        error_message = self.action_run.stop()
        assert error_message == "Error: Can't find task id for the action."

    def test_handler_exiting_unknown(self):
        self.action_run.action_command = mock.create_autospec(actioncommand.ActionCommand, exit_status=None,)
        self.action_run.machine.transition("start")
        self.action_run.machine.transition("started")
        assert self.action_run.handler(self.action_run.action_command, ActionCommand.EXITING,)
        assert self.action_run.is_unknown
        assert self.action_run.exit_status is None
        assert self.action_run.end_time is not None

    def test_handler_exiting_unknown_retry(self):
        self.action_run.action_command = mock.create_autospec(actioncommand.ActionCommand, exit_status=None,)
        self.action_run.retries_remaining = 1
        self.action_run.start = mock.Mock()

        self.action_run.machine.transition("start")
        self.action_run.machine.transition("started")
        assert self.action_run.handler(self.action_run.action_command, ActionCommand.EXITING,)
        assert self.action_run.retries_remaining == 0
        assert not self.action_run.is_unknown
        assert self.action_run.start.call_count == 1

    def test_handler_exiting_failstart_failed(self):
        self.action_run.action_command = mock.create_autospec(actioncommand.ActionCommand, exit_status=1,)
        self.action_run.machine.transition("start")
        assert self.action_run.handler(self.action_run.action_command, ActionCommand.FAILSTART,)
        assert self.action_run.is_failed


class TestKubernetesActionRun:
    @pytest.fixture
    def mock_k8s_action_run(self):
        command_config = ActionCommandConfig(
            command="mock_command",
            extra_volumes=set(),
            constraints=set(),
            docker_parameters=set(),
            cpus=1,
            mem=50,
            disk=42,
            docker_image="container:v2",
            env={
                "TESTING": "true",
                "TRON_JOB_NAMESPACE": "mock_namespace",
                "TRON_JOB_NAME": "mock_job",
                "TRON_RUN_NUM": "42",
                "TRON_ACTION": "mock_action_name",
            },
        )

        return KubernetesActionRun(
            job_run_id="mock_namespace.mock_job.42",
            name="mock_action_name",
            command_config=command_config,
            node=mock.create_autospec(node.Node),
            output_path=mock.create_autospec(filehandler.OutputPath),
            executor=ExecutorTypes.kubernetes.value,
        )

    def test_k8s_handler_exiting_unknown(self, mock_k8s_action_run):
        mock_k8s_action_run.action_command = mock.create_autospec(actioncommand.ActionCommand, exit_status=None,)
        mock_k8s_action_run.machine.transition("start")
        mock_k8s_action_run.machine.transition("started")
        assert mock_k8s_action_run.handler(mock_k8s_action_run.action_command, ActionCommand.EXITING,)
        assert mock_k8s_action_run.is_unknown
        assert mock_k8s_action_run.exit_status is None
        assert mock_k8s_action_run.end_time is not None

    def test_handler_exiting_unknown_retry(self, mock_k8s_action_run):
        mock_k8s_action_run.action_command = mock.create_autospec(actioncommand.ActionCommand, exit_status=None,)
        mock_k8s_action_run.retries_remaining = 1
        mock_k8s_action_run.start = mock.Mock()

        mock_k8s_action_run.machine.transition("start")
        mock_k8s_action_run.machine.transition("started")
        assert mock_k8s_action_run.handler(mock_k8s_action_run.action_command, ActionCommand.EXITING,)
        assert mock_k8s_action_run.retries_remaining == 0
        assert not mock_k8s_action_run.is_unknown
        assert mock_k8s_action_run.start.call_count == 1

    def test_handler_exiting_failstart_failed(self, mock_k8s_action_run):
        mock_k8s_action_run.action_command = mock.create_autospec(actioncommand.ActionCommand, exit_status=1,)
        mock_k8s_action_run.machine.transition("start")
        assert mock_k8s_action_run.handler(mock_k8s_action_run.action_command, ActionCommand.FAILSTART,)
        assert mock_k8s_action_run.is_failed

    @mock.patch("tron.core.actionrun.filehandler", autospec=True)
    @mock.patch("tron.core.actionrun.KubernetesClusterRepository", autospec=True)
    def test_recover(self, mock_cluster_repo, mock_filehandler, mock_k8s_action_run):
        mock_k8s_action_run.machine.state = ActionRun.UNKNOWN
        mock_k8s_action_run.end_time = 1000
        mock_k8s_action_run.exit_status = 0
        last_attempt = mock_k8s_action_run.create_attempt()
        last_attempt.kubernetes_task_id = "test-k8s-task-id"
        last_attempt.end_time = 1000
        last_attempt.exit_status = 0
        serializer = mock_filehandler.OutputStreamSerializer.return_value
        with mock.patch.object(mock_k8s_action_run, "watch", autospec=True,) as mock_watch:
            assert mock_k8s_action_run.recover()

            mock_get_cluster = mock_cluster_repo.get_cluster
            mock_get_cluster.assert_called_once_with()
            mock_get_cluster.return_value.create_task.assert_called_once_with(
                action_run_id=mock_k8s_action_run.id,
                command=last_attempt.rendered_command,
                cpus=mock_k8s_action_run.command_config.cpus,
                mem=mock_k8s_action_run.command_config.mem,
                disk=mock_k8s_action_run.command_config.disk,
                docker_image=mock_k8s_action_run.command_config.docker_image,
                env=mock.ANY,
                secret_env=mock_k8s_action_run.command_config.secret_env,
                serializer=serializer,
                volumes=mock_k8s_action_run.command_config.extra_volumes,
                cap_add=mock_k8s_action_run.command_config.cap_add,
                cap_drop=mock_k8s_action_run.command_config.cap_drop,
                task_id=last_attempt.kubernetes_task_id,
                node_selectors=mock_k8s_action_run.command_config.node_selectors,
                node_affinities=mock_k8s_action_run.command_config.node_affinities,
                pod_labels=mock_k8s_action_run.command_config.labels,
                pod_annotations=mock_k8s_action_run.command_config.annotations,
                service_account_name=mock_k8s_action_run.command_config.service_account_name,
            ), mock_get_cluster.return_value.create_task.calls
            task = mock_get_cluster.return_value.create_task.return_value
            mock_get_cluster.return_value.recover.assert_called_once_with(task)
            mock_watch.assert_called_once_with(task)

        assert mock_k8s_action_run.is_running
        assert mock_k8s_action_run.end_time is None
        assert mock_k8s_action_run.exit_status is None
        assert last_attempt.end_time is None
        assert last_attempt.exit_status is None
        mock_filehandler.OutputStreamSerializer.assert_called_with(mock_k8s_action_run.output_path,)

    @mock.patch("tron.core.actionrun.filehandler", autospec=True)
    @mock.patch("tron.core.actionrun.MesosClusterRepository", autospec=True)
    def test_recover_done_no_change(
        self, mock_cluster_repo, mock_filehandler, mock_k8s_action_run,
    ):
        mock_k8s_action_run.machine.state = ActionRun.SUCCEEDED
        last_attempt = mock_k8s_action_run.create_attempt()
        last_attempt.kubernetes_task_ic = "test-kubernetes-task-id"

        assert not mock_k8s_action_run.recover()
        assert mock_cluster_repo.get_cluster.call_count == 0
        assert mock_k8s_action_run.is_succeeded

    @mock.patch("tron.core.actionrun.filehandler", autospec=True)
    @mock.patch("tron.core.actionrun.KubernetesClusterRepository", autospec=True)
    def test_recover_no_k8s_task_id(
        self, mock_cluster_repo, mock_filehandler, mock_k8s_action_run,
    ):
        print(f"cluster: {type(mock_cluster_repo)} filehand: {type(mock_filehandler)} ar: {type(mock_k8s_action_run)}")
        mock_k8s_action_run.machine.state = ActionRun.UNKNOWN
        last_attempt = mock_k8s_action_run.create_attempt()
        last_attempt.mesos_task_id = None

        assert not mock_k8s_action_run.recover()
        assert mock_k8s_action_run.is_unknown
        assert mock_k8s_action_run.end_time is not None

    @mock.patch("tron.core.actionrun.filehandler", autospec=True)
    @mock.patch("tron.core.actionrun.KubernetesClusterRepository", autospec=True)
    def test_recover_task_none(self, mock_cluster_repo, mock_filehandler, mock_k8s_action_run):
        mock_k8s_action_run.machine.state = ActionRun.UNKNOWN
        last_attempt = mock_k8s_action_run.create_attempt()
        last_attempt.kubernetes_task_id = "test-kubernetes-task-id"
        # Task is None e.g. if Kubernetes is disabled
        mock_get_cluster = mock_cluster_repo.get_cluster
        mock_get_cluster.return_value.create_task.return_value = None
        assert not mock_k8s_action_run.recover()

        mock_get_cluster.assert_called_once_with()
        assert mock_k8s_action_run.is_unknown
        assert mock_get_cluster.return_value.recover.call_count == 0
        assert mock_k8s_action_run.end_time is not None

    @mock.patch("tron.core.actionrun.KubernetesClusterRepository", autospec=True)
    def test_kill_task_k8s(self, mock_cluster_repo, mock_k8s_action_run):
        mock_get_cluster = mock_cluster_repo.get_cluster
        last_attempt = mock_k8s_action_run.create_attempt()
        last_attempt.kubernetes_task_id = "fake_task_id"
        mock_k8s_action_run.machine.state = ActionRun.RUNNING

        mock_k8s_action_run.kill()
        mock_get_cluster.return_value.kill.assert_called_once_with(last_attempt.kubernetes_task_id)

    @mock.patch("tron.core.actionrun.KubernetesClusterRepository", autospec=True)
    def test_kill_task_no_task_id_k8s(self, mock_cluster_repo, mock_k8s_action_run):
        mock_k8s_action_run.machine.state = ActionRun.RUNNING
        mock_k8s_action_run.create_attempt()
        error_message = mock_k8s_action_run.kill()
        assert error_message == "Error: Can't find task id for the action."

    @mock.patch("tron.core.actionrun.KubernetesClusterRepository", autospec=True)
    def test_stop_task_k8s(self, mock_cluster_repo, mock_k8s_action_run):
        mock_get_cluster = mock_cluster_repo.get_cluster
        last_attempt = mock_k8s_action_run.create_attempt()
        last_attempt.kubernetes_task_id = "fake_task_id"
        mock_k8s_action_run.machine.state = ActionRun.RUNNING

        mock_k8s_action_run.stop()
        mock_get_cluster.return_value.kill.assert_called_once_with(last_attempt.kubernetes_task_id)

    @mock.patch("tron.core.actionrun.KubernetesClusterRepository", autospec=True)
    def test_stop_task_no_task_id_k8s(self, mock_cluster_repo, mock_k8s_action_run):
        mock_k8s_action_run.machine.state = ActionRun.RUNNING
        mock_k8s_action_run.create_attempt()
        error_message = mock_k8s_action_run.stop()
        assert error_message == "Error: Can't find task id for the action."
