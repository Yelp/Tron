from __future__ import absolute_import
from __future__ import unicode_literals

import collections
import datetime
from unittest.mock import MagicMock

import mock
import six

from testifycompat import assert_equal
from testifycompat import assert_not_equal
from testifycompat import run
from testifycompat import setup
from testifycompat import setup_teardown
from testifycompat import TestCase
from tests import testingutils
from tests.assertions import assert_call
from tests.assertions import assert_length
from tests.assertions import assert_mock_calls
from tests.testingutils import autospec_method
from tron import actioncommand
from tron import node
from tron import scheduler
from tron.core import job
from tron.core import jobrun
from tron.core.actionrun import ActionRun


class TestJob(TestCase):
    @setup_teardown
    def setup_job(self):
        action_graph = mock.Mock(names=lambda: ['one', 'two'])
        scheduler = mock.Mock()
        run_collection = MagicMock()
        self.nodes = mock.create_autospec(node.NodePool)
        self.action_runner = mock.create_autospec(
            actioncommand.SubprocessActionRunnerFactory,
        )

        patcher = mock.patch(
            'tron.core.job.node.NodePoolRepository', autospec=True
        )
        with patcher as self.mock_node_repo:
            self.job = job.Job(
                "jobname",
                scheduler,
                eventbus_publish=lambda: None,
                run_collection=run_collection,
                action_graph=action_graph,
                node_pool=self.nodes,
                action_runner=actioncommand.NoActionRunnerFactory
            )
            autospec_method(self.job.notify)
            autospec_method(self.job.watch)
            yield

    def test__init__(self):
        assert str(self.job.output_path).endswith(self.job.name)

    def test_from_config(self):
        action = mock.MagicMock(
            name='first',
            command='doit',
            node=None,
            requires=[],
        )
        job_config = mock.Mock(
            name='ajob',
            node='thenodepool',
            monitoring={
                "team": "foo",
                "page": True,
            },
            all_nodes=False,
            queueing=True,
            enabled=True,
            run_limit=20,
            actions={action.name: action},
            cleanup_action=None,
        )
        scheduler = 'scheduler_token'
        parent_context = 'parent_context_token'
        output_path = ["base_path"]
        new_job = job.Job.from_config(
            job_config,
            scheduler,
            eventbus_publish=lambda: None,
            parent_context=parent_context,
            output_path=output_path,
            action_runner=self.action_runner,
        )

        assert_equal(new_job.scheduler, scheduler)
        assert_equal(new_job.context.next, parent_context)
        self.mock_node_repo.get_instance().get_by_name.assert_called_with(
            job_config.node,
        )
        assert_equal(new_job.enabled, True)
        assert_equal(new_job.get_monitoring()["team"], "foo")
        assert new_job.action_graph

    def test_update_from_job(self):
        action_runner = mock.Mock()
        other_job = job.Job(
            'otherjob',
            'scheduler',
            eventbus_publish=lambda: None,
            action_runner=action_runner,
        )
        self.job.update_from_job(other_job)
        assert_equal(self.job.name, 'otherjob')
        assert_equal(self.job.scheduler, 'scheduler')
        assert_equal(self.job, other_job)

    def test_status_disabled(self):
        self.job.enabled = False
        assert_equal(self.job.status, self.job.STATUS_DISABLED)

    def test_status_enabled(self):
        def state_in(state):
            return state in [ActionRun.STATE_SCHEDULED, ActionRun.STATE_QUEUED]

        self.job.runs.get_run_by_state = state_in
        assert_equal(self.job.status, self.job.STATUS_ENABLED)

    def test_status_running(self):
        self.job.runs.get_run_by_state = lambda s: MagicMock()
        assert_equal(self.job.status, self.job.STATUS_RUNNING)

    def test_status_unknown(self):
        self.job.runs.get_run_by_state = lambda s: None
        assert_equal(self.job.status, self.job.STATUS_UNKNOWN)

    def test_state_data(self):
        state_data = self.job.state_data
        assert_equal(state_data['runs'], self.job.runs.state_data)
        assert state_data['enabled']

    def test_get_job_runs_from_state(self):
        job_runs = [
            dict(
                run_num=i,
                job_name="thename",
                run_time="sometime",
                start_time="start_time",
                end_time="sometime",
                cleanup_run=None,
                runs=[],
            ) for i in range(0, 3)
        ]
        state_data = {'enabled': False, 'runs': job_runs}
        self.job.get_job_runs_from_state(state_data)
        assert not self.job.enabled

    def test_build_new_runs(self):
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        runs = list(self.job.build_new_runs(run_time))

        self.job.node_pool.next.assert_called_with()
        node = self.job.node_pool.next.return_value
        assert_call(
            self.job.runs.build_new_run,
            0,
            self.job,
            run_time,
            node,
            manual=False,
        )
        assert_length(runs, 1)
        self.job.watch.assert_called_with(runs[0])

    def test_build_new_runs_all_nodes(self):
        self.job.all_nodes = True
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        node_count = 2
        self.job.node_pool.nodes = [mock.Mock()] * node_count
        runs = list(self.job.build_new_runs(run_time))

        assert_length(runs, node_count)
        for i in range(len(runs)):
            node = self.job.node_pool.nodes[i]
            assert_call(
                self.job.runs.build_new_run,
                i,
                self.job,
                run_time,
                node,
                manual=False,
            )

        calls = []
        for r in runs:
            calls.extend(r.mock_calls)
        self.job.watch.assert_has_calls(calls)

    def test_build_new_runs_manual(self):
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        runs = list(self.job.build_new_runs(run_time, manual=True))

        self.job.node_pool.next.assert_called_with()
        node = self.job.node_pool.next.return_value
        assert_length(runs, 1)
        assert_call(
            self.job.runs.build_new_run,
            0,
            self.job,
            run_time,
            node,
            manual=True,
        )
        self.job.watch.assert_called_with(runs[0])

    def test_handler(self):
        self.job.handler(None, jobrun.JobRun.NOTIFY_STATE_CHANGED)
        self.job.notify.assert_called_with(self.job.NOTIFY_STATE_CHANGE)

        self.job.handler(None, jobrun.JobRun.NOTIFY_DONE)
        self.job.notify.assert_called_with(self.job.NOTIFY_RUN_DONE)

    def test__eq__(self):
        other_job = job.Job("jobname", 'scheduler', eventbus_publish=lambda: None)
        assert not self.job == other_job
        other_job.update_from_job(self.job)
        assert_equal(self.job, other_job)

    def test__ne__(self):
        other_job = job.Job("jobname", 'scheduler', eventbus_publish=lambda: None)
        assert self.job != other_job
        other_job.update_from_job(self.job)
        assert not self.job != other_job

    def test__eq__true(self):
        action_runner = mock.Mock()
        first = job.Job("jobname", 'scheduler', eventbus_publish=lambda: None, action_runner=action_runner)
        second = job.Job("jobname", 'scheduler', eventbus_publish=lambda: None, action_runner=action_runner)
        assert_equal(first, second)

    def test__eq__false(self):
        first = job.Job("jobname", 'scheduler', eventbus_publish=lambda: None, action_runner=mock.Mock())
        second = job.Job("jobname", 'scheduler', eventbus_publish=lambda: None, action_runner=mock.Mock())
        assert_not_equal(first, second)


class TestJobScheduler(TestCase):
    @setup
    def setup_job(self):
        self.scheduler = scheduler.ConstantScheduler()
        mock_graph = mock.Mock(autospec=True)
        mock_graph.get_action_map.return_value = {}
        mock_graph.action_map = {}
        self.job = mock.Mock(autospec=True)
        self.job.allow_overlap = False
        self.job.max_runtime = datetime.timedelta(days=1)
        self.job_scheduler = job.JobScheduler(job=self.job)

    def test_restore_state_sets_job_runs(self):
        self.job.enabled = False
        mock_runs = [mock.Mock(), mock.Mock()]
        mock_action_runner = mock.Mock()
        job_state_data = {'runs': mock_runs, 'enabled': True}

        self.job_scheduler._set_callback = lambda x: x

        self.job.runs.runs = collections.deque()
        self.job.runs.get_scheduled.return_value = [mock.Mock()]
        self.job.get_job_runs_from_state.return_value = mock_runs

        with mock.patch(
            'tron.core.job.recovery.launch_recovery_actionruns_for_job_runs',
            autospec=True,
        ) as mock_launch_recovery:
            mock_launch_recovery.return_value = mock.Mock(autospec=True)
            self.job_scheduler.restore_state(
                job_state_data, mock_action_runner
            )
            assert self.job.runs.runs == collections.deque(mock_runs)
            mock_launch_recovery.assert_called_once_with(
                job_runs=mock_runs, master_action_runner=mock_action_runner
            )
            calls = [mock.call(mock_runs[i]) for i in range(0, len(mock_runs))]
            self.job.watch.assert_has_calls(calls)

    def test_disable(self):
        self.job_scheduler.disable()
        assert self.job_scheduler.job.enabled is False
        self.job_scheduler.job.runs.cancel_pending.assert_called_once()

    def test_schedule_reconfigured(self):
        self.job_scheduler.schedule_reconfigured()
        self.job.runs.remove_pending.assert_called_once()

    def test_run_job(self):
        self.job_scheduler.schedule = mock.Mock(autospec=True)
        self.scheduler.schedule_on_complete = False
        self.job.runs.get_active = lambda n: []
        job_run = mock.Mock(autospec=True)
        job_run.is_cancelled = False
        self.job_scheduler.run_job(job_run)
        assert job_run.start.called_once()
        assert self.job_scheduler.schedule.called_once()

    def test_run_job_job_disabled(self):
        self.job_scheduler.schedule = MagicMock()
        job_run = MagicMock()
        self.job.enabled = False
        self.job_scheduler.run_job(job_run)
        assert_length(self.job_scheduler.schedule.mock_calls, 0)
        assert_length(job_run.start.mock_calls, 0)
        assert_length(job_run.cancel.mock_calls, 1)

    def test_run_job_cancelled(self):
        self.job_scheduler.schedule = MagicMock()
        job_run = MagicMock(is_scheduled=False)
        self.job_scheduler.run_job(job_run)
        assert_length(job_run.start.mock_calls, 0)
        assert_length(self.job_scheduler.schedule.mock_calls, 1)

    def test_run_job_already_running_queuing(self):
        self.job_scheduler.schedule = mock.Mock(autospec=True)
        self.job.runs.get_active = lambda s: [mock.Mock(autospec=True)]
        job_run = mock.Mock(autospec=True)
        job_run.is_cancelled = False
        self.job_scheduler.run_job(job_run)
        assert not job_run.start.called
        job_run.queue.assert_called_once()
        assert not self.job_scheduler.schedule.called

    def test_run_job_already_running_cancel(self):
        self.job_scheduler.schedule = mock.Mock(autospec=True)
        self.job.runs.get_active = lambda s: [mock.Mock(autospec=True)]
        self.job.queueing = False
        job_run = mock.Mock(autospec=True)
        job_run.is_cancelled = False
        self.job_scheduler.run_job(job_run)
        assert not job_run.start.called
        job_run.cancel.assert_called_once()
        self.job_scheduler.schedule.assert_called_once()

    def test_run_job_already_running_allow_overlap(self):
        self.job_scheduler.schedule = mock.Mock()
        self.job.runs.get_active = lambda s: [mock.Mock()]
        self.job.allow_overlap = True
        job_run = MagicMock(is_cancelled=False)
        self.job_scheduler.run_job(job_run)
        job_run.start.assert_called_with()

    def test_run_job_has_starting_queueing(self):
        self.job_scheduler.schedule = mock.Mock(autospec=True)
        self.job.runs.get_active = lambda s: [mock.Mock(autospec=True)]
        job_run = mock.Mock(autospec=True)
        job_run.is_cancelled = False
        self.job_scheduler.run_job(job_run)
        assert not job_run.start.called
        job_run.queue.assert_called_once()
        assert not self.job_scheduler.schedule.called

    def test_run_job_schedule_on_complete(self):
        self.job_scheduler.schedule = MagicMock()
        self.scheduler.schedule_on_complete = True
        self.job.runs.get_active = lambda s: []
        job_run = MagicMock(is_cancelled=False)
        self.job_scheduler.run_job(job_run)
        assert_length(job_run.start.mock_calls, 1)
        assert_length(self.job_scheduler.schedule.mock_calls, 0)


class TestJobSchedulerGetRunsToSchedule(TestCase):
    @setup
    def setup_job(self):
        self.scheduler = mock.Mock()
        run_collection = mock.Mock(has_pending=False)
        node_pool = mock.Mock()
        self.job = job.Job(
            "jobname",
            self.scheduler,
            eventbus_publish=lambda: None,
            run_collection=run_collection,
            node_pool=node_pool,
        )
        self.job_scheduler = job.JobScheduler(self.job)
        self.job.runs.get_pending.return_value = False
        self.scheduler.queue_overlapping = True

    def test_get_runs_to_schedule_no_queue_with_pending(self):
        self.scheduler.queue_overlapping = False
        self.job.runs.has_pending = True
        job_runs = self.job_scheduler.get_runs_to_schedule(False)
        assert_length(job_runs, 0)

    def test_get_runs_to_schedule_queue_with_pending(self):
        job_runs = list(self.job_scheduler.get_runs_to_schedule(False))

        self.job.runs.get_newest.assert_called_with(include_manual=False)
        self.job.scheduler.next_run_time.assert_called_once_with(
            self.job.runs.get_newest.return_value.run_time,
        )
        assert_length(job_runs, 1)
        # This should return a JobRun which has the job attached as an observer
        job_runs[0].attach.assert_any_call(True, self.job)

    def test_get_runs_to_schedule_no_pending(self):
        job_runs = list(self.job_scheduler.get_runs_to_schedule(False))

        self.job.runs.get_newest.assert_called_with(include_manual=False)
        self.job.scheduler.next_run_time.assert_called_once_with(
            self.job.runs.get_newest.return_value.run_time,
        )
        assert_length(job_runs, 1)
        # This should return a JobRun which has the job attached as an observer
        job_runs[0].attach.assert_any_call(True, self.job)

    def test_get_runs_to_schedule_no_last_run(self):
        self.job.runs.get_newest.return_value = None

        job_runs = list(self.job_scheduler.get_runs_to_schedule(False))
        self.job.scheduler.next_run_time.assert_called_once_with(None)
        assert_length(job_runs, 1)
        # This should return a JobRun which has the job attached as an observer
        job_runs[0].attach.assert_any_call(True, self.job)

    def test_get_runs_to_schedule_ignore_last(self):
        job_runs = list(self.job_scheduler.get_runs_to_schedule(True))
        self.job.scheduler.next_run_time.assert_called_once_with(None)
        assert_length(job_runs, 1)
        self.scheduler.next_run_time.assert_called_once_with(None)


class JobSchedulerManualStartTestCase(testingutils.MockTimeTestCase):

    now = datetime.datetime.now()

    @setup
    def setup_job(self):
        self.scheduler = mock.Mock()
        run_collection = mock.Mock()
        node_pool = mock.Mock()
        self.job = job.Job(
            "jobname",
            self.scheduler,
            eventbus_publish=lambda: None,
            run_collection=run_collection,
            node_pool=node_pool,
        )
        self.job_scheduler = job.JobScheduler(self.job)
        self.manual_run = mock.Mock()
        self.job.build_new_runs = mock.Mock(return_value=[self.manual_run])

    def test_manual_start(self):
        manual_runs = self.job_scheduler.manual_start()

        self.job.build_new_runs.assert_called_with(self.now, manual=True)
        assert_length(manual_runs, 1)
        self.manual_run.start.assert_called_once_with()

    def test_manual_start_default_with_timezone(self):
        self.job.time_zone = mock.Mock()
        with mock.patch(
            'tron.core.job.timeutils.current_time',
            autospec=True,
        ) as mock_current:
            manual_runs = self.job_scheduler.manual_start()
            mock_current.assert_called_with(tz=self.job.time_zone)
            self.job.build_new_runs.assert_called_with(
                mock_current.return_value,
                manual=True,
            )
        assert_length(manual_runs, 1)
        self.manual_run.start.assert_called_once_with()

    def test_manual_start_with_run_time(self):
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        manual_runs = self.job_scheduler.manual_start(run_time)

        self.job.build_new_runs.assert_called_with(run_time, manual=True)
        assert_length(manual_runs, 1)
        self.manual_run.start.assert_called_once_with()


class TestJobSchedulerSchedule(TestCase):
    @setup
    def setup_job(self):
        self.scheduler = mock.Mock(autospec=True)
        self.scheduler.next_run_time.return_value = 0
        mock_run = mock.Mock()
        mock_run.seconds_until_run_time.return_value = 0
        run_collection = mock.Mock(
            has_pending=False,
            autospec=True,
            return_value=[mock_run],
        )
        mock_build_new_run = mock.Mock()
        run_collection.build_new_run.return_value = mock_build_new_run
        mock_build_new_run.seconds_until_run_time.return_value = 0
        node_pool = mock.Mock()
        self.job = job.Job(
            name="jobname",
            scheduler=self.scheduler,
            eventbus_publish=lambda: None,
            run_collection=run_collection,
            node_pool=node_pool,
        )
        self.job_scheduler = job.JobScheduler(self.job)
        self.original_build_new_runs = self.job.build_new_runs
        self.job.build_new_runs = mock.Mock(return_value=[mock_run])

    @setup_teardown
    def mock_eventloop(self):
        patcher = mock.patch('tron.core.job.eventloop', autospec=True)
        with patcher as self.eventloop:
            yield

    def test_enable(self):
        self.job.enabled = False
        self.job_scheduler.enable()
        assert self.job.enabled
        assert_length(self.eventloop.call_later.mock_calls, 1)

    def test_enable_noop(self):
        self.job.enabled = True
        self.job_scheduler.enable()
        assert self.job.enabled
        assert_length(self.eventloop.call_later.mock_calls, 0)

    def test_schedule(self):
        self.job.build_new_runs = self.original_build_new_runs
        self.job_scheduler.schedule()
        assert_length(self.eventloop.call_later.mock_calls, 1)

        # Args passed to callLater
        call_args = self.eventloop.call_later.mock_calls[0][1]
        assert_equal(call_args[1], self.job_scheduler.run_job)
        secs = call_args[0]
        run = call_args[2]

        run.seconds_until_run_time.assert_called_with()
        # Assert that we use the seconds we get from the run to schedule
        assert_equal(run.seconds_until_run_time.return_value, secs)

    def test_schedule_disabled_job(self):
        self.job.enabled = False
        self.job_scheduler.schedule()
        assert_length(self.eventloop.call_later.mock_calls, 0)

    def test_handle_job_events_no_schedule_on_complete(self):
        self.job_scheduler.run_job = mock.Mock()
        self.job.scheduler.schedule_on_complete = False
        queued_job_run = mock.Mock()
        self.job.runs.get_first_queued = lambda: queued_job_run
        self.job_scheduler.handle_job_events(self.job, job.Job.NOTIFY_RUN_DONE)
        self.eventloop.call_later.assert_any_call(
            0,
            self.job_scheduler.run_job,
            queued_job_run,
            run_queued=True,
        )

    def test_handle_job_events_schedule_on_complete(self):
        self.job_scheduler.schedule = mock.Mock()
        self.job.scheduler.schedule_on_complete = True
        self.job_scheduler.handle_job_events(self.job, job.Job.NOTIFY_RUN_DONE)
        self.job_scheduler.schedule.assert_called_with()

    def test_handler_unknown_event(self):
        self.job.runs.get_runs_by_state = mock.Mock()
        self.job_scheduler.handler(self.job, 'some_other_event')
        self.job.runs.get_runs_by_state.assert_not_called()

    def test_handler_no_queued(self):
        self.job_scheduler.run_job = mock.Mock()

        def get_queued(state):
            if state == ActionRun.STATE_QUEUED:
                return []

        self.job.runs.get_runs_by_state = get_queued
        self.job_scheduler.handler(self.job, job.Job.NOTIFY_RUN_DONE)
        self.job_scheduler.run_job.assert_not_called()

    def test_run_queue_schedule(self):
        with mock.patch.object(
            self.job_scheduler,
            'schedule',
        ) as mock_schedule:
            self.job_scheduler.run_job = mock.Mock()
            self.job.scheduler.schedule_on_complete = False
            queued_job_run = mock.Mock()
            self.job.runs.get_first_queued = lambda: queued_job_run
            self.job_scheduler.run_queue_schedule()
            self.eventloop.call_later.assert_called_once_with(
                0,
                self.job_scheduler.run_job,
                queued_job_run,
                run_queued=True,
            )
            mock_schedule.assert_called_once_with()


class TestJobSchedulerFactory(TestCase):
    @setup
    def setup_factory(self):
        self.context = mock.Mock()
        self.output_stream_dir = mock.Mock()
        self.time_zone = mock.Mock()
        self.action_runner = mock.create_autospec(
            actioncommand.SubprocessActionRunnerFactory,
        )
        self.eventbus_publish = lambda: None
        self.factory = job.JobSchedulerFactory(
            self.context,
            self.output_stream_dir,
            self.time_zone,
            self.action_runner,
            self.eventbus_publish,
        )

    def test_build(self):
        config = mock.Mock()
        with mock.patch('tron.core.job.Job', autospec=True) as mock_job:
            job_scheduler = self.factory.build(config)
            _, kwargs = mock_job.from_config.call_args
            assert_equal(kwargs['job_config'], config)
            assert_equal(
                job_scheduler.get_job(),
                mock_job.from_config.return_value,
            )
            assert_equal(kwargs['parent_context'], self.context)
            assert_equal(kwargs['output_path'].base, self.output_stream_dir)
            assert_equal(kwargs['action_runner'], self.action_runner)
            assert_equal(kwargs['eventbus_publish'], self.eventbus_publish)


class TestJobCollection(TestCase):
    @setup
    def setup_collection(self):
        self.collection = job.JobCollection()

    def test_load_from_config(self):
        autospec_method(self.collection.jobs.filter_by_name)
        autospec_method(self.collection.add)
        factory = mock.create_autospec(job.JobSchedulerFactory)
        job_configs = {'a': mock.Mock(), 'b': mock.Mock()}
        result = self.collection.load_from_config(job_configs, factory, True)
        result = list(result)
        self.collection.jobs.filter_by_name.assert_called_with(job_configs)
        expected_calls = [mock.call(v) for v in six.itervalues(job_configs)]
        assert_mock_calls(expected_calls, factory.build.mock_calls)
        assert_length(self.collection.add.mock_calls, len(job_configs) * 2)
        assert_length(result, len(job_configs))
        job_schedulers = [
            call[1][0] for call in self.collection.add.mock_calls[::2]
        ]
        for job_scheduler in job_schedulers:
            job_scheduler.schedule.assert_called_with()
            job_scheduler.get_job.assert_called_with()

    def test_update(self):
        mock_scheduler = mock.create_autospec(job.JobScheduler)
        existing_scheduler = mock.create_autospec(job.JobScheduler)
        autospec_method(
            self.collection.get_by_name,
            return_value=existing_scheduler,
        )
        assert self.collection.update(mock_scheduler)
        self.collection.get_by_name.assert_called_with(
            mock_scheduler.get_name(),
        )
        existing_scheduler.get_job().update_from_job.assert_called_with(
            mock_scheduler.get_job.return_value,
        )
        existing_scheduler.schedule_reconfigured.assert_called_with()


if __name__ == '__main__':
    run()
