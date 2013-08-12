import datetime
import mock
import contextlib

from testify import setup, teardown, TestCase, run, assert_equal
from testify import setup_teardown
from testify.assertions import assert_not_equal
from tests import mocks
from tests.assertions import assert_length, assert_mock_calls
from tests.testingutils import autospec_method
from tests import testingutils
from tron import node, event, actioncommand
from tron.core import job, jobrun
from tron.core.actionrun import ActionRun


class JobContainerTestCase(TestCase):

    @setup_teardown
    def setup_job(self):
        self.job_state = job.JobState(True, 'test')
        self.run_collection = mock.Mock()
        self.nodes = mock.create_autospec(node.NodePool)
        self.action_runner = mock.create_autospec(
            actioncommand.SubprocessActionRunnerFactory)
        self.job_scheduler = mock.Mock()
        self.watcher = mock.Mock()

        patch_node = mock.patch('tron.core.job.node.NodePoolRepository')
        patch_event = mock.patch('tron.core.job.event', autospec=True)
        with contextlib.nested(patch_node, patch_event) \
        as (self.mock_node_repo, self.mock_event):
            self.job = job.JobContainer("jobname", self.job_state,
                    self.run_collection, self.job_scheduler, self.watcher)
            self.job.event = mock.create_autospec(event.EventRecorder)
            yield

    def test_from_config(self):
        action = mock.Mock(name='first', command='doit', node=None, requires=[])
        job_config = mock.Mock(
            name='ajob',
            node='thenodepool',
            all_nodes=False,
            queueing=True,
            enabled=True,
            run_limit=20,
            actions={action.name: action},
            cleanup_action=None)
        factory = job.JobSchedulerFactory('test_job', 'tests/core', 'PST',
            self.action_runner)
        state_watcher = mock.Mock()
        new_job = job.JobContainer.from_config(
            job_config, factory, state_watcher)

        assert_equal(new_job.config, job_config)
        assert_equal(new_job.enabled, job_config.enabled)
        self.mock_node_repo.get_instance().get_by_name.assert_called_with(
            job_config.node)
        assert_equal(new_job.watcher, state_watcher)
        assert new_job.action_graph

    def test_update_from_job(self):
        job_state = mock.create_autospec(job.JobState)
        job_scheduler = mock.Mock()
        other_job = job.JobContainer('otherjob', job_state,
            self.run_collection, job_scheduler, self.watcher)
        self.job.update_from_job(other_job)
        assert_equal(self.job.name, 'otherjob')
        assert_equal(self.job.action_runner, other_job.action_runner)
        assert_equal(self.job.config, job_scheduler.config)
        assert_equal(self.job, other_job)
        self.job.event.ok.assert_called_with('reconfigured')

    def test_disable(self):
        self.job.disable()
        assert_equal(self.job.status, job.JobState.STATUS_DISABLED)
        self.job.job_runs.cancel_pending.assert_called_once_with()
        assert not self.job.enabled

    def test_enable(self):
        def state_in(state):
            return state in [ActionRun.STATE_SCHEDULED, ActionRun.STATE_QUEUED]

        with mock.patch.object(self.job.job_runs, 'get_run_by_state', side_effect=state_in):
            self.job.job_state.enabled = False
            self.job.enable()
            assert_equal(self.job.status, job.JobState.STATUS_ENABLED)
            self.job.job_scheduler.create_and_schedule_runs.assert_called_once_with(ignore_last_run_time=True)
            assert self.job.enabled

    def test_status_running(self):
        with mock.patch.object(self.job.job_runs, 'get_run_by_state',
        return_value=ActionRun.STATE_RUNNING):
            assert_equal(self.job.status, job.JobState.STATUS_RUNNING)

    def test_status_unknown(self):
        with mock.patch.object(self.job.job_runs, 'get_run_by_state',
        return_value=None):
            assert_equal(self.job.status, job.JobState.STATUS_UNKNOWN)

    def test_restore_state(self):
        run_data = [{'run_num': 1}, {'run_num': 2}]
        job_runs = [mock.Mock(), mock.Mock()]
        state_data = ({'enabled': False, 'run_ids': [1, 2]}, run_data)

        with contextlib.nested(
            mock.patch.object(self.job.job_runs, 'restore_state', return_value=job_runs),
            mock.patch.object(self.job.job_runs, 'get_run_numbers', return_value=state_data[0]['run_ids'])
        ):
            self.job.restore_state(state_data)

            assert not self.job.enabled
            calls = [mock.call(job_runs[i]) for i in xrange(len(job_runs))]
            self.job.watcher.watch.assert_has_calls(calls)
            calls = [mock.call(job_runs[i], jobrun.JobRun.NOTIFY_DONE) for i in xrange(len(job_runs))]
            self.job_scheduler.watch.assert_has_calls(calls)
            assert_equal(self.job.job_state.state_data, state_data[0])
            self.job.job_runs.restore_state.assert_called_once_with(
                sorted(run_data, key=lambda data: data['run_num'], reverse=True),
                self.job.action_graph,
                self.job.output_path.clone(),
                self.job.context,
                self.job.node_pool
            )
            self.job.job_runs.get_run_numbers.assert_called_once_with()
            self.job.job_scheduler.restore_state.assert_called_once_with()
            self.job.event.ok.assert_called_with('restored')

    def test__eq__(self):
        other_job = job.JobContainer("jobname",
            mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock())
        assert not self.job == other_job
        other_job.update_from_job(self.job)
        assert_equal(self.job, other_job)

    def test__ne__(self):
        other_job = job.JobContainer("jobname",
            mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock())
        assert self.job != other_job
        other_job.update_from_job(self.job)
        assert not self.job != other_job

    def test__eq__true(self):
        first = job.JobContainer("jobname", self.job_state,
            self.run_collection, self.job_scheduler, self.watcher)
        second = job.JobContainer("jobname", self.job_state,
            self.run_collection, self.job_scheduler, self.watcher)
        assert_equal(first, second)

    def test__eq__false(self):
        first = job.JobContainer("jobname",
            mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock())
        second = job.JobContainer("jobname",
            mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock())
        assert_not_equal(first, second)

class JobSchedulerBuildRunsTestCase(TestCase):

    @setup_teardown
    def setup_job(self):
        self.job_scheduler = job.JobScheduler(mock.Mock(), mock.Mock(),
            mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock(),
            mock.Mock(), mock.Mock(), mock.Mock())  # ...ew.
        self.job_scheduler.node_pool = node.NodePool(['one', 'two'],
            'test')
        self.job_scheduler.config.all_nodes = False
        self.job_scheduler.watch = mock.Mock()
        with mock.patch.object(self.job_scheduler.node_pool, 'next',
        return_value='one'):
            yield

    def test_build_new_runs(self):
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        self.job_scheduler.config.all_nodes = False
        with mock.patch.object(self.job_scheduler.job_runs, 'build_new_run',
        return_value=mock.Mock()):
            runs = list(self.job_scheduler.build_new_runs(run_time))

            self.job_scheduler.node_pool.next.assert_called_with()
            node = self.job_scheduler.node_pool.next.return_value
            self.job_scheduler.job_runs.build_new_run.assert_called_once_with(
                self.job_scheduler, run_time, node, manual=False)
            assert_length(runs, 1)
            self.job_scheduler.watcher.watch.assert_called_with(runs[0])
            self.job_scheduler.watch.assert_called_with(runs[0], 'notify_done')

    def test_build_new_runs_all_nodes(self):
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        self.job_scheduler.config.all_nodes = True
        node_count = 2
        self.job_scheduler.node_pool.nodes = [mock.Mock()] * node_count
        runs = list(self.job_scheduler.build_new_runs(run_time))

        assert_length(runs, node_count)
        for i in xrange(len(runs)):
            node = self.job_scheduler.node_pool.nodes[i]
            self.job_scheduler.job_runs.build_new_run.assert_called_with(
                    self.job_scheduler, run_time, node, manual=False)

        self.job_scheduler.watch.assert_has_calls(
            [mock.call(run, 'notify_done') for run in runs])

    def test_build_new_runs_manual(self):
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        self.job_scheduler.config.all_nodes = False
        runs = list(self.job_scheduler.build_new_runs(run_time, manual=True))

        self.job_scheduler.node_pool.next.assert_called_with()
        node = self.job_scheduler.node_pool.next.return_value
        assert_length(runs, 1)
        self.job_scheduler.job_runs.build_new_run.assert_called_once_with(
                self.job_scheduler, run_time, node, manual=True)
        self.job_scheduler.watch.assert_called_with(runs[0], 'notify_done')

class JobSchedulerContextTestCase(TestCase):
    @setup_teardown
    def setup_job(self):
        self.context = mock.Mock()
        with mock.patch('tron.command_context.build_context', autospec=True) \
        as self.build_patch:
            self.job_scheduler = job.JobScheduler(
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                self.context,
                mock.Mock(),
                mock.Mock()
            )
            yield

    def test_build_context(self):
        self.build_patch.assert_called_once_with(self.job_scheduler, self.context)

class JobSchedulerTestCase(TestCase):

    @setup
    def setup_job(self):
        self.scheduler = mock.Mock()
        self.job_scheduler = job.JobScheduler(
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                self.scheduler,
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock()
        )
        self.job_scheduler.job_state.enabled = True

    def test_restore_job_state(self):
        run_collection = mocks.MockJobRunCollection(get_scheduled=lambda: ['a'])
        self.job_scheduler.job_runs = run_collection
        self.job_scheduler._set_callback = mock.Mock()
        #state_data = 'state_data_token'
        with contextlib.nested(
            mock.patch.object(self.job_scheduler, 'schedule'),
            mock.patch.object(self.job_scheduler, '_set_callback')) \
        as (sched_patch, callback_patch):
            self.job_scheduler.restore_state()
            sched_patch.assert_called_once_with()
            callback_patch.assert_called_once_with('a')

    def test_schedule_reconfigured(self):
        autospec_method(self.job_scheduler.create_and_schedule_runs)
        self.job_scheduler.schedule_reconfigured()
        self.job_scheduler.job_runs.remove_pending.assert_called_once_with()
        self.job_scheduler.create_and_schedule_runs.assert_called_with(
            ignore_last_run_time=True)

    def test_run_job(self):
        with contextlib.nested(
            mock.patch.object(self.job_scheduler, 'schedule'),
            mock.patch.object(self.job_scheduler.job_runs, 'get_active', return_value=[]),
            mock.patch.object(self.job_scheduler, 'schedule_termination')) \
        as (schedule_patch, active_patch, stop_patch):
            self.scheduler.schedule_on_complete = False
            job_run = mock.Mock(is_cancelled=False)
            self.job_scheduler.run_job(job_run)
            job_run.start.assert_called_once_with()
            schedule_patch.assert_called_once_with()
            stop_patch.assert_called_once_with(job_run)

    def test_run_job_shutdown_requested(self):
        self.job_scheduler.shutdown_requested = True
        job_run = mock.Mock()
        with mock.patch.object(self.job_scheduler, 'schedule') as sched_patch:
            self.job_scheduler.run_job(job_run)
            assert not sched_patch.called
            assert not job_run.start.called
            assert not job_run.cancel.called

    def test_run_job_job_disabled(self):
        job_run = mock.Mock()
        self.job_scheduler.job_state.is_enabled = False
        with mock.patch.object(self.job_scheduler, 'schedule') as sched_patch:
            self.job_scheduler.run_job(job_run)
            assert not sched_patch.called
            assert not job_run.start.called
            job_run.cancel.assert_called_once_with()

    def test_run_job_cancelled(self):
        self.scheduler.schedule_on_complete = True
        job_run = mock.Mock(is_scheduled=False)
        with mock.patch.object(self.job_scheduler, 'schedule') as sched_patch:
            self.job_scheduler.run_job(job_run)
            assert not job_run.start.called
            sched_patch.assert_called_once_with()

    def test_run_job_already_running_queuing(self):
        self.job_scheduler.config.allow_overlap = False
        job_run = mock.Mock(is_cancelled=False)
        with contextlib.nested(
            mock.patch.object(self.job_scheduler, 'schedule'),
            mock.patch.object(self.job_scheduler.job_runs, 'get_active', return_value=['a'])) \
        as (schedule_patch, active_patch):
            self.job_scheduler.run_job(job_run)
            assert not job_run.start.called
            job_run.queue.assert_called_once_with()
            assert not schedule_patch.called

    def test_run_job_already_running_cancel(self):
        self.job_scheduler.config.queueing = False
        self.job_scheduler.config.allow_overlap = False
        job_run = mock.Mock(is_cancelled=False)
        with contextlib.nested(
            mock.patch.object(self.job_scheduler, 'schedule'),
            mock.patch.object(self.job_scheduler.job_runs, 'get_active', return_value=['a'])) \
        as (schedule_patch, active_patch):
            self.job_scheduler.run_job(job_run)
            assert not job_run.start.called
            job_run.cancel.assert_called_once_with()
            schedule_patch.assert_called_once_with()

    def test_run_job_already_running_allow_overlap(self):
        self.job_scheduler.config.allow_overlap = True
        job_run = mock.Mock(is_cancelled=False)
        with contextlib.nested(
            mock.patch.object(self.job_scheduler, 'schedule'),
            mock.patch.object(self.job_scheduler.job_runs, 'get_active', return_value=[]),
            mock.patch.object(self.job_scheduler, 'schedule_termination')) \
        as (schedule_patch, active_patch, stop_patch):
            self.job_scheduler.run_job(job_run)
            job_run.start.assert_called_once_with()
            stop_patch.assert_called_once_with(job_run)

    def test_run_job_schedule_on_complete(self):
        self.scheduler.schedule_on_complete = True
        job_run = mock.Mock(is_cancelled=False)
        with contextlib.nested(
            mock.patch.object(self.job_scheduler, 'schedule'),
            mock.patch.object(self.job_scheduler.job_runs, 'get_active', return_value=[]),
            mock.patch.object(self.job_scheduler, 'schedule_termination')) \
        as (schedule_patch, active_patch, stop_patch):
            self.job_scheduler.run_job(job_run)
            job_run.start.assert_called_once_with()
            assert not schedule_patch.called

class JobSchedulerGetRunsToScheduleTestCase(TestCase):

    @setup
    def setup_job(self):
        self.scheduler = mock.Mock()
        run_collection = mock.Mock(has_pending=False)
        self.job_scheduler = job.JobScheduler(
                mock.Mock(),
                run_collection,
                mock.Mock(),
                self.scheduler,
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock()
        )
        self.job_scheduler.job_state.enabled = True
        self.job_scheduler.job_runs.get_pending.return_value = False
        self.scheduler.queue_overlapping = True

    def test_get_runs_to_schedule_no_queue_with_pending(self):
        self.scheduler.queue_overlapping = False
        self.job_scheduler.job_runs.has_pending = True
        job_runs = self.job_scheduler.get_runs_to_schedule(False)
        assert_length(job_runs, 0)

    def test_get_runs_to_schedule_queue_no_pending(self):
        self.job_scheduler.job_runs.has_pending = False
        with contextlib.nested(
            mock.patch.object(self.job_scheduler, 'build_new_runs',
                return_value=[mock.Mock()]),
            mock.patch.object(self.job_scheduler.scheduler, 'next_run_time',
                return_value=130),
            mock.patch.object(self.job_scheduler.job_runs, 'get_newest',
                return_value = mock.Mock(run_time='test'))
        ) as (build_patch, next_patch, get_patch):
            job_runs = list(self.job_scheduler.get_runs_to_schedule(False))
            assert_length(job_runs, 1)
            get_patch.assert_called_once_with(include_manual=False)
            next_patch.assert_called_once_with('test')
            build_patch.assert_called_once_with(130)

    def test_get_runs_to_schedule_with_pending(self):
        self.job_scheduler.job_runs.has_pending = True
        job_runs = list(self.job_scheduler.get_runs_to_schedule(False))

        assert not self.job_scheduler.job_runs.get_newest.called
        assert not self.job_scheduler.scheduler.next_run_time.called
        assert_length(job_runs, 0)

    def test_get_runs_to_schedule_no_last_run(self):
        self.job_scheduler.job_runs.has_pending = False
        with mock.patch.object(self.job_scheduler.job_runs, 'get_newest',
        return_value=None) as get_patch:
            assert self.job_scheduler.get_runs_to_schedule(False)
            self.job_scheduler.scheduler.next_run_time.assert_called_once_with(None)
            get_patch.assert_called_once_with(include_manual=False)

    def test_get_runs_to_schedule_ignore_last(self):
        self.job_scheduler.job_runs.has_pending = False
        with contextlib.nested(
            mock.patch.object(self.job_scheduler.scheduler, 'next_run_time',
                return_value=None),
            mock.patch.object(self.job_scheduler, 'build_new_runs')
        ) as (next_patch, build_patch):
            self.job_scheduler.get_runs_to_schedule(True)
            assert not self.job_scheduler.job_runs.get_newest.called
            next_patch.assert_called_once_with(None)
            build_patch.assert_called_once_with(None)


class JobSchedulerManualStartTestCase(testingutils.MockTimeTestCase):

    now = datetime.datetime.now()

    @setup
    def setup_job(self):
        self.scheduler = mock.Mock()
        run_collection = mock.Mock(has_pending=False)
        self.job_scheduler = job.JobScheduler(
                mock.Mock(),
                run_collection,
                mock.Mock(),
                self.scheduler,
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock()
        )
        self.manual_run = mock.Mock()
        self.job_scheduler.build_new_runs = mock.Mock(return_value=[self.manual_run])

    def test_manual_start(self):
        manual_runs = self.job_scheduler.manual_start()

        self.job_scheduler.build_new_runs.assert_called_with(self.now, manual=True)
        assert_length(manual_runs, 1)
        self.manual_run.start.assert_called_once_with()

    def test_manual_start_with_run_time(self):
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        manual_runs = self.job_scheduler.manual_start(run_time)

        self.job_scheduler.build_new_runs.assert_called_with(run_time, manual=True)
        assert_length(manual_runs, 1)
        self.manual_run.start.assert_called_once_with()


class JobSchedulerScheduleTestCase(TestCase):

    @setup
    def setup_job(self):
        self.fake_job_run = mock.create_autospec(jobrun.JobRun)
        self.scheduler = mock.Mock()
        run_collection = mock.Mock(has_pending=False)
        self.job_scheduler = job.JobScheduler(
                mock.Mock(),
                run_collection,
                mock.Mock(),
                self.scheduler,
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock(),
                mock.Mock()
        )

    @setup_teardown
    def mock_eventloop(self):
        patcher = mock.patch('tron.core.job.eventloop', autospec=True)
        get_patch = mock.patch.object(self.job_scheduler, 'get_runs_to_schedule',
            return_value=[mock.Mock()])
        with contextlib.nested(patcher, get_patch) as (self.eventloop, self.get_patch):
            yield

    @teardown
    def teardown_job(self):
        event.EventManager.reset()

    def test_restore_state_scheduled(self):
        mock_scheduled = [mock.Mock(), mock.Mock()]
        with contextlib.nested(
            mock.patch.object(self.job_scheduler.job_runs, 'get_scheduled',
                return_value=iter(mock_scheduled)),
            mock.patch.object(self.job_scheduler, 'schedule'),
            mock.patch.object(self.job_scheduler, '_set_callback')
        ) as (get_patch, sched_patch, back_patch):
            self.job_scheduler.restore_state()
            get_patch.assert_called_once_with()
            calls = [mock.call(m) for m in mock_scheduled]
            back_patch.assert_has_calls(calls)
            sched_patch.assert_called_once_with()

    def test_restore_state_queued(self):
        queued = mock.Mock()
        with contextlib.nested(
            mock.patch.object(self.job_scheduler.job_runs, 'get_scheduled',
                return_value=iter([])),
            mock.patch.object(self.job_scheduler.job_runs, 'get_first_queued',
                return_value=queued),
            mock.patch.object(self.job_scheduler, 'schedule'),
            mock.patch.object(job.eventloop, 'call_later')
        ) as (get_patch, queue_patch, sched_patch, later_patch):
            self.job_scheduler.restore_state()
            get_patch.assert_called_once_with()
            later_patch.assert_called_once_with(0, self.job_scheduler.run_job, queued, run_queued=True)
            sched_patch.assert_called_once_with()

    def test_schedule(self):
        with mock.patch.object(self.job_scheduler.job_state, 'is_enabled',
        new=True):
            self.job_scheduler.job_state.enabled = True
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
        with mock.patch.object(self.job_scheduler.job_state, 'is_enabled',
        new=False):
            self.job_scheduler.schedule()
            assert_length(self.eventloop.call_later.mock_calls, 0)

    def test_handle_job_events_no_schedule_on_complete(self):
        queued_job_run = mock.Mock()
        with contextlib.nested(
            mock.patch.object(self.job_scheduler, 'run_job'),
            mock.patch.object(self.job_scheduler.job_runs, 'get_first_queued',
                return_value=queued_job_run)
        ) as (run_patch, queue_patch):
            self.job_scheduler.scheduler.schedule_on_complete = False
            self.job_scheduler.handle_job_events(self.fake_job_run, jobrun.JobRun.NOTIFY_DONE)
            self.eventloop.call_later.assert_any_call(0,
                self.job_scheduler.run_job, queued_job_run, run_queued=True)

    def test_handle_job_events_schedule_on_complete(self):
        with mock.patch.object(self.job_scheduler, 'schedule') as sched_patch:
            self.job_scheduler.scheduler.schedule_on_complete = True
            self.job_scheduler.handle_job_events(self.fake_job_run, jobrun.JobRun.NOTIFY_DONE)
            sched_patch.assert_called_with()

    def test_handler_unknown_event(self):
        with mock.patch.object(self.job_scheduler.job_runs,
        'get_runs_by_state') as runs_patch:
            self.job_scheduler.handler(self.fake_job_run, 'some_other_event')
            runs_patch.assert_not_called()

    def test_handler_no_queued(self):
        def get_queued(state):
            if state == ActionRun.STATE_QUEUED:
                return []
        with contextlib.nested(
            mock.patch.object(self.job_scheduler, 'run_job'),
            mock.patch.object(self.job_scheduler.job_runs, 'get_runs_by_state',
                side_effect=get_queued),
            mock.patch.object(self.job_scheduler.job_runs, 'get_first_queued',
                return_value=None)
        ) as (job_patch, runs_patch, queue_patch):
            self.job_scheduler.handler(self.fake_job_run, jobrun.JobRun.NOTIFY_DONE)
            job_patch.assert_not_called()
            queue_patch.assert_called_once_with()


class JobSchedulerFactoryTestCase(TestCase):

    @setup
    def setup_factory(self):
        self.context = mock.Mock()
        self.output_stream_dir = mock.Mock()
        self.time_zone = mock.Mock()
        self.action_runner = mock.create_autospec(
            actioncommand.SubprocessActionRunnerFactory)
        self.factory = job.JobSchedulerFactory(
            self.context, self.output_stream_dir, self.time_zone, self.action_runner)

    def test_build(self):
        config = mock.Mock()
        runs = mock.Mock()
        state = mock.Mock()
        graph = mock.Mock()
        nodes = mock.Mock()
        watcher = mock.Mock()
        with mock.patch('tron.core.job.JobScheduler', autospec=True) as mock_job:
            self.factory.build(config, runs, state, graph, nodes, watcher)
            args, _ = mock_job.call_args
            (job_runs,
            job_config,
            job_state,
            scheduler,
            action_graph,
            job_nodes,
            output_path,
            context,
            state_watcher,
            action_runner) = args
            assert_equal(job_config, config)
            assert_equal(job_runs, runs)
            assert_equal(job_state, state)
            assert_equal(action_graph, graph)
            assert_equal(job_nodes, nodes)
            assert_equal(watcher, state_watcher)
            assert_equal(output_path.base, self.output_stream_dir)
            assert_equal(action_runner, self.action_runner)


class JobCollectionTestCase(TestCase):

    @setup
    def setup_collection(self):
        self.collection = job.JobCollection()

    def test_load_from_config(self):
        autospec_method(self.collection.jobs.filter_by_name)
        autospec_method(self.collection.add)
        factory = mock.create_autospec(job.JobSchedulerFactory)
        job_configs = {'a': mock.Mock(), 'b': mock.Mock()}
        state_watcher = mock.Mock()
        with mock.patch('tron.core.job.JobContainer', autospec=True) as job_patch:
            result = self.collection.load_from_config(job_configs, factory, True, state_watcher)
            result = list(result)
            self.collection.jobs.filter_by_name.assert_called_with(job_configs)
            expected_calls = [mock.call(v, factory, state_watcher) for v in job_configs.itervalues()]
            assert_mock_calls(expected_calls, job_patch.from_config.mock_calls)
            assert_length(self.collection.add.mock_calls, len(job_configs) * 2)
            assert_length(result, len(job_configs))
            job_containers = [call[1][0] for call in self.collection.add.mock_calls[::2]]
            for job_container in job_containers:
                job_container.schedule.assert_called_with()

    def test_update(self):
        mock_scheduler = mock.create_autospec(job.JobContainer)
        existing_scheduler = mock.create_autospec(job.JobContainer)
        autospec_method(self.collection.get_by_name, return_value=existing_scheduler)
        existing_scheduler.schedule_reconfigured = mock.Mock()
        assert self.collection.update(mock_scheduler)
        self.collection.get_by_name.assert_called_with(mock_scheduler.get_name())
        existing_scheduler.update_from_job.assert_called_with(
            mock_scheduler)
        existing_scheduler.schedule_reconfigured.assert_called_with()

    def test_get_jobs_from_namespace(self):
        fake_job_uno = mock.create_autospec(job.JobContainer)
        fake_job_dos = mock.create_autospec(job.JobContainer)
        #fake_job_uno.namespace = 'uno'
        #fake_job_dos.namespace = 'dos'
        uno_patch = mock.patch.object(fake_job_uno, 'namespace', new='uno')
        dos_patch = mock.patch.object(fake_job_dos, 'namespace', new='dos')
        fake_jobs = [fake_job_uno, fake_job_dos]
        def fake_jobs_iter():
            return iter(fake_jobs)
        with contextlib.nested(
            mock.patch.object(self.collection.jobs, 'itervalues',
                side_effect=fake_jobs_iter),
            uno_patch,
            dos_patch
        ):
            assert_equal(self.collection.get_jobs_by_namespace('uno'),
                [fake_job_uno])
            assert_equal(self.collection.get_jobs_by_namespace('dos'),
                [fake_job_dos])


class JobStateTestCase(TestCase):

    @setup
    def setup_state(self):
        self.state = job.JobState(True, 'test_state')

    def test_enable(self):
        with mock.patch.object(self.state, 'notify') as notify_patch:
            self.state.enable()
            assert self.state.is_enabled
            notify_patch.assert_called_once_with(job.JobState.NOTIFY_STATUS_CHANGE)

    def test_disable(self):
        with mock.patch.object(self.state, 'notify') as notify_patch:
            self.state.disable()
            assert not self.state.is_enabled
            notify_patch.assert_called_once_with(job.JobState.NOTIFY_STATUS_CHANGE)

    def test_status_disabled(self):
        job_runs = mock.create_autospec(jobrun.JobRunCollection)
        with mock.patch.object(self.state, 'notify'):
            self.state.disable()
            assert_equal(self.state.status(job_runs), job.JobState.STATUS_DISABLED)
            self.state.enable()

    def test_status_running(self):
        job_runs = mock.create_autospec(jobrun.JobRunCollection)
        mock_true = mock.Mock(return_value=True)
        job_runs.get_run_by_state = mock_true
        assert_equal(self.state.status(job_runs), job.JobState.STATUS_RUNNING)
        mock_true.assert_called_with(ActionRun.STATE_RUNNING)

    def test_status_enabled_scheduled(self):
        job_runs = mock.create_autospec(jobrun.JobRunCollection)

        def mock_scheduled_func(state):
            return state == ActionRun.STATE_SCHEDULED
        mock_scheduled = mock.Mock(side_effect=mock_scheduled_func)

        job_runs.get_run_by_state = mock_scheduled
        assert_equal(self.state.status(job_runs), job.JobState.STATUS_ENABLED)
        mock_scheduled.assert_called_with(ActionRun.STATE_SCHEDULED)

    def test_status_enabled_queued(self):
        job_runs = mock.create_autospec(jobrun.JobRunCollection)

        def mock_queued_func(state):
            return state == ActionRun.STATE_QUEUED
        mock_queued = mock.Mock(side_effect=mock_queued_func)

        job_runs.get_run_by_state = mock_queued
        assert_equal(self.state.status(job_runs), job.JobState.STATUS_ENABLED)
        mock_queued.assert_called_with(ActionRun.STATE_QUEUED)

    def test_status_unknown(self):
        job_runs = mock.create_autospec(jobrun.JobRunCollection)
        mock_false = mock.Mock(return_value=False)
        job_runs.get_run_by_state = mock_false
        assert_equal(self.state.status(job_runs), job.JobState.STATUS_UNKNOWN)

    def test_restore_state(self):
        fake_state = job.JobState(False, 'test_state')
        fake_state.run_ids = [1, 2, 3, 4, 5]
        fake_state.restore_state(self.state.state_data)
        assert_equal(fake_state.is_enabled, self.state.is_enabled)
        assert_equal(fake_state.run_ids, self.state.run_ids)

    def test_state_data(self):
        fake_state = job.JobState(False, 'test_state')
        fake_state.run_ids = [1, 2, 3, 4, 5]
        assert_equal(fake_state.state_data['enabled'], False)
        assert_equal(fake_state.state_data['run_ids'], [1, 2, 3, 4, 5])
        assert_equal(len(fake_state.state_data), 2)

    def test_set_run_ids(self):
        with mock.patch.object(self.state, 'notify') as notify_patch:
            self.state.set_run_ids([1, 2, 3])
            assert_equal(self.state.run_ids, [1, 2, 3])
            notify_patch.assert_called_once_with(job.JobState.NOTIFY_STATUS_CHANGE)
            self.state.run_ids = []


if __name__ == '__main__':
    run()
