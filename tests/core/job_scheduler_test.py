import datetime

import mock

from testifycompat import assert_equal
from testifycompat import setup
from testifycompat import TestCase
from tests import testingutils
from tests.assertions import assert_length
from tron import actioncommand
from tron.core import job
from tron.core.actionrun import ActionRun
from tron.core.job_scheduler import JobScheduler
from tron.core.job_scheduler import JobSchedulerFactory


class TestJobSchedulerGetRunsToSchedule(TestCase):
    @setup
    def setup_job(self):
        self.scheduler = mock.Mock()
        run_collection = mock.Mock(has_pending=False)
        node_pool = mock.Mock()
        self.job = job.Job(
            "jobname",
            self.scheduler,
            run_collection=run_collection,
            node_pool=node_pool,
        )
        self.job_scheduler = JobScheduler(self.job)
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
            run_collection=run_collection,
            node_pool=node_pool,
        )
        self.job_scheduler = JobScheduler(self.job)
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
            'tron.core.job_scheduler.timeutils.current_time',
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
            run_collection=run_collection,
            node_pool=node_pool,
        )
        self.job_scheduler = JobScheduler(self.job)
        self.original_build_new_runs = self.job.build_new_runs
        self.job.build_new_runs = mock.Mock(return_value=[mock_run])

    @mock.patch('tron.core.job_scheduler.reactor', autospec=True)
    def test_enable(self, reactor):
        self.job.enabled = False
        self.job_scheduler.enable()
        assert self.job.enabled
        assert_length(reactor.callLater.mock_calls, 1)

    @mock.patch('tron.core.job_scheduler.reactor', autospec=True)
    def test_enable_noop(self, reactor):
        self.job.enabled = True
        self.job_scheduler.enable()
        assert self.job.enabled
        assert_length(reactor.callLater.mock_calls, 0)

    @mock.patch('tron.core.job_scheduler.reactor', autospec=True)
    def test_schedule(self, reactor):
        self.job.build_new_runs = self.original_build_new_runs
        self.job_scheduler.schedule()
        assert reactor.callLater.call_count == 1

        # Args passed to callLater
        call_args = reactor.callLater.mock_calls[0][1]
        assert_equal(call_args[1], self.job_scheduler.run_job)
        secs = call_args[0]
        run = call_args[2]

        run.seconds_until_run_time.assert_called_with()
        # Assert that we use the seconds we get from the run to schedule
        assert_equal(run.seconds_until_run_time.return_value, secs)

    @mock.patch('tron.core.job_scheduler.reactor', autospec=True)
    def test_schedule_disabled_job(self, reactor):
        self.job.enabled = False
        self.job_scheduler.schedule()
        assert reactor.callLater.call_count == 0

    @mock.patch('tron.core.job_scheduler.reactor', autospec=True)
    def test_handle_job_events_no_schedule_on_complete(self, reactor):
        self.job_scheduler.run_job = mock.Mock()
        self.job.scheduler.schedule_on_complete = False
        queued_job_run = mock.Mock()
        self.job.runs.get_first_queued = lambda: queued_job_run
        self.job_scheduler.handle_job_events(self.job, job.Job.NOTIFY_RUN_DONE)
        reactor.callLater.assert_any_call(
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
            if state == ActionRun.QUEUED:
                return []

        self.job.runs.get_runs_by_state = get_queued
        self.job_scheduler.handler(self.job, job.Job.NOTIFY_RUN_DONE)
        self.job_scheduler.run_job.assert_not_called()

    @mock.patch('tron.core.job_scheduler.reactor', autospec=True)
    def test_run_queue_schedule(self, reactor):
        with mock.patch.object(
            self.job_scheduler,
            'schedule',
        ) as mock_schedule:
            self.job_scheduler.run_job = mock.Mock()
            self.job.scheduler.schedule_on_complete = False
            queued_job_run = mock.Mock()
            self.job.runs.get_first_queued = lambda: queued_job_run
            self.job_scheduler.run_queue_schedule()
            reactor.callLater.assert_called_once_with(
                0,
                self.job_scheduler.run_job,
                queued_job_run,
                run_queued=True,
            )
            mock_schedule.assert_called_once_with()


class TestJobSchedulerOther(TestCase):
    """ Test other JobScheduler functions """

    def _make_job_scheduler(self, job_name, enabled=True):
        scheduler = mock.Mock()
        run_collection = mock.Mock()
        node_pool = mock.Mock()
        new_job = job.Job(
            job_name,
            scheduler,
            run_collection=run_collection,
            node_pool=node_pool,
            enabled=enabled,
        )
        return new_job, JobScheduler(new_job)

    @setup
    def setup_job(self):
        self.job, self.job_scheduler = self._make_job_scheduler(
            'jobname', True
        )

    def test_disable(self):
        self.job.runs.cancel_pending = mock.Mock()

        self.job_scheduler.disable()

        assert not self.job.enabled
        assert self.job.runs.cancel_pending.call_count == 1

    def test_update_from_job_scheduler_disable(self):
        new_job, new_job_scheduler = self._make_job_scheduler('jobname', False)
        self.job.update_from_job = mock.Mock()
        self.job_scheduler.disable = mock.Mock()

        self.job_scheduler.update_from_job_scheduler(new_job_scheduler)

        assert self.job.update_from_job.call_args == mock.call(
            new_job_scheduler.get_job()
        )
        assert self.job_scheduler.disable.call_count == 1

    def test_update_from_job_scheduler_enable(self):
        new_job, new_job_scheduler = self._make_job_scheduler('jobname', True)
        self.job.update_from_job = mock.Mock()
        self.job.enabled = False
        self.job.config_enabled = False
        self.job_scheduler.enable = mock.Mock()

        self.job_scheduler.update_from_job_scheduler(new_job_scheduler)

        assert self.job.update_from_job.call_args == mock.call(
            new_job_scheduler.get_job()
        )
        assert self.job_scheduler.enable.call_count == 1

    def test_update_from_job_scheduler_no_config_change(self):
        new_job, new_job_scheduler = self._make_job_scheduler('jobname', True)
        self.job.enabled = False
        self.job.update_from_job = mock.Mock()
        self.job_scheduler.enable = mock.Mock()
        self.job_scheduler.disable = mock.Mock()

        self.job_scheduler.update_from_job_scheduler(new_job_scheduler)

        assert self.job.update_from_job.call_args == mock.call(
            new_job_scheduler.get_job()
        )
        assert self.job_scheduler.enable.call_count == 0
        assert self.job_scheduler.disable.call_count == 0
        assert self.job.config_enabled == new_job.config_enabled
        assert not self.job.enabled


class TestJobSchedulerFactory(TestCase):
    @setup
    def setup_factory(self):
        self.context = mock.Mock()
        self.output_stream_dir = mock.Mock()
        self.time_zone = mock.Mock()
        self.action_runner = mock.create_autospec(
            actioncommand.SubprocessActionRunnerFactory,
        )
        self.factory = JobSchedulerFactory(
            self.context,
            self.output_stream_dir,
            self.time_zone,
            self.action_runner,
        )

    def test_build(self):
        config = mock.Mock()
        with mock.patch(
            'tron.core.job_scheduler.Job', autospec=True
        ) as mock_job:
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
