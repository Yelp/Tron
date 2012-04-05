import datetime

from testify import setup, teardown, TestCase, run, assert_equal, assert_raises
from tests.assertions import assert_length, assert_call
from tests.testingutils import MockReactorTestCase, Turtle, TestNode
from tron import node
from tron.core import job, jobrun
from tron.core.actionrun import ActionRun
from tron.utils import timeutils


class JobContextTestCase(TestCase):

    @setup
    def setup_job(self):
        self.last_success = datetime.datetime(2012, 3, 14)
        scheduler = Turtle()
        run_collection = Turtle(last_success=self.last_success)
        self.job = job.Job("jobname", scheduler, run_collection=run_collection)
        self.context = job.JobContext(self.job)

    def test_name(self):
        assert_equal(self.context.name, self.job.name)

    def test__getitem__last_success(self):
        item = self.context["last_success:day-1"]
        expected = (self.last_success - datetime.timedelta(days=1)).day
        assert_equal(item, str(expected))

        item = self.context["last_success:shortdate"]
        assert_equal(item, "2012-03-14")

    def test__getitem__last_success_bad_date_spec(self):
        name = "last_success:beers-3"
        assert_raises(KeyError, lambda: self.context[name])

    def test__getitem__last_success_bad_date_name(self):
        name = "first_success:shortdate-1"
        assert_raises(KeyError, lambda: self.context[name])

    def test__getitem__last_success_no_date_spec(self):
        name = "last_success"
        assert_raises(KeyError, lambda: self.context[name])

    def test__getitem__missing(self):
        assert_raises(KeyError, lambda: self.context['bogus'])


class JobTestCase(TestCase):

    @setup
    def setup_job(self):
        action_graph = Turtle(names=lambda: ['one', 'two'])
        scheduler = Turtle()
        run_collection = Turtle()
        self.nodes = [TestNode("box1"), TestNode("box0")]
        node_store = node.NodePoolStore.get_instance()
        node_store.put(Turtle(name="thenodepool",
                nodes=self.nodes))

        self.job = job.Job("jobname", scheduler,
                run_collection=run_collection, action_graph=action_graph,
                node_pool=node_store.get('thenodepool'))
        self.job.notify = Turtle()
        self.job.watch = Turtle()

    @teardown
    def teardown_job(self):
        node.NodePoolStore.get_instance().clear()

    def test__init__(self):
        assert str(self.job.output_path).endswith(self.job.name)

    def test_from_config(self):
        job_config = Turtle(
            name='ajob',
            node='thenodepool',
            all_nodes=False,
            queueing=True,
            enabled=True,
            run_limit=20,
            actions={
                'first': Turtle(
                    name='first', command='doit', node=None, requires=[])
            },
            cleanup_action=None,
        )
        scheduler = 'scheduler_token'
        parent_context = 'parent_context_token'
        output_path = ["base_path"]
        new_job = job.Job.from_config(
                job_config, scheduler, parent_context, output_path)

        assert_equal(new_job.scheduler, scheduler)
        assert_equal(new_job.context.next, parent_context)
        assert_equal(new_job.node_pool.nodes, self.nodes)
        assert_equal(new_job.enabled, True)
        assert new_job.action_graph

    def test_update_from_job(self):
        other_job = job.Job('otherjob', 'scheduler')
        self.job.update_from_job(other_job)
        assert_equal(self.job.name, 'otherjob')
        assert_equal(self.job.scheduler, 'scheduler')
        assert_call(self.job.notify, 0, self.job.EVENT_RECONFIGURED)

    def test_status_disabled(self):
        self.job.enabled = False
        assert_equal(self.job.status, self.job.STATUS_DISABLED)

    def test_status_enabled(self):
        def state_in(state):
            return state in [ActionRun.STATE_SCHEDULED, ActionRun.STATE_QUEUED]

        self.job.runs.get_run_by_state = state_in
        assert_equal(self.job.status, self.job.STATUS_ENABLED)

    def test_status_running(self):
        self.job.runs.get_run_by_state = lambda s: Turtle()
        assert_equal(self.job.status, self.job.STATUS_RUNNING)

    def test_status_unknown(self):
        self.job.runs.get_run_by_state = lambda s: None
        assert_equal(self.job.status, self.job.STATUS_UNKNOWN)

    def test_repr_data(self):
        repr_data = self.job.repr_data()
        assert_equal(repr_data['name'], self.job.name)
        assert_equal(repr_data['scheduler'], str(self.job.scheduler))
        assert_equal(repr_data['node_pool'], ["box1", "box0"])
        assert_equal(repr_data['status'], self.job.STATUS_RUNNING)

    def test_state_data(self):
        state_data = self.job.state_data
        assert_equal(state_data['runs'], self.job.runs.state_data)
        assert state_data['enabled']

    def test_restore_state(self):
        run_data = ['one', 'two']
        job_runs = [Turtle(), Turtle()]
        self.job.runs.restore_state = lambda r, a: job_runs
        state_data = {'enabled': False, 'runs': run_data}

        self.job.restore_state(state_data)

        assert not self.job.enabled
        for i in xrange(len(job_runs)):
            assert_call(self.job.watch, i, job_runs[i])
        assert_call(self.job.notify, 0, self.job.EVENT_STATE_RESTORED)

    def test_build_new_runs(self):
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        runs = list(self.job.build_new_runs(run_time))

        assert_call(self.job.node_pool.next, 0)
        node = self.job.node_pool.next.returns[0]
        assert_call(self.job.runs.build_new_run,
                0, self.job, run_time, node, manual=False)
        assert_length(runs, 1)
        assert_call(self.job.watch, 0, runs[0])

    def test_build_new_runs_all_nodes(self):
        self.job.all_nodes = True
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        runs = list(self.job.build_new_runs(run_time))

        assert_length(runs, 2)
        for i in xrange(len(runs)):
            node = self.job.node_pool.nodes[i]
            assert_call(self.job.runs.build_new_run,
                    i, self.job, run_time, node, manual=False)
            assert_call(self.job.watch, 1, runs[1])

    def test_build_new_runs_manual(self):
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        runs = list(self.job.build_new_runs(run_time, manual=True))

        assert_call(self.job.node_pool.next, 0)
        node = self.job.node_pool.next.returns[0]
        assert_length(runs, 1)
        assert_call(self.job.runs.build_new_run,
                0, self.job, run_time, node, manual=True)
        assert_call(self.job.watch, 0, runs[0])

    def test_watcher(self):
        self.job.watcher(None, jobrun.JobRun.NOTIFY_STATE_CHANGED)
        assert_call(self.job.notify, 0, self.job.NOTIFY_STATE_CHANGE)

        self.job.watcher(None, jobrun.JobRun.NOTIFY_DONE)
        assert_call(self.job.notify, 1, self.job.NOTIFY_RUN_DONE)

        self.job.watcher(None, jobrun.JobRun.NOTIFY_START_FAILED)
        assert_call(self.job.notify, 2, self.job.NOTIFY_RUN_DONE)

    def test__eq__(self):
        other_job = job.Job("jobname", 'scheduler')
        assert not self.job == other_job
        other_job.update_from_job(self.job)
        assert_equal(self.job, other_job)

    def test__ne__(self):
        other_job = job.Job("jobname", 'scheduler')
        assert self.job != other_job
        other_job.update_from_job(self.job)
        assert not self.job != other_job


class JobSchedulerTestCase(TestCase):

    @setup
    def setup_job(self):
        self.scheduler = Turtle()
        run_collection = Turtle()
        node_pool = Turtle()
        self.job = job.Job(
                "jobname",
                self.scheduler,
                run_collection=run_collection,
                node_pool=node_pool,
        )
        self.job_scheduler = job.JobScheduler(self.job)

    def test_enable(self):
        self.job_scheduler.schedule = Turtle()
        self.job_scheduler.enable()
        assert self.job.enabled
        assert_length(self.job_scheduler.schedule.calls, 1)

    def test_disable(self):
        self.job_scheduler.disable()
        assert not self.job.enabled
        assert_length(self.job.runs.cancel_pending.calls, 1)

    def test_schedule_reconfigured(self):
        self.job_scheduler.schedule = Turtle()
        self.job_scheduler.schedule_reconfigured()
        assert_length(self.job.runs.cancel_pending.calls, 1)
        assert_length(self.job_scheduler.schedule.calls, 1)

    def test_run_job(self):
        self.job_scheduler.schedule = Turtle()
        self.job.runs.get_run_by_state = lambda s: not ActionRun.STATE_RUNNING
        job_run = Turtle(is_cancelled=False)
        self.job_scheduler.run_job(job_run)
        assert_length(job_run.start.calls, 1)
        assert_length(self.job_scheduler.schedule.calls, 1)

    def test_run_job_job_disabled(self):
        self.job_scheduler.schedule = Turtle()
        job_run = Turtle()
        self.job.enabled = False
        self.job_scheduler.run_job(job_run)
        assert_length(self.job_scheduler.schedule.calls, 0)
        assert_length(job_run.start.calls, 0)
        assert_length(job_run.cancel.calls, 1)

    def test_run_job_cancelled(self):
        self.job_scheduler.schedule = Turtle()
        job_run = Turtle(is_scheduled=False)
        self.job_scheduler.run_job(job_run)
        assert_length(job_run.start.calls, 0)
        assert_length(self.job_scheduler.schedule.calls, 1)

    def test_run_job_already_running_queuing(self):
        self.job_scheduler.schedule = Turtle()
        self.job.runs.get_run_by_state = lambda s: Turtle()
        job_run = Turtle(is_cancelled=False)
        self.job_scheduler.run_job(job_run)
        assert_length(job_run.start.calls, 0)
        assert_length(job_run.queue.calls, 1)
        assert_length(self.job_scheduler.schedule.calls, 0)

    def test_run_job_already_running_cancel(self):
        self.job_scheduler.schedule = Turtle()
        self.job.runs.get_run_by_state = lambda s: Turtle()
        self.job.queueing = False
        job_run = Turtle(is_cancelled=False)
        self.job_scheduler.run_job(job_run)
        assert_length(job_run.start.calls, 0)
        assert_length(job_run.cancel.calls, 1)
        assert_length(self.job_scheduler.schedule.calls, 0)

    def test_watcher(self):
        self.job_scheduler.run_job = Turtle()
        queued_job_run = Turtle()
        def get_queued(state):
            if state == ActionRun.STATE_QUEUED:
                return [queued_job_run, queued_job_run]

        self.job.runs.get_runs_by_state = get_queued
        self.job_scheduler.watcher(self.job, job.Job.NOTIFY_RUN_DONE)
        assert_length(self.job_scheduler.run_job.calls, 2)

    def test_watcher_unknown_event(self):
        self.job.runs.get_runs_by_state = Turtle()
        self.job_scheduler.watcher(self.job, 'some_other_event')
        assert_length(self.job.runs.get_runs_by_state.calls, 0)

    def test_watcher_no_queued(self):
        self.job_scheduler.run_job = Turtle()
        def get_queued(state):
            if state == ActionRun.STATE_QUEUED:
                return []
        self.job.runs.get_runs_by_state = get_queued
        self.job_scheduler.watcher(self.job, job.Job.NOTIFY_RUN_DONE)
        assert_length(self.job_scheduler.run_job.calls, 0)

    def test_get_runs_to_schedule_no_queue_with_pending(self):
        self.scheduler.queue_overlapping = False
        self.job.runs.get_pending = lambda: True
        job_runs = self.job_scheduler.get_runs_to_schedule()
        assert_length(job_runs, 0)

    def test_get_runs_to_schedule_queue_with_pending(self):
        self.scheduler.queue_overlapping = True
        self.job.runs.get_pending = lambda: True

        job_runs = list(self.job_scheduler.get_runs_to_schedule())

        assert_call(self.job.runs.get_newest, 0, include_manual=False)
        assert_length(self.job.scheduler.next_run_time.calls, 1)
        assert_length(job_runs, 1)
        # This should return a JobRun which has the job attached as an observer
        assert_call(job_runs[0].attach, 0, True, self.job)

    def test_get_runs_to_schedule_no_pending(self):
        self.job.runs.get_pending = lambda: False
        job_runs = list(self.job_scheduler.get_runs_to_schedule())

        assert_call(self.job.runs.get_newest, 0, include_manual=False)
        assert_length(self.job.scheduler.next_run_time.calls, 1)
        assert_length(job_runs, 1)
        # This should return a JobRun which has the job attached as an observer
        assert_call(job_runs[0].attach, 0, True, self.job)

    def test_get_runs_to_schedule_no_last_run(self):
        self.job.runs.get_pending = lambda: False
        self.job.runs.get_newest = lambda **kwargs: None

        job_runs = list(self.job_scheduler.get_runs_to_schedule())
        assert_length(self.job.scheduler.next_run_time.calls, 1)
        assert_length(job_runs, 1)
        # This should return a JobRun which has the job attached as an observer
        assert_call(job_runs[0].attach, 0, True, self.job)


class MockRunBuilder(Turtle):
    def __call__(self, *args, **kwargs):
        super(MockRunBuilder, self).__call__(*args, **kwargs)
        return [self.manual_run]


class JobSchedulerManualStartTestCase(TestCase):

    @setup
    def setup_job(self):
        self.scheduler = Turtle()
        run_collection = Turtle()
        node_pool = Turtle()
        self.job = job.Job(
            "jobname",
            self.scheduler,
            run_collection=run_collection,
            node_pool=node_pool,
        )
        self.job_scheduler = job.JobScheduler(self.job)
        self.manual_run = Turtle()
        self.job.build_new_runs = MockRunBuilder(manual_run=self.manual_run)

        self.now = datetime.datetime.now()
        timeutils.override_current_time(self.now)

    @teardown
    def teardown_timeutils(self):
        timeutils.override_current_time(None)

    def test_manual_start(self):
        manual_runs = self.job_scheduler.manual_start()

        assert_call(self.job.build_new_runs, 0, self.now, manual=True)
        assert_length(manual_runs, 1)
        assert_length(self.manual_run.start.calls, 1)

    def test_manual_start_with_run_time(self):
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        manual_runs = self.job_scheduler.manual_start(run_time)

        assert_call(self.job.build_new_runs, 0, run_time, manual=True)
        assert_length(manual_runs, 1)
        assert_length(self.manual_run.start.calls, 1)


class JobSchedulerScheduleTestCase(MockReactorTestCase):

    module_to_mock = job

    @setup
    def setup_job(self):
        self.scheduler = Turtle()
        run_collection = Turtle()
        node_pool = Turtle()
        self.job = job.Job(
            "jobname",
            self.scheduler,
            run_collection=run_collection,
            node_pool=node_pool,
        )
        self.job_scheduler = job.JobScheduler(self.job)

    def test_schedule(self):
        self.job_scheduler.schedule()
        assert_length(self.reactor.callLater.calls, 1)

        # Args passed to callLater
        call_args = self.reactor.callLater.calls[0][0]
        assert_equal(call_args[1], self.job_scheduler.run_job)
        secs = call_args[0]
        run = call_args[2]

        assert_call(run.seconds_until_run_time, 0)
        # Assert that we use the seconds we get from the run to schedule
        assert_equal(run.seconds_until_run_time.returns[0], secs)

    def test_schedule_disabled_job(self):
        self.job.enabled = False
        self.job_scheduler.schedule()
        assert_length(self.reactor.callLater.calls, 0)


if __name__ == '__main__':
    run()
