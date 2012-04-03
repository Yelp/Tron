import datetime

from testify import setup, teardown, TestCase, run, assert_equal, assert_raises
from tests.assertions import assert_length, assert_call
from tests.testingutils import MockReactorTestCase, Turtle
from tron import node
from tron.core import action, job
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
        scheduler = Turtle()
        run_collection = Turtle()
        self.job = job.Job("jobname", scheduler, run_collection=run_collection)
        nodes = node.NodePoolStore.get_instance()
        nodes.put(Turtle(name="thenodepool", nodes=["box1", "box0"]))

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
        assert_equal(new_job.node_pool.nodes, ["box1", "box0"])
        assert_equal(new_job.enabled, True)
        assert new_job.action_graph

    def test_update_from_job(self):
        # TODO:
        self.job.topo_actions = []
        run_num = 4

        act1 = action.Action("Action Test1")
        act1.command = "test1"
        act1.job = self.job
        act1_id = ''.join([self.job.name, '.', str(run_num), '.', act1.name])

        act2 = action.Action("Action Test2")
        act2.command = "test2"
        act2.job = self.job

        act3 = action.Action("Action Test3")
        act3.command = "test3"
        act3.job = self.job
        act3_id = ''.join([self.job.name, '.', str(run_num), '.', act3.name])

        cact = action.Action(name="Test Cleanup Action")
        cact.command = "Test Cleanup Command"
        cact.job = self.job
        cact_id = ''.join([self.job.name, '.', str(run_num), '.', cact.name])

        self.job.topo_actions.append(act1)
        self.job.topo_actions.append(act2)
        self.job.topo_actions.append(act3)
        self.job.cleanup_action = cact

        # filter out Action Test2 from restored state. upon trond restart
        #   restore_main_run(state_data) should filter out actions for runs
        #   before the new action was introduced
        state_data = \
        {'end_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 234159),
         'run_num': run_num,
         'run_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 125149),
         'runs': [{'command': act1.command,
               'end_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 232693),
               'id': act1_id,
               'run_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 125149),
               'start_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 128291),
               'state': 'scheduled'},
              {'command': act3.command,
               'end_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 234116),
               'id': act3_id,
               'run_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 125149),
               'start_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 133002),
               'state': 'scheduled'}],
         'cleanup_run': {'command': cact.command,
               'end_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 234116),
               'id': cact_id,
               'run_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 125149),
               'start_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 133002),
               'state': 'scheduled'},
         'start_time': datetime.datetime(2010, 12, 13, 15, 32, 3, 128152)}


        job_run = self.job.restore_run(state_data)

        # act2 was filtered
        assert_length(job_run.action_runs, 2)
        assert_equal(job_run.action_runs[0].id, act1_id)
        assert_equal(job_run.action_runs[1].id, act3_id)
        assert_equal(job_run.action_runs_with_cleanup[2].id, cact_id)

    def test_build_new_runs_all_nodes(self):
        # TODO
        self.job.all_nodes = True
        self.job.node_pool = Turtle(nodes=[Turtle(),
                                           Turtle(),
                                           Turtle()])
        runs = self.job.build_runs(None)

        assert_length(runs, 3)

        assert_equal(runs[0].node, self.job.node_pool.nodes[0])
        assert_equal(runs[1].node, self.job.node_pool.nodes[1])
        assert_equal(runs[2].node, self.job.node_pool.nodes[2])


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

    def test_run_job_already_running(self):
        self.job_scheduler.schedule = Turtle()
        self.job.runs.get_run_by_state = lambda s: Turtle()
        job_run = Turtle(is_cancelled=False)
        self.job_scheduler.run_job(job_run)
        assert_length(job_run.start.calls, 0)
        assert_length(job_run.queue.calls, 1)
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


if __name__ == '__main__':
    run()
