import datetime
import pytz
from testify import TestCase, setup, assert_equal, teardown
from testify.assertions import assert_in
from tests.assertions import assert_length, assert_raises, assert_call
from tron.core import jobrun, actionrun
from tests.testingutils import Turtle, TestNode
from tron.utils import timeutils


class JobRunContextTestCase(TestCase):

    @setup
    def setup_context(self):
        self.jobrun = Turtle()
        self.context = jobrun.JobRunContext(self.jobrun)

    def test_cleanup_job_status(self):
        self.jobrun.action_runs.is_failed = False
        self.jobrun.action_runs.is_complete = True
        assert_equal(self.context.cleanup_job_status, 'SUCCESS')


class JobRunTestCase(TestCase):

    @setup
    def setup_jobrun(self):
        self.action_graph = Turtle(action_map=dict(anaction=Turtle()))
        self.job = Turtle(name="aname", action_graph=self.action_graph)
        self.run_time = datetime.datetime(2012, 3, 14, 15, 9 ,26)
        node = TestNode('thenode')
        self.job_run = jobrun.JobRun('jobname', 7, self.run_time, node,
                action_runs=Turtle(
                    action_runs_with_cleanup=[],
                    get_startable_action_runs=lambda: [],
                    is_running=False
                ))
        self.job_run.watch = Turtle()
        self.job_run.notify = Turtle()

    @teardown
    def teardown_jobrun(self):
        timeutils.override_current_time(None)

    def test__init__(self):
        assert_equal(self.job_run.job_name, 'jobname')
        assert_equal(self.job_run.run_time, self.run_time)
        assert str(self.job_run.output_path).endswith(self.job_run.id)

    def test_for_job(self):
        run_num = 6
        node = TestNode('anode')
        run = jobrun.JobRun.for_job(
                self.job, run_num, self.run_time, node, False)

        assert run.action_runs.run_map
        assert_equal(run.job_name, self.job.name)
        assert_equal(run.run_num, run_num)
        assert_equal(run.node, node)
        assert not run.manual

    def test_for_job_manual(self):
        run_num = 6
        node = TestNode('anode')
        run = jobrun.JobRun.for_job(
                self.job, run_num, self.run_time, node, True)
        assert run.action_runs.run_map
        assert run.manual

    def test_state_data(self):
        state_data = self.job_run.state_data
        assert_equal(state_data['run_num'], 7)
        assert not state_data['manual']
        assert_equal(state_data['run_time'], self.run_time)

    def test_register_action_runs(self):
        self.job_run.action_runs = None
        action_runs = [Turtle(), Turtle()]
        run_collection = Turtle(action_runs_with_cleanup=action_runs)
        self.job_run.register_action_runs(run_collection)
        assert_length(self.job_run.watch.calls, 2)
        for i in xrange(2):
            assert_call(self.job_run.watch, i, action_runs[i])
        assert_equal(self.job_run.action_runs, run_collection)
        assert self.job_run.action_runs_proxy

    def test_register_action_runs_none(self):
        self.job_run.action_runs = None
        run_collection = Turtle(action_runs_with_cleanup=[])
        self.job_run.register_action_runs(run_collection)
        assert_length(self.job_run.watch.calls, 0)
        assert_equal(self.job_run.action_runs, run_collection)

    def test_register_action_duplicate(self):
        run_collection = Turtle(action_runs_with_cleanup=[])
        assert_raises(ValueError,
                self.job_run.register_action_runs, run_collection)

    def test_seconds_until_run_time(self):
        now = datetime.datetime(2012, 3, 14, 15, 9, 20)
        timeutils.override_current_time(now)
        seconds = self.job_run.seconds_until_run_time()
        assert_equal(seconds, 6)

    def test_seconds_until_run_time_with_tz(self):
        self.job_run.run_time = self.run_time.replace(tzinfo=pytz.utc)
        now = datetime.datetime(2012, 3, 14, 15, 9, 20)
        timeutils.override_current_time(now)
        seconds = self.job_run.seconds_until_run_time()
        assert_equal(seconds, 6)

    def test_start(self):
        self.job_run._do_start = Turtle()

        assert self.job_run.start()
        assert_call(self.job_run.notify, 0, self.job_run.EVENT_START)
        assert_call(self.job_run._do_start, 0)
        assert_length(self.job_run.notify.calls, 1)

    def test_start_failed(self):
        self.job_run._do_start = lambda: False

        assert not self.job_run.start()
        assert_call(self.job_run.notify, 0, self.job_run.EVENT_START)
        assert_call(self.job_run.notify, 1, self.job_run.NOTIFY_START_FAILED)
        assert_length(self.job_run.notify.calls, 2)

    def test_start_no_startable_action_runs(self):
        self.job_run._do_start = Turtle()
        self.job_run.action_runs.has_startable_action_runs = False

        assert not self.job_run.start()
        assert_call(self.job_run.notify, 0, self.job_run.EVENT_START)
        assert_call(self.job_run.notify, 1, self.job_run.NOTIFY_START_FAILED)
        assert_length(self.job_run.notify.calls, 2)

    def test_do_start(self):
        timeutils.override_current_time(self.run_time)
        startable_runs = [Turtle(), Turtle(), Turtle()]
        self.job_run.action_runs.get_startable_action_runs = lambda: startable_runs

        assert self.job_run._do_start()
        assert_equal(self.job_run.start_time, self.run_time)
        assert_call(self.job_run.action_runs.ready, 0)
        for i, startable_run in enumerate(startable_runs):
            assert_call(startable_run.start, 0)

        assert_length(self.job_run.notify.calls, 1)
        assert_call(self.job_run.notify, 0, self.job_run.EVENT_STARTED)

    def test_do_start_all_failed(self):
        timeutils.override_current_time(self.run_time)
        self.job_run._start_action_runs = lambda: [None]

        assert not self.job_run._do_start()
        assert_equal(self.job_run.start_time, self.run_time)
        assert_length(self.job_run.notify.calls, 0)

    def test_do_start_some_failed(self):
        timeutils.override_current_time(self.run_time)
        self.job_run._start_action_runs = lambda: [True, None]

        assert self.job_run._do_start()
        assert_equal(self.job_run.start_time, self.run_time)
        assert_length(self.job_run.notify.calls, 1)
        assert_call(self.job_run.notify, 0, self.job_run.EVENT_STARTED)

    def test_do_start_no_runs(self):
        assert not self.job_run._do_start()

    def test_start_action_runs(self):
        startable_runs = [Turtle(), Turtle(), Turtle()]
        self.job_run.action_runs.get_startable_action_runs = lambda: startable_runs

        started_runs = self.job_run._start_action_runs()
        assert_equal(started_runs, startable_runs)

    def test_start_action_runs_failed(self):
        def failing():
            raise actionrun.Error()
        startable_runs = [Turtle(start=failing), Turtle(), Turtle()]
        self.job_run.action_runs.get_startable_action_runs = lambda: startable_runs

        started_runs = self.job_run._start_action_runs()
        assert_equal(started_runs, startable_runs[1:])

    def test_start_action_runs_all_failed(self):
        def failing():
            raise actionrun.Error()
        startable_runs = [Turtle(start=failing), Turtle(start=failing)]
        self.job_run.action_runs.get_startable_action_runs = lambda: startable_runs

        started_runs = self.job_run._start_action_runs()
        assert_equal(started_runs, [])

    def test_watcher_not_end_state_event(self):
        self.job_run.finalize = Turtle()
        self.job_run.watcher(None, actionrun.ActionRun.STATE_STARTING)
        assert_length(self.job_run.finalize.calls, 0)

    def test_watcher_with_startable(self):
        self.job_run.action_runs.get_startable_action_runs = lambda: True
        startable_run = Turtle()
        self.job_run.action_runs.get_startable_action_runs = lambda: [startable_run]
        self.job_run.finalize = Turtle()

        self.job_run.watcher(None, actionrun.ActionRun.STATE_SUCCEEDED)
        assert_call(self.job_run.notify, 0, self.job_run.NOTIFY_STATE_CHANGED)
        assert_call(startable_run.start, 0)
        assert_length(self.job_run.finalize.calls, 0)

    def test_watcher_not_done(self):
        self.job_run.action_runs.is_running = True
        self.job_run._start_action_runs = lambda: []
        self.job_run.finalize = Turtle()

        self.job_run.watcher(None, actionrun.ActionRun.STATE_SUCCEEDED)
        assert_length(self.job_run.finalize.calls, 0)

    def test_watcher_finished_without_cleanup(self):
        self.job_run.action_runs.cleanup_action_run = None
        self.job_run.finalize = Turtle()

        self.job_run.watcher(None, actionrun.ActionRun.STATE_SUCCEEDED)
        assert_call(self.job_run.finalize, 0)

    def test_watcher_finished_with_cleanup_done(self):
        self.job_run.action_runs.cleanup_action_run = Turtle(is_done=True)
        self.job_run.finalize = Turtle()

        self.job_run.watcher(None, actionrun.ActionRun.STATE_SUCCEEDED)
        assert_call(self.job_run.finalize, 0)

    def test_watcher_finished_with_cleanup(self):
        self.job_run.action_runs.cleanup_action_run = Turtle(is_done=False)
        self.job_run.finalize = Turtle()

        self.job_run.watcher(None, actionrun.ActionRun.STATE_SUCCEEDED)
        assert_length(self.job_run.finalize.calls, 0)
        assert_call(self.job_run.action_runs.cleanup_action_run.start, 0)

    def test_state(self):
        assert_equal(self.job_run.state, actionrun.ActionRun.STATE_SUCCEEDED)

    def test_finalize(self):
        timeutils.override_current_time(self.run_time)
        self.job_run.action_runs.is_failed = False
        self.job_run.finalize()
        assert_call(self.job_run.notify, 0, self.job_run.EVENT_SUCCEEDED)
        assert_call(self.job_run.notify, 1, self.job_run.NOTIFY_DONE)
        assert_equal(self.job_run.end_time, self.run_time)

    def test_finalize_failure(self):
        timeutils.override_current_time(self.run_time)
        self.job_run.finalize()
        assert_call(self.job_run.notify, 0, self.job_run.EVENT_FAILED)
        assert_call(self.job_run.notify, 1, self.job_run.NOTIFY_DONE)
        assert_equal(self.job_run.end_time, self.run_time)

    def test_cleanup(self):
        self.job_run.clear_watchers = Turtle()
        self.job_run.output_path = Turtle()
        self.job_run.cleanup()

        assert_call(self.job_run.clear_watchers, 0)
        assert_call(self.job_run.output_path.delete, 0)
        assert not self.job_run.node
        assert not self.job_run.action_graph
        assert not self.job_run.action_runs

    def test__getattr__(self):
        assert self.job_run.cancel
        assert self.job_run.is_queued
        assert self.job_run.is_succeeded

    def test__getattr__miss(self):
        assert_raises(AttributeError, lambda: self.job_run.bogus)


class JobRunFromStateTestCase(TestCase):

    @setup
    def setup_jobrun(self):
        self.action_graph = Turtle(action_map=dict(anaction=Turtle()))
        self.run_time = datetime.datetime(2012, 3, 14, 15, 9 ,26)
        self.path = ['base', 'path']
        self.output_path = Turtle(clone=lambda: self.path)
        self.action_run_state_data = [{
            'job_run_id':       'thejobname.22',
            'action_name':      'blingaction',
            'state':            'succeeded',
            'run_time':         'sometime',
            'start_time':       'sometime',
            'end_time':         'sometime',
            'command':          'doit',
            'node_name':        'thenode'
        }]
        self.state_data = {
            'job_name':         'thejobname',
            'run_num':          22,
            'run_time':         self.run_time,
            'node_name':        'thebox',
            'end_time':         'the_end',
            'start_time':       'start_time',
            'runs':             self.action_run_state_data,
            'cleanup_run':      None,
            'manual':           True
        }

    def test_from_state(self):
        run = jobrun.JobRun.from_state(
                self.state_data, self.action_graph, self.output_path)
        assert_length(run.action_runs.run_map, 1)
        assert_equal(run.job_name, self.state_data['job_name'])
        assert_equal(run.run_time, self.run_time)
        assert run.manual
        assert_equal(run.output_path, self.output_path)

    def test_from_state_old_state_data(self):
        del self.state_data['manual']
        del self.state_data['job_name']
        del self.state_data['node_name']
        self.state_data['id'] = 'thejobname.22'

        run = jobrun.JobRun.from_state(
                self.state_data, self.action_graph, self.output_path)
        assert_length(run.action_runs.run_map, 1)
        assert_equal(run.job_name, 'thejobname')
        assert_equal(run.run_time, self.run_time)
        assert not run.manual
        assert not run.node


class MockJobRun(Turtle):

    manual = False

    @property
    def is_scheduled(self):
        return self.state == actionrun.ActionRun.STATE_SCHEDULED

    @property
    def is_queued(self):
        return self.state == actionrun.ActionRun.STATE_QUEUED

    @property
    def is_running(self):
        return self.state == actionrun.ActionRun.STATE_RUNNING

    def __repr__(self):
        return str(self.__dict__)


class JobRunCollectionTestCase(TestCase):

    def _mock_run(self, **kwargs):
        return MockJobRun(**kwargs)

    @setup
    def setup_runs(self):
        self.run_collection = jobrun.JobRunCollection(5)
        self.job_runs = [
            self._mock_run(state=actionrun.ActionRun.STATE_QUEUED, run_num=4),
            self._mock_run(state=actionrun.ActionRun.STATE_RUNNING, run_num=3)
        ] + [
            self._mock_run(state=actionrun.ActionRun.STATE_SUCCEEDED, run_num=i)
            for i in xrange(2,0,-1)
        ]
        self.run_collection.runs.extend(self.job_runs)

    def test__init__(self):
        assert_equal(self.run_collection.run_limit, 5)

    def test_from_config(self):
        job_config = Turtle(run_limit=20)
        runs = jobrun.JobRunCollection.from_config(job_config)
        assert_equal(runs.run_limit, 20)

    def test_restore_state(self):
        run_collection = jobrun.JobRunCollection(20)
        state_data = [
            dict(
                run_num=i,
                job_name="thename",
                run_time="sometime",
                start_time="start_time",
                end_time="sometime",
                cleanup_run=None,
                runs=[]
            ) for i in xrange(3,-1,-1)
        ]
        action_graph = [Turtle()]
        output_path = Turtle()

        restored_runs = run_collection.restore_state(
                state_data, action_graph, output_path)
        assert_equal(run_collection.runs[0].run_num, 3)
        assert_equal(run_collection.runs[3].run_num, 0)
        assert_length(restored_runs, 4)

    def test_restore_state_with_runs(self):
        assert_raises(ValueError,
                self.run_collection.restore_state, None, None, None)

    def test_build_new_run(self):
        self.run_collection.remove_old_runs = Turtle()
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        job = Turtle(name="thejob")
        job.action_graph.action_map = {}
        node = TestNode("thenode")
        job_run = self.run_collection.build_new_run(job, run_time, node)
        assert_in(job_run, self.run_collection.runs)
        assert_call(self.run_collection.remove_old_runs, 0)
        assert_equal(job_run.run_num, 5)
        assert_equal(job_run.job_name, "thejob")

    def test_build_new_run_manual(self):
        self.run_collection.remove_old_runs = Turtle()
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        job = Turtle(name="thejob")
        job.action_graph.action_map = {}
        node = TestNode("thenode")
        job_run = self.run_collection.build_new_run(job, run_time, node, True)
        assert_in(job_run, self.run_collection.runs)
        assert_call(self.run_collection.remove_old_runs, 0)
        assert_equal(job_run.run_num, 5)
        assert job_run.manual

    def test_cancel_pending(self):
        pending_runs = [Turtle(), Turtle()]
        self.run_collection.get_pending = lambda: pending_runs
        self.run_collection.cancel_pending()
        for i in xrange(len(pending_runs)):
            assert_call(pending_runs[i].cancel, 0)

    def test_cancel_pending_no_pending(self):
        self.run_collection.get_pending = lambda: []
        self.run_collection.cancel_pending()

    def test_remove_pending(self):
        self.run_collection.remove_pending()
        assert_length(self.run_collection.runs, 3)
        assert_equal(self.run_collection.runs[0], self.job_runs[1])
        assert_call(self.job_runs[0].cancel, 0)
        assert_call(self.job_runs[0].cleanup, 0)

    def test_get_run_by_state(self):
        state = actionrun.ActionRun.STATE_SUCCEEDED
        run = self.run_collection.get_run_by_state(state)
        assert_equal(run, self.job_runs[2])

    def test_get_run_by_state_no_match(self):
        state = actionrun.ActionRun.STATE_UNKNOWN
        run = self.run_collection.get_run_by_state(state)
        assert_equal(run, None)

    def test_get_runs_by_state(self):
        state = actionrun.ActionRun.STATE_QUEUED
        runs = self.run_collection.get_runs_by_state(state)
        assert_equal(list(runs), self.job_runs[:1])

    def test_get_runs_by_state_no_match(self):
        state = actionrun.ActionRun.STATE_UNKNOWN
        runs = self.run_collection.get_runs_by_state(state)
        assert_equal(list(runs), [])

    def test_get_run_by_num(self):
        run = self.run_collection.get_run_by_num(1)
        assert_equal(run.run_num, 1)

    def test_get_run_by_num_no_match(self):
        run = self.run_collection.get_run_by_num(7)
        assert_equal(run, None)

    def test_get_run_by_state_short_name(self):
        run = self.run_collection.get_run_by_state_short_name("RUNN")
        assert_equal(run, self.job_runs[1])

    def test_get_run_by_state_short_name_no_match(self):
        run = self.run_collection.get_run_by_state_short_name("FAIL")
        assert_equal(run, None)

    def test_get_newest(self):
        run = self.run_collection.get_newest()
        assert_equal(run, self.job_runs[0])

    def test_get_newest_exclude_manual(self):
        run = self._mock_run(
                state=actionrun.ActionRun.STATE_RUNNING, run_num=5, manual=True)
        self.job_runs.insert(0, run)
        newest_run = self.run_collection.get_newest(include_manual=False)
        assert_equal(newest_run, self.job_runs[1])

    def test_get_newest_no_runs(self):
        run_collection = jobrun.JobRunCollection(5)
        assert_equal(run_collection.get_newest(), None)

    def test_pending(self):
        run_num = self.run_collection.next_run_num()
        scheduled_run = self._mock_run(
                run_num=run_num,
                state=actionrun.ActionRun.STATE_SCHEDULED)
        self.run_collection.runs.appendleft(scheduled_run)
        pending = list(self.run_collection.get_pending())
        assert_length(pending, 2)
        assert_equal(pending, [scheduled_run, self.job_runs[0]])

    def test_get_next_to_finish(self):
        next_run = self.run_collection.get_next_to_finish()
        assert_equal(next_run, self.job_runs[1])

    def test_get_next_to_finish_by_node(self):
        self.job_runs[1].node = "seven"
        scheduled_run = self._mock_run(
            run_num=self.run_collection.next_run_num(),
            state=actionrun.ActionRun.STATE_SCHEDULED,
            node="nine")
        self.run_collection.runs.appendleft(scheduled_run)

        next_run = self.run_collection.get_next_to_finish(node="seven")
        assert_equal(next_run, self.job_runs[1])

    def test_get_next_to_finish_none(self):
        next_run = self.run_collection.get_next_to_finish(node="seven")
        assert_equal(next_run, None)

        self.job_runs[1].state = None
        next_run = self.run_collection.get_next_to_finish()
        assert_equal(next_run, None)

    def test_get_next_run_num(self):
        assert_equal(self.run_collection.next_run_num(), 5)

    def test_get_next_run_num_first(self):
        run_collection = jobrun.JobRunCollection(5)
        assert_equal(run_collection.next_run_num(), 0)

    def test_remove_old_runs(self):
        self.run_collection.run_limit = 1
        self.run_collection.remove_old_runs()

        assert_length(self.run_collection.runs, 3)
        assert_call(self.job_runs[-1].cleanup, 0)
        for job_run in self.run_collection.runs:
            assert_length(job_run.cancel.calls, 0)

    def test_remove_old_runs_none(self):
        self.run_collection.remove_old_runs()
        for job_run in self.job_runs:
            assert_length(job_run.cancel.calls, 0)

    def test_remove_old_runs_no_runs(self):
        run_collection = jobrun.JobRunCollection(4)
        run_collection.remove_old_runs()

    def test_state_data(self):
        assert_length(self.run_collection.state_data, len(self.job_runs))

    def test_last_success(self):
        assert_equal(self.run_collection.last_success, self.job_runs[2])

    def test__str__(self):
        expected = "JobRunCollection[4(queued), 3(running), 2(succeeded), 1(succeeded)]"
        assert_equal(str(self.run_collection), expected)
