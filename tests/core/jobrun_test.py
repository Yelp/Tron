import datetime
from testify import TestCase, setup, assert_equal
from testify.assertions import assert_in
from tests.assertions import assert_length, assert_raises, assert_call
from tron.core import jobrun, actionrun
from tests.testingutils import Turtle, TestNode


class JobRunContextTestCase(TestCase):

    @setup
    def setup_context(self):
        self.jobrun = Turtle()
        self.context = jobrun.JobRunContext(self.jobrun)

    def test_cleanup_job_status(self):
        self.jobrun.action_runs.is_failure = False
        self.jobrun.action_runs.all_but_cleanup_success = True
        assert_equal(self.context.cleanup_job_status, 'SUCCESS')


class JobRunTestCase(TestCase):

    @setup
    def setup_jobrun(self):
        self.action_graph = Turtle(action_map=dict(anaction=Turtle()))
        self.job = Turtle(name="aname", action_graph=self.action_graph)
        self.run_time = datetime.datetime(2012, 3, 14, 15, 9 ,26)
        node = TestNode('thenode')
        self.job_run = jobrun.JobRun('jobname', 7, self.run_time, node)
        self.job_run.watch = Turtle()

    def test__init__(self):
        assert_equal(self.job_run.job_name, 'jobname')
        assert_equal(self.job_run.run_time, self.run_time)
        assert str(self.job_run.output_path).endswith(self.job_run.id)

    def test_for_job(self):
        run_num = 6
        node = TestNode('anode')
        run = jobrun.JobRun.for_job(
                self.job, run_num, self.run_time, node, False)

        assert run.action_runs
        assert_equal(run.job_name, self.job.name)
        assert_equal(run.run_num, run_num)
        assert_equal(run.node, node)
        assert not run.manual

    def test_for_job_manual(self):
        run_num = 6
        node = TestNode('anode')
        run = jobrun.JobRun.for_job(
                self.job, run_num, self.run_time, node, True)
        assert run.action_runs
        assert run.manual

    def test_from_state(self):
        action_run_state_data = [Turtle(), Turtle()]
        state_data = {
            'job_name':         'thejobname',
            'run_num':          22,
            'run_time':         self.run_time,
            'node_name':        'thebox',
            'end_time':         'the_end',
            'start_time':       'start_time',
            'runs':             action_run_state_data,
            'cleanup_run':      Turtle()
        }

        run = jobrun.JobRun.from_state(state_data, self.action_graph)
        # TODO:


    def test_from_state_old_state_data(self):
        pass
        # No stored_node and id instead of job_name/run_num
        # No manual

    def test_success(self):
        pass
#        assert self.run.is_scheduled
#        assert self.dep_run.is_scheduled, self.dep_run.state
#        self.job_run.start()
#
#        assert self.dep_run.is_queued
#        self.run.succeed()
#
#        # Make it look like we started successfully
#        #self.dep_run.action_command.machine.transition('start')
#
#        assert self.dep_run.is_running
#        assert not self.dep_run.is_done
#        assert self.dep_run.start_time
#        assert not self.dep_run.end_time
#
#        self.dep_run.succeed()
#
#        assert not self.dep_run.is_running
#        assert self.dep_run.is_done
#        assert self.dep_run.start_time
#        assert self.dep_run.end_time

    def test_fail(self):
        pass
#        self.job_run.start()
#        self.run.fail(1)
#
#        assert self.dep_run.is_queued, self.dep_run.state


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

        restored_runs = run_collection.restore_state(state_data, action_graph)
        assert_equal(run_collection.runs[0].run_num, 3)
        assert_equal(run_collection.runs[3].run_num, 0)
        assert_length(restored_runs, 4)

    def test_restore_state_with_runs(self):
        assert_raises(ValueError, self.run_collection.restore_state, None, None)

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
