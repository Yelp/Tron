from testify import TestCase, setup, teardown, assert_equal, turtle
from tron.core import jobrun, actionrun

# TODO
class JobRunTestCase(TestCase):

    @setup
    def build_job(self):
        pass

    @teardown
    def teardown_job(self):
        pass

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


class JobRunCollectionTestCase(TestCase):

    @setup
    def setup_runs(self):
        self.run_collection = jobrun.JobRunCollection(5)
        self.job_runs = [
            turtle.Turtle(state=actionrun.ActionRun.STATE_QUEUED, run_num=4),
            turtle.Turtle(state=actionrun.ActionRun.STATE_RUNNING, run_num=3)
        ] + [
            turtle.Turtle(state=actionrun.ActionRun.STATE_SUCCEEDED, run_num=i)
            for i in xrange(2,0,-1)
        ]
        self.run_collection.runs.extend(self.job_runs)

    # TODO: some other tests

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

    def test_get_newest_no_runs(self):
        run_collection = jobrun.JobRunCollection(5)
        assert_equal(run_collection.get_newest(), None)

    # TODO: some other tests