import datetime
import os

from testify import *
from testify.utils import turtle

from tron import node, action, job, scheduler
from tron.utils import timeutils

def get_num_runs_by_state(job, state):
    count = 0
    for run in job.runs:
        count += len(filter(lambda r: r.state == state, run.runs))
    return count

class TestAction(TestCase):
    """Unit testing for Action class"""
    @setup
    def setup(self):
        self.action = action.Action(name="Test Action")
        self.action.command = "Test command"
        self.job = job.Job("Test Job", self.action)
        self.action.job = self.job

    def test_next_run(self):
        assert_equals(self.job.next_run(), None)
        
        self.action.scheduler = turtle.Turtle()
        self.action.scheduler.next_run = lambda j:None

        assert_equals(self.job.next_run(), None)
        assert_equals(get_num_runs_by_state(self.job, action.ACTION_RUN_SCHEDULED), 0)

        self.job.scheduler = scheduler.ConstantScheduler()
        assert self.job.next_run()
        assert_equals(get_num_runs_by_state(self.job, action.ACTION_RUN_SCHEDULED), 1)

    def test_next_run_prev(self):
        self.job.scheduler = scheduler.DailyScheduler()
        run = self.job.next_run()
        assert_equals(run.prev, None)

        run2 = self.job.next_run()

        assert run
        assert run2
        assert_equals(get_num_runs_by_state(self.job, action.ACTION_RUN_SCHEDULED), 2)
        assert_equals(run2.prev, run)

        run3 = self.job.next_run()
        assert_equals(run3.prev, run2)

        run3.runs[0].state = action.ACTION_RUN_CANCELLED
        run4 = self.job.next_run()
        assert_equals(run4.prev, run3)


class TestActionRun(TestCase):
    """Unit testing for ActionRun class"""
    @setup
    def setup(self):
        self.action = action.Action(name="Test Action", node_pool=turtle.Turtle())
        self.job = job.Job("Test Job", self.action)
        self.job.scheduler = scheduler.DailyScheduler()
        self.job.queueing = True
        self.action.job = self.job
        self.action.command = "Test command"

        self.job_run = self.job.next_run()
        self.run = self.job_run.runs[0]

    def test_scheduled_start_succeed(self):
        self.job_run.scheduled_start()

        assert self.run.is_running
        assert_equals(get_num_runs_by_state(self.job, action.ACTION_RUN_SCHEDULED), 0)
        assert_equals(get_num_runs_by_state(self.job, action.ACTION_RUN_RUNNING), 1)
        assert_equals(self.run.state, action.ACTION_RUN_RUNNING)

    def test_scheduled_start_wait(self):
        job_run2 = self.job.next_run()
        
        assert_equals(get_num_runs_by_state(self.job, action.ACTION_RUN_SCHEDULED), 2)
        job_run2.scheduled_start()
        assert job_run2.is_queued
        assert_equals(get_num_runs_by_state(self.job, action.ACTION_RUN_SCHEDULED), 1)
        
        self.job_run.scheduled_start()
        assert self.run.is_running
        
        self.run.succeed()
        assert self.run.is_success
        assert job_run2.runs[0].is_running

    def test_scheduled_start_cancel(self):
        self.job.queueing = False
        job_run2 = self.job.next_run()
        #self.action.scheduled[run2.id] = run2.state_data
        
        assert_equals(get_num_runs_by_state(self.job, action.ACTION_RUN_SCHEDULED), 2)
        job_run2.scheduled_start()
        assert job_run2.is_cancelled
        assert_equals(get_num_runs_by_state(self.job, action.ACTION_RUN_SCHEDULED), 1)
        
        self.job_run.scheduled_start()
        assert self.run.is_running
        
        self.run.succeed()
        assert self.run.is_success
        assert job_run2.is_cancelled


class ActionRunState(TestCase):
    """Check that our action runs can start/stop and manage their state"""
    @setup
    def build_job(self):
        self.action = action.Action(name="Test Action")
        self.action.command = "Test command"
        self.action.node_pool = turtle.Turtle()
        self.action.job = turtle.Turtle()
        self.run = self.action.build_run(turtle.Turtle())
        self.run.job_run = turtle.Turtle()

        def noop_execute():
            pass

        self.run._execute = noop_execute

    def test_success(self):
        assert not self.run.is_running
        assert not self.run.is_done
        
        self.run.start()
        
        assert self.run.is_running
        assert not self.run.is_done
        assert self.run.start_time
        
        self.run.succeed()
        
        assert not self.run.is_running
        assert self.run.is_done
        assert self.run.end_time
        assert_equal(self.run.exit_status, 0)

    def test_failure(self):
        self.run.start()

        self.run.fail(1)
        assert not self.run.is_running
        assert self.run.is_done
        assert self.run.end_time
        assert_equal(self.run.exit_status, 1)

class TestRunDependency(TestCase):
    @setup
    def build_job(self):
        self.action = action.Action(name="Test Action1")
        self.action.command = "Test command1"
        self.action.node_pool = turtle.Turtle()

        self.dep_action = action.Action(name="Test Action2")
        self.dep_action.command = "Test command2"
        self.dep_action.node_pool = turtle.Turtle()
        self.dep_action.required_actions.append(self.action)

        self.job = job.Job("Test Job", self.action)
        self.action.job = self.job
        self.dep_action.job = self.job 

        self.job.topo_actions.append(self.dep_action)
        self.job.scheduler = scheduler.DailyScheduler()
        self.job_run = self.job.next_run()
        self.run = self.job_run.runs[0]
        self.dep_run = self.job_run.runs[1]

    def test_success(self):
        assert self.dep_run.is_queued
        self.run.start()
        
        assert self.dep_run.is_queued
        self.run.succeed()
        
        assert self.dep_run.is_running
        assert not self.dep_run.is_done
        assert self.dep_run.start_time
        assert not self.dep_run.end_time
       
        self.dep_run.succeed()

        assert not self.dep_run.is_running
        assert self.dep_run.is_done
        assert self.dep_run.start_time
        assert self.dep_run.end_time
              
    def test_fail(self):
        self.run.start()
        self.run.fail(1)

        assert self.dep_run.is_queued


class ActionRunBuildingTest(TestCase):
    """Check hat we can create and manage action runs"""
    @setup
    def build_job(self):
        self.action = action.Action(name="Test Action")
        self.job = job.Job(self.action)
        self.action.job = self.job
        self.action.command = "Test Action Command"

    def test_build_run(self):
        run = self.job.build_run()
        act_run = self.action.build_run(run)
        assert_equal(act_run.job_run, run)
        assert run.id

    def test_no_schedule(self):
        run = self.job.next_run()
        assert_equal(run, None)


class ActionRunLogFileTest(TestCase):
    @setup
    def build_job(self):
        self.action = action.Action(name="Test Action", node_pool=turtle.Turtle())
        self.job = job.Job("Test Job", self.action)
        self.action.job = self.job
        self.action.command = "Test command"

    def test_no_logging(self):
        run = self.action.build_run(turtle.Turtle())
        run.start()

    def test_file_log(self):
        self.action.output_path = "./test_output_file.out"
        run = self.action.build_run(turtle.Turtle())
        run.start()
        assert os.path.isfile("./test_output_file.out")
        os.remove("./test_output_file.out")


class ActionRunVariablesTest(TestCase):
    @class_setup
    def freeze_time(self):
        timeutils.override_current_time(datetime.datetime.now())
        self.now = timeutils.current_time()

    @class_teardown
    def unfreeze_time(self):
        timeutils.override_current_time(None)
    
    @setup
    def build_job(self):
        self.action = action.Action(name="Test Action")
        self.action.command = "Test Action Command"
        self.job = job.Job("Test Job", self.action)
        self.action.job = self.job
        self.job.scheduler = scheduler.ConstantScheduler()

    def _cmd(self):
        job_run = self.job.next_run()
        return job_run.runs[0].command

    def test_name(self):
        self.action.command = "somescript --name=%(actionname)s"
        assert_equal(self._cmd(), "somescript --name=%s" % self.action.name)

    def test_runid(self):
        self.action.command = "somescript --id=%(runid)s"
        job_run = self.job.next_run()
        action_run = job_run.runs[0]
        assert_equal(action_run.command, "somescript --id=%s" % action_run.id)

    def test_shortdate(self):
        self.action.command = "somescript -d %(shortdate)s"
        assert_equal(self._cmd(), "somescript -d %.4d-%.2d-%.2d" % (self.now.year, self.now.month, self.now.day))

    def test_shortdate_plus(self):
        self.action.command = "somescript -d %(shortdate+1)s"
        tmrw = self.now + datetime.timedelta(days=1)
        assert_equal(self._cmd(), "somescript -d %.4d-%.2d-%.2d" % (tmrw.year, tmrw.month, tmrw.day))

    def test_shortdate_minus(self):
        self.action.command = "somescript -d %(shortdate-1)s"
        ystr = self.now - datetime.timedelta(days=1)
        assert_equal(self._cmd(), "somescript -d %.4d-%.2d-%.2d" % (ystr.year, ystr.month, ystr.day))

    def test_unixtime(self):
        self.action.command = "somescript -t %(unixtime)s"
        timestamp = int(timeutils.to_timestamp(self.now))
        assert_equal(self._cmd(), "somescript -t %d" % timestamp)

    def test_unixtime_plus(self):
        self.action.command = "somescript -t %(unixtime+100)s"
        timestamp = int(timeutils.to_timestamp(self.now)) + 100
        assert_equal(self._cmd(), "somescript -t %d" % timestamp)

    def test_unixtime_minus(self):
        self.action.command = "somescript -t %(unixtime-100)s"
        timestamp = int(timeutils.to_timestamp(self.now)) - 100
        assert_equal(self._cmd(), "somescript -t %d" % timestamp)

    def test_daynumber(self):
        self.action.command = "somescript -d %(daynumber)s"
        assert_equal(self._cmd(), "somescript -d %d" % (self.now.toordinal(),))

    def test_daynumber_plus(self):
        self.action.command = "somescript -d %(daynumber+1)s"
        tmrw = self.now + datetime.timedelta(days=1)
        assert_equal(self._cmd(), "somescript -d %d" % (tmrw.toordinal(),))

    def test_daynumber_minus(self):
        self.action.command = "somescript -d %(daynumber-1)s"
        ystr = self.now - datetime.timedelta(days=1)
        assert_equal(self._cmd(), "somescript -d %d" % (ystr.toordinal(),))

        
if __name__ == '__main__':
    run()
