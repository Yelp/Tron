import datetime
import os

from testify import *
from testify.utils import turtle

from tron import node, job, job_flow, scheduler
from tron.utils import timeutils

def get_runs_by_state(job, state):
    return filter(lambda r: r.state == state, job.runs)

class TestJob(TestCase):
    """Unit testing for Job class"""
    @setup
    def setup(self):
        self.job = job.Job(name="Test Job")
        self.job.command = "Test command"
        self.flow = job_flow.JobFlow("Test Flow", self.job)

    def test_next_run(self):
        assert_equals(self.flow.next_run(), None)
        
        self.job.scheduler = turtle.Turtle()
        self.job.scheduler.next_run = lambda j:None

        assert_equals(self.flow.next_run(), None)
        assert_equals(len(get_runs_by_state(self.job, job.JOB_RUN_SCHEDULED)), 0)

        self.flow.scheduler = scheduler.ConstantScheduler()
        assert self.flow.next_run()
        assert_equals(len(get_runs_by_state(self.job, job.JOB_RUN_SCHEDULED)), 1)

    def test_next_run_prev(self):
        self.flow.scheduler = scheduler.DailyScheduler()
        run = self.flow.next_run()
        assert_equals(run.prev, None)

        run2 = self.flow.next_run()

        assert run
        assert run2
        assert_equals(len(get_runs_by_state(self.job, job.JOB_RUN_SCHEDULED)), 2)
        assert_equals(run2.prev, run)

        run3 = self.flow.next_run()
        assert_equals(run3.prev, run2)

        run3.runs[0].state = job.JOB_RUN_CANCELLED
        run4 = self.flow.next_run()
        assert_equals(run4.prev, run3)

    def test_build_run(self):
        run = self.job.build_run()
        assert_equals(len(self.job.runs), 1)
        assert_equals(self.job.runs[0], run)

    def test_restore(self):
        run = self.job.build_run()
        rest = self.job.restore(run.id, run.state_data)
        
        assert_equals(len(self.job.runs), 2) 
        assert_equals(rest.id, run.id)
        assert_equals(rest.state_data, run.state_data)

class TestJobRun(TestCase):
    """Unit testing for JobRun class"""
    @setup
    def setup(self):
        self.job = job.Job(name="Test Job", node=turtle.Turtle())
        self.flow = job_flow.JobFlow("Test Flow", self.job)
        self.flow.scheduler = scheduler.DailyScheduler()
        self.flow.queueing = True
        self.job.command = "Test command"

        self.flow_run = self.flow.next_run()
        self.run = self.flow_run.runs[0]
        self.job.data[self.run.id] = self.run.state_data

    def test_scheduled_start_succeed(self):
        self.flow_run.scheduled_start()

        assert self.run.is_running
        assert_equals(len(get_runs_by_state(self.job, job.JOB_RUN_SCHEDULED)), 0)
        assert_equals(len(get_runs_by_state(self.job, job.JOB_RUN_RUNNING)), 1)
        assert_equals(self.run.state, job.JOB_RUN_RUNNING)

    def test_scheduled_start_wait(self):
        flow_run2 = self.flow.next_run()
        
        assert_equals(len(get_runs_by_state(self.job, job.JOB_RUN_SCHEDULED)), 2)
        flow_run2.scheduled_start()
        assert flow_run2.is_queued
        assert_equals(len(get_runs_by_state(self.job, job.JOB_RUN_SCHEDULED)), 1)
        
        self.flow_run.scheduled_start()
        assert self.run.is_running
        
        self.run.succeed()
        assert self.run.is_success
        assert flow_run2.runs[0].is_running

    def test_scheduled_start_cancel(self):
        self.flow.queueing = False
        flow_run2 = self.flow.next_run()
        #self.job.scheduled[run2.id] = run2.state_data
        
        assert_equals(len(get_runs_by_state(self.job, job.JOB_RUN_SCHEDULED)), 2)
        flow_run2.scheduled_start()
        assert flow_run2.is_cancelled
        assert_equals(len(get_runs_by_state(self.job, job.JOB_RUN_SCHEDULED)), 1)
        
        self.flow_run.scheduled_start()
        assert self.run.is_running
        
        self.run.succeed()
        assert self.run.is_success
        assert flow_run2.is_cancelled


class JobRunState(TestCase):
    """Check that our job runs can start/stop and manage their state"""
    @setup
    def build_job(self):
        self.job = job.Job(name="Test Job")
        self.job.command = "Test command"
        self.job.node = turtle.Turtle()
        self.run = self.job.build_run()
        self.run.flow_run = turtle.Turtle()

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

class JobRunJobDependency(TestCase):
    @setup
    def build_job(self):
        self.job = job.Job(name="Test Job1")
        self.job.command = "Test command1"
        self.job.node = turtle.Turtle()

        self.dep_job = job.Job(name="Test Job2")
        self.dep_job.command = "Test command2"
        self.dep_job.node = turtle.Turtle()
        self.dep_job.required_jobs.append(self.job)

        self.job_flow = job_flow.JobFlow("Test Flow", self.job)
        self.job_flow.topo_jobs.append(self.dep_job)
        self.job_flow.scheduler = scheduler.DailyScheduler()
        self.flow_run = self.job_flow.next_run()
        self.run = self.flow_run.runs[0]
        self.dep_run = self.flow_run.runs[1]

    def test_success(self):
        assert_equal(len(self.dep_job.runs), 1)
        assert self.dep_run.is_queued
        
        self.run.start()

        assert_equal(len(self.dep_job.runs), 1)
        assert self.dep_run.is_queued

        self.run.succeed()

        assert_equal(len(self.dep_job.runs), 1)
 
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

        assert_equal(len(self.dep_job.runs), 1)
        assert self.dep_run.is_queued


class JobRunBuildingTest(TestCase):
    """Check hat we can create and manage job runs"""
    @setup
    def build_job(self):
        self.job = job.Job(name="Test Job")
        self.flow = job_flow.JobFlow(self.job)

    def test_build_run(self):
        run = self.job.build_run()

        assert_equal(self.job.runs[-1], run)
        assert run.id
        
        assert_equal(len(self.job.runs), 1)

    def test_no_schedule(self):
        run = self.flow.next_run()
        assert_equal(run, None)


class JobRunReadyTest(TestCase):
    """Test whether our job thinks it's time to start
    
    This means meeting resource requirements and and time schedules
    """
    @setup
    def build_job(self):
        self.job = job.Job(name="Test Job")
        self.job.command = "Test command"
        self.flow = job_flow.JobFlow("Test Flow", self.job)
        self.flow.scheduler = scheduler.ConstantScheduler()

    def test_ready_no_resources(self):
        flow_run = self.flow.next_run()
        assert flow_run.should_start
    
    def test_ready_with_resources(self):
        res = turtle.Turtle()
        res.ready = False
        self.job.resources.append(res)
        
        flow_run = self.flow.next_run()
        run = flow_run.runs[0]
        assert not run.should_start
        
        res.ready = True
        assert run.should_start
        

class JobRunLogFileTest(TestCase):
    @setup
    def build_job(self):
        self.job = job.Job(name="Test Job", node=turtle.Turtle())
        self.flow = job_flow.JobFlow("Test Flow", self.job)
        self.job.command = "Test command"

    def test_no_logging(self):
        run = self.job.build_run()
        run.start()

    def test_directory_log(self):
        self.job.output_dir = "."
        run = self.job.build_run()
        run.start()
        assert os.path.isfile("./Test Job.out")
        os.remove("./Test Job.out")
        
    def test_file_log(self):
        self.job.output_dir = "./test_output_file.out"
        run = self.job.build_run()
        run.start()
        assert os.path.isfile("./test_output_file.out")
        os.remove("./test_output_file.out")


class JobRunVariablesTest(TestCase):
    @class_setup
    def freeze_time(self):
        timeutils.override_current_time(datetime.datetime.now())
        self.now = timeutils.current_time()

    @class_teardown
    def unfreeze_time(self):
        timeutils.override_current_time(None)
    
    @setup
    def build_job(self):
        self.job = job.Job(name="Test Job")
        self.flow = job_flow.JobFlow("Test Flow", self.job)
        self.flow.scheduler = scheduler.ConstantScheduler()
    
    def _cmd(self):
        flow_run = self.flow.next_run()
        return flow_run.runs[0].command

    def test_name(self):
        self.job.command = "somescript --name=%(jobname)s"
        assert_equal(self._cmd(), "somescript --name=%s" % self.job.name)

    def test_runid(self):
        self.job.command = "somescript --id=%(runid)s"
        flow_run = self.flow.next_run()
        job_run = flow_run.runs[0]
        assert_equal(job_run.command, "somescript --id=%s" % job_run.id)

    def test_shortdate(self):
        self.job.command = "somescript -d %(shortdate)s"
        assert_equal(self._cmd(), "somescript -d %.4d-%.2d-%.2d" % (self.now.year, self.now.month, self.now.day))

    def test_shortdate_plus(self):
        self.job.command = "somescript -d %(shortdate+1)s"
        tmrw = self.now + datetime.timedelta(days=1)
        assert_equal(self._cmd(), "somescript -d %.4d-%.2d-%.2d" % (tmrw.year, tmrw.month, tmrw.day))

    def test_shortdate_minus(self):
        self.job.command = "somescript -d %(shortdate-1)s"
        ystr = self.now - datetime.timedelta(days=1)
        assert_equal(self._cmd(), "somescript -d %.4d-%.2d-%.2d" % (ystr.year, ystr.month, ystr.day))

    def test_unixtime(self):
        self.job.command = "somescript -t %(unixtime)s"
        timestamp = int(timeutils.to_timestamp(self.now))
        assert_equal(self._cmd(), "somescript -t %d" % timestamp)

    def test_unixtime_plus(self):
        self.job.command = "somescript -t %(unixtime+100)s"
        timestamp = int(timeutils.to_timestamp(self.now)) + 100
        assert_equal(self._cmd(), "somescript -t %d" % timestamp)

    def test_unixtime_minus(self):
        self.job.command = "somescript -t %(unixtime-100)s"
        timestamp = int(timeutils.to_timestamp(self.now)) - 100
        assert_equal(self._cmd(), "somescript -t %d" % timestamp)

    def test_daynumber(self):
        self.job.command = "somescript -d %(daynumber)s"
        assert_equal(self._cmd(), "somescript -d %d" % (self.now.toordinal(),))

    def test_daynumber_plus(self):
        self.job.command = "somescript -d %(daynumber+1)s"
        tmrw = self.now + datetime.timedelta(days=1)
        assert_equal(self._cmd(), "somescript -d %d" % (tmrw.toordinal(),))

    def test_daynumber_minus(self):
        self.job.command = "somescript -d %(daynumber-1)s"
        ystr = self.now - datetime.timedelta(days=1)
        assert_equal(self._cmd(), "somescript -d %d" % (ystr.toordinal(),))

        
if __name__ == '__main__':
    run()
