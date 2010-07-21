import datetime
import os

from testify import *
from testify.utils import turtle

from tron import node, task, job, scheduler
from tron.utils import timeutils

def get_runs_by_state(task, state):
    return filter(lambda r: r.state == state, task.runs)

class TestTask(TestCase):
    """Unit testing for Task class"""
    @setup
    def setup(self):
        self.task = task.Task(name="Test Task")
        self.task.command = "Test command"
        self.job = job.Job("Test Job", self.task)
        self.task.job = self.job

    def test_next_run(self):
        assert_equals(self.job.next_run(), None)
        
        self.task.scheduler = turtle.Turtle()
        self.task.scheduler.next_run = lambda j:None

        assert_equals(self.job.next_run(), None)
        assert_equals(len(get_runs_by_state(self.task, task.TASK_RUN_SCHEDULED)), 0)

        self.job.scheduler = scheduler.ConstantScheduler()
        assert self.job.next_run()
        assert_equals(len(get_runs_by_state(self.task, task.TASK_RUN_SCHEDULED)), 1)

    def test_next_run_prev(self):
        self.job.scheduler = scheduler.DailyScheduler()
        run = self.job.next_run()
        assert_equals(run.prev, None)

        run2 = self.job.next_run()

        assert run
        assert run2
        assert_equals(len(get_runs_by_state(self.task, task.TASK_RUN_SCHEDULED)), 2)
        assert_equals(run2.prev, run)

        run3 = self.job.next_run()
        assert_equals(run3.prev, run2)

        run3.runs[0].state = task.TASK_RUN_CANCELLED
        run4 = self.job.next_run()
        assert_equals(run4.prev, run3)

    def test_build_run(self):
        run = self.task.build_run()
        assert_equals(len(self.task.runs), 1)
        assert_equals(self.task.runs[0], run)


class TestTaskRun(TestCase):
    """Unit testing for TaskRun class"""
    @setup
    def setup(self):
        self.task = task.Task(name="Test Task", node=turtle.Turtle())
        self.job = job.Job("Test Job", self.task)
        self.job.scheduler = scheduler.DailyScheduler()
        self.job.queueing = True
        self.task.job = self.job
        self.task.command = "Test command"

        self.job_run = self.job.next_run()
        self.run = self.job_run.runs[0]

    def test_scheduled_start_succeed(self):
        self.job_run.scheduled_start()

        assert self.run.is_running
        assert_equals(len(get_runs_by_state(self.task, task.TASK_RUN_SCHEDULED)), 0)
        assert_equals(len(get_runs_by_state(self.task, task.TASK_RUN_RUNNING)), 1)
        assert_equals(self.run.state, task.TASK_RUN_RUNNING)

    def test_scheduled_start_wait(self):
        job_run2 = self.job.next_run()
        
        assert_equals(len(get_runs_by_state(self.task, task.TASK_RUN_SCHEDULED)), 2)
        job_run2.scheduled_start()
        assert job_run2.is_queued
        assert_equals(len(get_runs_by_state(self.task, task.TASK_RUN_SCHEDULED)), 1)
        
        self.job_run.scheduled_start()
        assert self.run.is_running
        
        self.run.succeed()
        assert self.run.is_success
        assert job_run2.runs[0].is_running

    def test_scheduled_start_cancel(self):
        self.job.queueing = False
        job_run2 = self.job.next_run()
        #self.task.scheduled[run2.id] = run2.state_data
        
        assert_equals(len(get_runs_by_state(self.task, task.TASK_RUN_SCHEDULED)), 2)
        job_run2.scheduled_start()
        assert job_run2.is_cancelled
        assert_equals(len(get_runs_by_state(self.task, task.TASK_RUN_SCHEDULED)), 1)
        
        self.job_run.scheduled_start()
        assert self.run.is_running
        
        self.run.succeed()
        assert self.run.is_success
        assert job_run2.is_cancelled


class TaskRunState(TestCase):
    """Check that our task runs can start/stop and manage their state"""
    @setup
    def build_job(self):
        self.task = task.Task(name="Test Task")
        self.task.command = "Test command"
        self.task.node = turtle.Turtle()
        self.task.job = turtle.Turtle()
        self.run = self.task.build_run()
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
        self.task = task.Task(name="Test Task1")
        self.task.command = "Test command1"
        self.task.node = turtle.Turtle()

        self.dep_task = task.Task(name="Test Task2")
        self.dep_task.command = "Test command2"
        self.dep_task.node = turtle.Turtle()
        self.dep_task.required_tasks.append(self.task)

        self.job = job.Job("Test Job", self.task)
        self.task.job = self.job
        self.dep_task.job = self.job 

        self.job.topo_tasks.append(self.dep_task)
        self.job.scheduler = scheduler.DailyScheduler()
        self.job_run = self.job.next_run()
        self.run = self.job_run.runs[0]
        self.dep_run = self.job_run.runs[1]

    def test_success(self):
        assert_equal(len(self.dep_task.runs), 1)
        assert self.dep_run.is_queued
        
        self.run.start()

        assert_equal(len(self.dep_task.runs), 1)
        assert self.dep_run.is_queued

        self.run.succeed()

        assert_equal(len(self.dep_task.runs), 1)
 
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

        assert_equal(len(self.dep_task.runs), 1)
        assert self.dep_run.is_queued


class TaskRunBuildingTest(TestCase):
    """Check hat we can create and manage task runs"""
    @setup
    def build_job(self):
        self.task = task.Task(name="Test Task")
        self.job = job.Job(self.task)
        self.task.job = self.job
        self.task.command = "Test Task Command"

    def test_build_run(self):
        run = self.task.build_run()

        assert_equal(self.task.runs[-1], run)
        assert run.id
        
        assert_equal(len(self.task.runs), 1)

    def test_no_schedule(self):
        run = self.job.next_run()
        assert_equal(run, None)


class TaskRunLogFileTest(TestCase):
    @setup
    def build_job(self):
        self.task = task.Task(name="Test Task", node=turtle.Turtle())
        self.job = job.Job("Test Job", self.task)
        self.task.job = self.job
        self.task.command = "Test command"

    def test_no_logging(self):
        run = self.task.build_run()
        run.start()

    def test_directory_log(self):
        self.task.output_dir = "."
        run = self.task.build_run()
        run.start()
        assert os.path.isfile("./Test Task.out")
        os.remove("./Test Task.out")
        
    def test_file_log(self):
        self.task.output_dir = "./test_output_file.out"
        run = self.task.build_run()
        run.start()
        assert os.path.isfile("./test_output_file.out")
        os.remove("./test_output_file.out")


class TaskRunVariablesTest(TestCase):
    @class_setup
    def freeze_time(self):
        pass
        #timeutils.override_current_time(datetime.datetime.now())
        self.now = timeutils.current_time()

    @class_teardown
    def unfreeze_time(self):
        pass
        #timeutils.override_current_time(None)
    
    @setup
    def build_job(self):
        self.task = task.Task(name="Test Task")
        self.task.command = "Test Task Command"
        self.job = job.Job("Test Job", self.task)
        self.task.job = self.job
        self.job.scheduler = scheduler.DailyScheduler()

    def _cmd(self):
        job_run = self.job.next_run()
        return job_run.runs[0].command

    def test_name(self):
        self.task.command = "somescript --name=%(jobname)s"
        assert_equal(self._cmd(), "somescript --name=%s" % self.task.name)

    def test_runid(self):
        self.task.command = "somescript --id=%(runid)s"
        job_run = self.job.next_run()
        task_run = job_run.runs[0]
        assert_equal(task_run.command, "somescript --id=%s" % task_run.id)

    def test_shortdate(self):
        self.task.command = "somescript -d %(shortdate)s"
        assert_equal(self._cmd(), "somescript -d %.4d-%.2d-%.2d" % (self.now.year, self.now.month, self.now.day))

    def test_shortdate_plus(self):
        self.task.command = "somescript -d %(shortdate+1)s"
        tmrw = self.now + datetime.timedelta(days=1)
        assert_equal(self._cmd(), "somescript -d %.4d-%.2d-%.2d" % (tmrw.year, tmrw.month, tmrw.day))

    def test_shortdate_minus(self):
        self.task.command = "somescript -d %(shortdate-1)s"
        ystr = self.now - datetime.timedelta(days=1)
        assert_equal(self._cmd(), "somescript -d %.4d-%.2d-%.2d" % (ystr.year, ystr.month, ystr.day))

    def test_unixtime(self):
        self.task.command = "somescript -t %(unixtime)s"
        timestamp = int(timeutils.to_timestamp(self.now))
        assert_equal(self._cmd(), "somescript -t %d" % timestamp)

    def test_unixtime_plus(self):
        self.task.command = "somescript -t %(unixtime+100)s"
        timestamp = int(timeutils.to_timestamp(self.now)) + 100
        assert_equal(self._cmd(), "somescript -t %d" % timestamp)

    def test_unixtime_minus(self):
        self.task.command = "somescript -t %(unixtime-100)s"
        timestamp = int(timeutils.to_timestamp(self.now)) - 100
        assert_equal(self._cmd(), "somescript -t %d" % timestamp)

    def test_daynumber(self):
        self.task.command = "somescript -d %(daynumber)s"
        assert_equal(self._cmd(), "somescript -d %d" % (self.now.toordinal(),))

    def test_daynumber_plus(self):
        self.task.command = "somescript -d %(daynumber+1)s"
        tmrw = self.now + datetime.timedelta(days=1)
        assert_equal(self._cmd(), "somescript -d %d" % (tmrw.toordinal(),))

    def test_daynumber_minus(self):
        self.task.command = "somescript -d %(daynumber-1)s"
        ystr = self.now - datetime.timedelta(days=1)
        assert_equal(self._cmd(), "somescript -d %d" % (ystr.toordinal(),))

        
if __name__ == '__main__':
    run()
