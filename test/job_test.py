import datetime

from testify import *
from testify.utils import turtle

from tron import job, scheduler
from tron.utils import timeutils

class JobRunState(TestCase):
    """Check that our job runs can start/stop and manage their state"""
    @setup
    def build_job(self):
        self.job = job.Job(name="Test Job")
        self.run = self.job.build_run()

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


class JobRunBuildingTest(TestCase):
    """Check hat we can create and manage job runs"""
    @setup
    def build_job(self):
        self.job = job.Job(name="Test Job")
    
    def test_build_run(self):
        run = self.job.build_run()

        assert self.job.runs[-1] == run
        assert run.id
        
        other_run = self.job.next_run()
        assert other_run == run
        assert_equal(len(self.job.runs), 1)

    def test_no_schedule(self):
        run = self.job.next_run()
        assert_equal(run, None)


class JobRunReadyTest(TestCase):
    """Test whether our job thinks it's time to start
    
    This means meeting resource requirements and and time schedules
    """
    @setup
    def build_job(self):
        self.job = job.Job(name="Test Job")
        self.job.scheduler = scheduler.ConstantScheduler()

    def test_ready_no_resources(self):
        run = self.job.next_run()
        assert run.should_start
    
    def test_ready_with_resources(self):
        res = turtle.Turtle()
        res.ready = False
        self.job.resources.append(res)
        
        run = self.job.next_run()
        assert not run.should_start
        
        res.ready = True
        assert run.should_start
        

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
        self.job.scheduler = scheduler.ConstantScheduler()
    
    def _cmd(self):
        job_run = self.job.next_run()
        return job_run.command

    def test_name(self):
        self.job.command = "somescript --name=%(jobname)s"
        assert_equal(self._cmd(), "somescript --name=%s" % self.job.name)

    def test_runid(self):
        self.job.command = "somescript --id=%(runid)s"
        job_run = self.job.next_run()
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


        
if __name__ == '__main__':
    run()