from testify import *
from testify.utils import turtle

from tron import job, scheduler

class JobRunState(TestCase):
    """Check that our job runs can start/stop and manage their state"""
    @setup
    def build_job(self):
        self.job = job.Job(name="Test Job")
        self.run = self.job.build_run()
    
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
        

if __name__ == '__main__':
    run()