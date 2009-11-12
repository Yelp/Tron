from testify import *

from tron import job

class TestJobRunState(TestCase):
    @setup
    def build_job(self):
        self.job = job.Job(name="Test Job")
        self.run = self.job.build_run()
    
    def test(self):
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


class TestJobRunFail(TestCase):
    @setup
    def build_job(self):
        self.job = job.Job(name="Test Job")
        self.run = self.job.build_run()
        self.run.start()
    
    def test(self):
        self.run.fail(1)
        assert not self.run.is_running
        assert self.run.is_done
        assert self.run.end_time
        assert_equal(self.run.exit_status, 1)


class TestJobRunBuilding(TestCase):
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

if __name__ == '__main__':
    run()