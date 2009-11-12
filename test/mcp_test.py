from testify import *

from tron import mcp, job, scheduler

class SimpleTest(TestCase):
    def build_jobs(self):
        self.job = job.Job()
        self.job.name = "Test Job"
        job.scheduler = scheduler.ConstantScheduler()
        
        self.master = mcp.MasterControlProgram()
        self.master.add_node(turtle.Turtle())
        self.master.add_job(job)
        
    def test(self):
        self.master.check_and_run()
        
        assert_equal(len(self.job.runs), 1)
        run = self.job.runs[0]
        assert run.is_done
        assert run.is_success