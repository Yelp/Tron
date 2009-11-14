from testify import *
from testify.utils import turtle

from tron import mcp, job, scheduler

class MockJob(turtle.Turtle):
    name = "Test Job"
    scheduler = scheduler.ConstantScheduler()
    def __init__(self):
        self.runs = []
    def next_run(self):
        class MockJobRun(turtle.Turtle):
            should_start = True
            def start(self):
                self.is_done = True
                self.is_success = True

        my_run = MockJobRun()
        self.runs.append(my_run)
        return my_run


class SimpleTest(TestCase):
    @setup
    def build_jobs(self):
        self.job = MockJob()

        self.master = mcp.MasterControlProgram()
        self.master.add_job(self.job)
        
    def test(self):
        self.master.check_and_run()
        
        assert_equal(len(self.job.runs), 1)
        run = self.job.runs[0]
        assert run.is_done
        assert run.is_success