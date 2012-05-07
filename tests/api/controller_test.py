import os
import tempfile
from testify import teardown, setup, TestCase, run, assert_equal
from tests.assertions import assert_call
from tests.testingutils import Turtle
from tron.api.controller import JobController, ConfigController

class JobControllerTestCase(TestCase):

    @setup
    def setup_controller(self):
        self.jobs           = [Turtle(), Turtle(), Turtle()]
        self.mcp            = Turtle(get_jobs=lambda: self.jobs)
        self.controller     = JobController(self.mcp)

    def test_disable_all(self):
        self.controller.disable_all()
        for job in self.jobs:
            assert_call(job.disable, 0)

    def test_enable_all(self):
        self.controller.enable_all()
        for job in self.jobs:
            assert_call(job.enable, 0)


class ConfigControllerTestCase(TestCase):

    @setup
    def setup_controller(self):
        self.filename = os.path.join(tempfile.gettempdir(), 'test_config')
        self.controller = ConfigController(self.filename)

    @teardown
    def teardown_controller(self):
        try:
            os.unlink(self.filename)
        except OSError:
            pass

    def test_read_config(self):
        content = "12345"
        with open(self.filename, 'w') as fh:
            fh.write(content)

        assert_equal(self.controller.read_config(), content)

    def test_read_config_missing(self):
        self.controller.filepath = '/bogggusssss'
        assert not self.controller.read_config()

    def test_rewrite_config(self):
        content = '123456'
        assert self.controller.rewrite_config(content)
        assert_equal(self.controller.read_config(), content)

    def test_rewrite_config_missing(self):
        self.controller.filepath = '/bogggusssss'
        assert not self.controller.rewrite_config('123')


if __name__ == "__main__":
    run()