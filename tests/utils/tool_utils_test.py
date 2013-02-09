import os
import tempfile
from testify import TestCase, run, assert_equal, setup, class_teardown, teardown
from tests.assertions import assert_raises
from tron.utils import tool_utils

class WorkingDirTestCase(TestCase):

    @setup
    def setup_cwd(self):
        self.cwd = os.getcwd()
        self.temp_dir = tempfile.mkdtemp()
        self.second_dir = tempfile.mkdtemp()

    @teardown
    def cleanup(self):
        os.rmdir(self.temp_dir)
        os.rmdir(self.second_dir)

    @class_teardown
    def check_for_test_pollution(self):
        assert_equal(self.cwd, os.getcwd())

    def test_working_dir(self):
        with tool_utils.working_dir(self.temp_dir):
            assert_equal(os.getcwd(), self.temp_dir)
        assert_equal(os.getcwd(), self.cwd)

    def test_working_dir_with_exception(self):
        def with_exc():
            with tool_utils.working_dir(self.temp_dir):
                assert_equal(os.getcwd(), self.temp_dir)
                raise Exception("oops")

        assert_raises(Exception, with_exc)
        assert_equal(os.getcwd(), self.cwd)

    def test_working_dir_nested(self):
        with tool_utils.working_dir(self.temp_dir):
            with tool_utils.working_dir(self.second_dir):
                assert_equal(os.getcwd(), self.second_dir)
            assert_equal(os.getcwd(), self.temp_dir)
        assert_equal(os.getcwd(), self.cwd)


if __name__ == "__main__":
    run()