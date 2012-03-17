import time
from tempfile import NamedTemporaryFile

from testify import TestCase, run, assert_equal, assert_not_in, assert_in
from testify import assert_not_equal
from testify import setup, teardown

from tron.filehandler import FileHandleManager

class FileHandleWrapperTestCase(TestCase):

    @setup
    def setup_fh_wrapper(self):
        self.file = NamedTemporaryFile('r')
        self.manager = FileHandleManager.get_instance()
        self.fh_wrapper = self.manager.open(self.file.name)

    @teardown
    def teardown_fh_wrapper(self):
         self.fh_wrapper.close()

    def test_init(self):
        assert_equal(self.fh_wrapper._fh, None)

    def test_close(self):
        # Test close without a write, no exception is good
        self.fh_wrapper.close()

    def test_close_with_write(self):
        # Test close with a write
        self.fh_wrapper.write("some things")
        self.fh_wrapper.close()
        assert_equal(self.fh_wrapper._fh, None)
        assert_equal(self.fh_wrapper.manager, self.manager)
        # This is somewhat coupled
        assert_not_in(self.fh_wrapper, self.manager.cache)

    def test_write(self):
        # Test write without a previous open
        before_time = time.time()
        self.fh_wrapper.write("some things")
        after_time = time.time()

        assert self.fh_wrapper._fh
        assert_equal(self.fh_wrapper._fh.closed, False)
        assert before_time <= self.fh_wrapper.last_accessed <= after_time

        # Test write after previous open
        before_time = time.time()
        self.fh_wrapper.write("\nmore things")
        after_time = time.time()
        assert before_time <= self.fh_wrapper.last_accessed <= after_time


class FileHandleManagerTestCase(TestCase):

    @setup
    def setup_fh_manager(self):
        self.file1 = NamedTemporaryFile('r')
        self.file2 = NamedTemporaryFile('r')
        FileHandleManager.set_max_idle_time(2)
        self.manager = FileHandleManager.get_instance()

    @teardown
    def teardown_fh_manager(self):
        FileHandleManager.reset()

    def test_get_instance(self):
        assert_equal(self.manager, FileHandleManager.get_instance())
        # Repeat for good measure
        assert_equal(self.manager, FileHandleManager.get_instance())

    def test_set_max_idle_time(self):
        max_idle_time = 300
        FileHandleManager.set_max_idle_time(max_idle_time)
        assert_equal(max_idle_time, self.manager.max_idle_time)

    def test_open(self):
        # Not yet in cache
        fh_wrapper = self.manager.open(self.file1.name)
        assert_in(fh_wrapper.name, self.manager.cache)

        # Should now be in cache
        fh_wrapper2 = self.manager.open(self.file1.name)

        # Same wrapper
        assert_equal(fh_wrapper, fh_wrapper2)

        # Different wrapper
        assert_not_equal(fh_wrapper, self.manager.open(self.file2.name))

    def test_cleanup_none(self):
        # Nothing to remove
        fh_wrapper = self.manager.open(self.file1.name)
        self.manager.cleanup()
        assert_in(fh_wrapper.name, self.manager.cache)

    def test_cleanup_single(self):
        fh_wrapper = self.manager.open(self.file1.name)
        fh_wrapper.last_accessed = 123456
        time_func = lambda: 123458.1
        self.manager.cleanup(time_func)
        assert_not_in(fh_wrapper.name, self.manager.cache)
        assert_equal(len(self.manager.cache), 0)

    def test_cleanup_many(self):
        fh_wrappers = [
            self.manager.open(self.file1.name),
            self.manager.open(self.file2.name),
            self.manager.open(NamedTemporaryFile('r').name),
            self.manager.open(NamedTemporaryFile('r').name),
            self.manager.open(NamedTemporaryFile('r').name),
        ]
        for i, fh_wrapper in enumerate(fh_wrappers):
            fh_wrapper.last_accessed = 123456 + i

        time_func = lambda: 123460.1
        self.manager.cleanup(time_func)
        assert_equal(len(self.manager.cache), 2)

        for fh_wrapper in fh_wrappers[:3]:
            assert_not_in(fh_wrapper.name, self.manager.cache)

        for fh_wrapper in fh_wrappers[3:]:
            assert_in(fh_wrapper.name, self.manager.cache)

    def test_cleanup_opened(self):
        fh_wrapper = self.manager.open(self.file1.name)
        fh_wrapper.write("Some things")

        fh_wrapper.last_accessed = 123456
        time_func = lambda: 123458.1
        self.manager.cleanup(time_func)
        assert_not_in(fh_wrapper.name, self.manager.cache)
        assert_equal(len(self.manager.cache), 0)

    def test_cleanup_natural(self):
        FileHandleManager.set_max_idle_time(1)
        fh_wrapper1 = self.manager.open(self.file1.name)
        fh_wrapper2 = self.manager.open(self.file2.name)
        fh_wrapper1.write("Some things")

        time.sleep(1.5)
        fh_wrapper2.write("Other things.")

        assert_not_in(fh_wrapper1.name, self.manager.cache)
        assert_in(fh_wrapper2.name, self.manager.cache)

        # Now that 1 is closed, try writing again
        fh_wrapper1.write("Some things")
        assert_in(fh_wrapper1.name, self.manager.cache)
        assert not fh_wrapper1._fh.closed

    def test_remove(self):
        # In cache
        fh_wrapper = self.manager.open(self.file1.name)
        assert_in(fh_wrapper.name, self.manager.cache)
        self.manager.remove(fh_wrapper)
        assert_not_in(fh_wrapper.name, self.manager.cache)

        # Not in cache
        self.manager.remove(fh_wrapper)
        assert_not_in(fh_wrapper.name, self.manager.cache)

    def test_update(self):
        fh_wrapper1 = self.manager.open(self.file1.name)
        fh_wrapper2 = self.manager.open(self.file2.name)
        assert_equal(self.manager.cache.keys(), [fh_wrapper1.name, fh_wrapper2.name])

        self.manager.update(fh_wrapper1)
        assert_equal(self.manager.cache.keys(), [fh_wrapper2.name, fh_wrapper1.name])


if __name__ == "__main__":
    run()