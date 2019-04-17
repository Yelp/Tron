from __future__ import absolute_import
from __future__ import unicode_literals

import os
import shutil
import time
from tempfile import mkdtemp
from tempfile import NamedTemporaryFile
from unittest import mock

from testifycompat import assert_equal
from testifycompat import assert_in
from testifycompat import assert_not_equal
from testifycompat import assert_not_in
from testifycompat import run
from testifycompat import setup
from testifycompat import suite
from testifycompat import teardown
from testifycompat import TestCase
from tron.serialize.filehandler import FileHandleManager
from tron.serialize.filehandler import NullFileHandle
from tron.serialize.filehandler import OutputPath
from tron.serialize.filehandler import OutputStreamSerializer


class TestFileHandleWrapper(TestCase):
    @setup
    def setup_fh_wrapper(self):
        self.file = NamedTemporaryFile('r')
        self.manager = FileHandleManager.get_instance()
        self.fh_wrapper = self.manager.open(self.file.name)

    @teardown
    def teardown_fh_wrapper(self):
        self.fh_wrapper.close()
        FileHandleManager.reset()

    def test_init(self):
        assert_equal(self.fh_wrapper._fh, NullFileHandle)

    def test_close(self):
        # Test close without a write, no exception is good
        self.fh_wrapper.close()
        # Test close again, after already closed
        self.fh_wrapper.close()

    def test_close_with_write(self):
        # Test close with a write
        self.fh_wrapper.write("some things")
        self.fh_wrapper.close()
        assert_equal(self.fh_wrapper._fh, NullFileHandle)
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
        self.fh_wrapper.close()
        with open(self.file.name) as fh:
            assert_equal(fh.read(), "some things\nmore things")

    def test_close_many(self):
        self.fh_wrapper.write("some things")
        self.fh_wrapper.close()
        self.fh_wrapper.close()

    def test_context_manager(self):
        with self.fh_wrapper as fh:
            fh.write("123")
        assert fh._fh is None
        with open(self.file.name) as fh:
            assert_equal(fh.read(), "123")


class TestFileHandleManager(TestCase):
    @setup
    def setup_fh_manager(self):
        FileHandleManager.reset()
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

        def time_func():
            return 123458.1

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

        def time_func():
            return 123460.1

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

        def time_func():
            return 123458.1

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
        assert_equal(
            list(self.manager.cache.keys()),
            [fh_wrapper1.name, fh_wrapper2.name],
        )

        self.manager.update(fh_wrapper1)
        assert_equal(
            list(self.manager.cache.keys()),
            [fh_wrapper2.name, fh_wrapper1.name],
        )


class TestOutputStreamSerializer(TestCase):
    @setup
    def setup_serializer(self):
        self.test_dir = mkdtemp()
        self.serial = OutputStreamSerializer([self.test_dir])
        self.filename = "STARS"
        self.content = "123\n456\n789"
        self.expected = [
            line for line in self.content.split('\n')
        ]

    @teardown
    def teardown_test_dir(self):
        shutil.rmtree(self.test_dir)

    def _write_contents(self):
        with open(self.serial.full_path(self.filename), 'w') as f:
            f.write(self.content)

    def test_open(self):
        with self.serial.open(self.filename) as fh:
            fh.write(self.content)

        with open(self.serial.full_path(self.filename)) as f:
            assert_equal(f.read(), self.content)

    @suite('integration')
    def test_init_with_output_path(self):
        path = OutputPath(self.test_dir, 'one', 'two', 'three')
        stream = OutputStreamSerializer(path)
        assert_equal(stream.base_path, str(path))

    def test_tail(self):
        self._write_contents()
        assert_equal(self.serial.tail(self.filename), self.expected)

    def test_tail_num_lines(self):
        self._write_contents()
        assert_equal(self.serial.tail(self.filename, 1), self.expected[-1:])

    def test_tail_file_does_not_exist(self):
        file_dne = 'bogusfile123'
        assert_equal(self.serial.tail(file_dne), [])


class TestOutputPath(TestCase):
    @setup
    def setup_path(self):
        self.path = OutputPath('one', 'two', 'three')

    def test__init__(self):
        assert_equal(self.path.base, 'one')
        assert_equal(self.path.parts, ['two', 'three'])

        path = OutputPath('base')
        assert_equal(path.base, 'base')
        assert_equal(path.parts, [])

    def test__iter__(self):
        assert_equal(list(self.path), ['one', 'two', 'three'])

    def test__str__(self):
        # Breaks in windows probably,
        assert_equal('one/two/three', str(self.path))

    def test_append(self):
        self.path.append('four')
        assert_equal(self.path.parts, ['two', 'three', 'four'])

    def test_clone(self):
        new_path = self.path.clone()
        assert_equal(str(new_path), str(self.path))

        self.path.append('alpha')
        assert_equal(str(new_path), 'one/two/three')

        new_path.append('beta')
        assert_equal(str(self.path), 'one/two/three/alpha')

    def test_clone_with_parts(self):
        new_path = self.path.clone('seven', 'ten')
        assert_equal(list(new_path), ['one/two/three', 'seven', 'ten'])

    def test_delete(self):
        tmp_dir = mkdtemp()
        path = OutputPath(tmp_dir)
        path.delete()
        assert not os.path.exists(tmp_dir)

    def test__eq__(self):
        other = mock.MagicMock(base='one', parts=['two', 'three'])
        assert_equal(self.path, other)

    def test__ne__(self):
        other = mock.MagicMock(base='one/two', parts=['three'])
        assert_not_equal(self.path, other)


if __name__ == "__main__":
    run()
