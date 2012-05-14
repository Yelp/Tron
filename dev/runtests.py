"""Run tests when a file changes."""
import os
import time
import subprocess
import optparse

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class TestRunner(object):

    def __init__(self, basepath):
        self.basepath = basepath
        self.test_exec = ['testify', '-v', '--summary']

    def testable_file(self, filename):
        return filename.endswith('.py') and filename.startswith(self.basepath)

    def start(self, filename):
        if not self.testable_file(filename):
            print "File not testable %s." % filename
            return

        test_filename = self.get_test_filename(filename)
        if not os.path.isfile(test_filename):
            print "Test missing %s." % test_filename
            return

        test_name = self.get_test_name(test_filename)
        self.run_test(test_name)

    def run_test(self, test_name):
        subprocess.call(self.test_exec + [test_name])

    def get_test_filename(self, filename):
        # Strip basepath
        test_name = filename[len(self.basepath)+1:]
        test_path = test_name.split('/')
        if not test_path[0] == 'tests':
            test_path[0] = 'tests'
            test_path[-1] = test_path[-1][:-3] + '_test.py'
        return "/".join(test_path)

    def get_test_name(self, filename):
        # Strip .py
        filename = filename[:-3]
        return ".".join(filename.split('/'))


class FileModifiedHandler(FileSystemEventHandler):

    def __init__(self, test_runner):
        super(FileModifiedHandler, self).__init__()
        self.test_runner = test_runner

    def on_modified(self, event):
        if event.is_directory:
            return
        self.test_runner.start(event.src_path)


def parse_args():
    parser = optparse.OptionParser()
    parser.add_option('-p', '--path',
        help='The filepath to watch.', default='.')
    parser.add_option('-b', '--basepath',
        help='The base path for the repository, if it differs from path.')
    parser.add_option('--no-recursive', default=False, action='store_true')
    opts, args = parser.parse_args()

    opts.path = os.path.abspath(opts.path)
    opts.basepath = os.path.abspath(opts.basepath or opts.path)
    print opts.path
    return opts, args


if __name__ == "__main__":
    opts, _ = parse_args()
    test_runner = TestRunner(opts.basepath)
    event_handler = FileModifiedHandler(test_runner)
    observer = Observer()

    observer.schedule(
            event_handler, path=opts.path, recursive=not opts.no_recursive)
    observer.start()

    try:
        while True:
            time.sleep(999)
    finally:
        observer.stop()
        observer.join()
