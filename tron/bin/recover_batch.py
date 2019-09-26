#!/usr/bin/env python3.6
import argparse
import logging
import signal
import sys
from queue import Queue

import psutil
from twisted.internet import inotify
from twisted.internet import reactor
from twisted.python import filepath

from tron import yaml

log = logging.getLogger('tron.recover_batch')


class StatusFileWatcher(object):
    """
    Watches the status file produced by action runners
    """

    def __init__(self, to_watch, callback):
        notifier = inotify.INotify()
        notifier.startReading()
        notifier.watch(filepath.FilePath(to_watch), callbacks=[callback])


def parse_args():
    parser = argparse.ArgumentParser(
        description='Check if a action runner has exited; wait otherwise',
    )
    parser.add_argument('filepath', )
    return parser.parse_args()


def read_last_yaml_entries(filename):
    with open(filename) as f:
        lines = list(yaml.load_all(f))
        if not lines:
            entries = {}
        else:
            entries = lines[-1]
    return entries


def notify(notify_queue, ignored, filepath, mask):
    exit_code, error_message = get_exit_code(filepath.path)
    if exit_code is not None:
        reactor.stop()
        notify_queue.put((exit_code, error_message))


def get_exit_code(filepath):
    entries = read_last_yaml_entries(filepath)
    pid = entries.get('runner_pid')
    return_code = entries.get('return_code')
    exit_code, error_message = None, None

    if return_code is not None:
        if return_code < 0:
            # from the subprocess docs on the return code of a process:
            # "A negative value -N indicates that the child was terminated by signal N (POSIX only)."
            # We should always exit with a positive code, so we take the absolute value of the return code
            exit_code = abs(return_code)
            error_message = f'Action run killed by signal {signal.Signals(exit_code).name}'
        else:
            exit_code = return_code
    elif pid is None:
        log.warning(f"Status file {filepath} didn't have a PID. Will watch the file for updates.")
    elif not psutil.pid_exists(pid):
        exit_code = 1
        error_message = f'Action runner pid {pid} no longer running. Assuming an exit of 1.'

    return exit_code, error_message


def run(fpath):
    # Check if the process has already completed.
    # If it has, we don't expect any more updates.
    return_code, error_message = get_exit_code(fpath)
    if return_code is not None:
        if error_message is not None:
            log.warning(error_message)
        sys.exit(return_code)

    # If not, wait for updates to the file.
    notify_queue = Queue()
    StatusFileWatcher(
        fpath,
        lambda *args, **kwargs: notify(notify_queue, *args, **kwargs)
    )
    reactor.run()
    exit_code, error_message = notify_queue.get()
    if error_message is not None:
        log.warning(error_message)
    sys.exit(exit_code)


if __name__ == "__main__":
    args = parse_args()
    run(args.filepath)
