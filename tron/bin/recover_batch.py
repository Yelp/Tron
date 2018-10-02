#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import logging
import signal
import sys
from queue import Queue

import psutil
import yaml
from twisted.internet import inotify
from twisted.internet import reactor
from twisted.python import filepath

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


def notify(notify_queue, ignored, filepath, mask):
    with open(filepath.path) as f:
        last_entry = f.readlines()[-1]
        x = yaml.load(last_entry)
        return_code = x.get('return_code')
        if return_code is not None:
            reactor.stop()
            notify_queue.put(return_code)


def get_key_from_last_line(filepath, key):
    with open(filepath) as f:
        lines = f.readlines()
        if lines:
            content = yaml.load(lines[-1])
            return content.get(key)
        return None


if __name__ == "__main__":
    args = parse_args()
    existing_return_code = get_key_from_last_line(args.filepath, 'return_code')
    if existing_return_code is not None:
        sys.exit(existing_return_code)

    runner_pid = get_key_from_last_line(args.filepath, 'runner_pid')
    if not psutil.pid_exists(runner_pid):
        log.warning(
            "action_runner pid %d no longer running; unable to recover batch" %
            runner_pid
        )
        #TODO: should we kill the process here?
        sys.exit(1)

    exit_code_queue = Queue()
    watcher = StatusFileWatcher(
        args.filepath,
        lambda *args, **kwargs: notify(exit_code_queue, *args, **kwargs)
    )
    reactor.run()
    exit_code = exit_code_queue.get()
    assert type(exit_code) == int
    if exit_code < 0:
        # from the subprocess docs on the return code of a process:
        # "A negative value -N indicates that the child was terminated by signal N (POSIX only)."
        # We should always exit with a positive code, so we take the absolute value of the returncode
        log.warning(
            f'action run killed by signal {signal.Signals(abs(exit_code)).name}'
        )
        exit_code = abs(exit_code)
    sys.exit(exit_code)
