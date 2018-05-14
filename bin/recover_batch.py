#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import logging
import sys

import psutil
import yaml
from twisted.internet import inotify
from twisted.internet import reactor
from twisted.python import filepath

log = logging.getLogger(__name__)


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


def notify(ignored, filepath, mask):
    with open(filepath.path) as f:
        last_entry = f.readlines()[-1]
        x = yaml.load(last_entry)
        return_code = x.get('return_code')
        if return_code is not None:
            reactor.stop()
            sys.exit(return_code)


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
        log.info(
            "action_runner pid %d no longer running; unable to recover batch" %
            runner_pid
        )
        #TODO: should we kill the process here?
        sys.exit(1)

    watcher = StatusFileWatcher(args.filepath, notify)
    reactor.run()
