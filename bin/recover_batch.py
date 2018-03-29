#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse

import yaml
from twisted.internet import inotify
from twisted.internet import reactor
from twisted.python import filepath


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
        x = yaml.loads(last_entry)
        return_code = x['return_code']
        if return_code:
            exit(return_code)


def get_existing_return_code(filepath):
    with open(filepath) as f:
        lines = f.readlines()
        if lines:
            content = yaml.load(lines[-1])
            if content['return_code'] is not None:
                return content['return_code']
            else:
                return None
        return None


if __name__ == "__main__":
    args = parse_args()
    existing_return_code = get_existing_return_code(args.filepath)
    if existing_return_code is not None:
        exit(existing_return_code)
    watcher = StatusFileWatcher("/tmp", notify)
    reactor.run()
