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


def notify(notify_queue, ignored, filepath, mask):
    with open(filepath.path) as f:
        last_line = f.readlines()[-1]
        entries = yaml.load(last_line)

        pid = entries.get('runner_pid')
        return_code = entries.get('return_code')
        exit_code, error_msg = None, None

        if return_code is not None:
            if return_code < 0:
                # from the subprocess docs on the return code of a process:
                # "A negative value -N indicates that the child was terminated by signal N (POSIX only)."
                # We should always exit with a positive code, so we take the absolute value of the return code
                exit_code = abs(return_code)
                error_msg = (
                    'Action run killed by signal '
                    f'{signal.Signals(exit_code).name}'
                )
            else:
                exit_code = return_code

        elif not psutil.pid_exists(pid):
            exit_code = 1
            error_msg = (
                f'Action runner pid {pid} no longer running; '
                'unable to recover it'
            )

        if exit_code is not None:
            reactor.stop()
            notify_queue.put((exit_code, error_msg))


def get_key_from_last_line(filepath, key):
    with open(filepath) as f:
        lines = f.readlines()
        if lines:
            content = yaml.load(lines[-1])
            return content.get(key)
        return None


def run(fpath):
    existing_return_code = get_key_from_last_line(fpath, 'return_code')
    if existing_return_code is not None:
        sys.exit(existing_return_code)

    runner_pid = get_key_from_last_line(fpath, 'runner_pid')
    if not psutil.pid_exists(runner_pid):
        log.warning(
            f"Action runner pid {runner_pid} no longer running; "
            "unable to recover it"
        )
        #TODO: should we kill the process here?
        sys.exit(1)

    notify_queue = Queue()
    StatusFileWatcher(
        fpath,
        lambda *args, **kwargs: notify(notify_queue, *args, **kwargs)
    )
    reactor.run()
    exit_code, error_msg = notify_queue.get()
    if error_msg is not None:
        log.warning(error_msg)
    sys.exit(exit_code)


if __name__ == "__main__":
    args = parse_args()
    run(args.filepath)
