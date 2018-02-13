#!/usr/bin/env python
"""
Read values from a status file created by action_runner.py
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import functools
import logging
import os
import signal
import sys

from tron import yaml


log = logging.getLogger('tron.action_status')


STATUS_FILE = 'status'


def print_field(field, status_file):
    sys.stdout.write(str(status_file[field]))


def print_status_file(status_file):
    yaml.dump(status_file, sys.stdout)


def send_signal(signal_num, status_file):
    pid = status_file['pid']
    try:
        os.kill(pid, signal_num)
    except OSError as e:
        msg = "Failed to signal %s with %s: %s"
        raise SystemExit(msg % (pid, signal_num, e))


commands = {
    'print':        print_status_file,
    'pid':          functools.partial(print_field, 'pid'),
    'return_code':  functools.partial(print_field, 'return_code'),
    'terminate':    functools.partial(send_signal, signal.SIGTERM),
    'kill':         functools.partial(send_signal, signal.SIGKILL),
}


def get_status_file(path):
    with open(path, 'r') as fh:
        return yaml.load(fh)


def parse_args(args):
    if len(args) != 3:
        raise SystemExit("Field and path are required")

    if args[2] not in commands:
        raise SystemExit("Unknown command %s" % args[2])

    return args[1:]


def run_command(command, status_file):
    commands[command](status_file)


if __name__ == "__main__":
    logging.basicConfig()
    path, command = parse_args(sys.argv)
    status_file = get_status_file(os.path.join(path, STATUS_FILE))
    run_command(command, status_file)
