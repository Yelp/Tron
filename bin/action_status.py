#!/usr/bin/env python
"""
Read values from a status file created by action_runner.py
"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os
import signal
import sys

from tron import yaml

log = logging.getLogger('tron.action_status')

STATUS_FILE = 'status'


def get_field(field, status_file):
    lines = status_file.readlines()
    content = yaml.load(lines[-1])
    return content.get(field)


def print_status_file(status_file):
    for line in status_file.readlines():
        print(yaml.load(line))


def send_signal(signal_num, status_file):
    pid = get_field('pid', status_file)
    if pid:
        try:
            os.kill(pid, signal_num)
        except OSError as e:
            msg = "Failed to signal %s with %s: %s"
            raise SystemExit(msg % (pid, signal_num, e))


commands = {
    'print':        print_status_file,
    'pid': lambda statusfile: print(get_field('pid', statusfile)),
    'return_code': lambda statusfile: print(get_field('return_code', statusfile)),
    'terminate': lambda statusfile: send_signal(signal.SIGTERM. statusfile),
    'kill': lambda statusfile: send_signal(signal.SIGKILL, statusfile),
}


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
    with open(os.path.join(path, STATUS_FILE)) as f:
        run_command(command, f)
