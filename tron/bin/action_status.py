#!/usr/bin/env python
"""
Read values from a status file created by action_runner.py
"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import logging
import os
import signal

from tron import yaml

log = logging.getLogger('tron.action_status')

STATUS_FILE = 'status'


def get_field(field, status_file):
    docs = yaml.load_all(status_file.read())
    content = list(docs)[-1]
    return content.get(field)


def print_status_file(status_file):
    for line in status_file.readlines():
        print(yaml.load(line))


def send_signal(signal_num, status_file):
    pid = get_field('pid', status_file)
    if pid:
        try:
            os.killpg(os.getpgid(pid), signal_num)
        except OSError as e:
            msg = "Failed to signal %s with %s: %s"
            raise SystemExit(msg % (pid, signal_num, e))


commands = {
    'print':
        print_status_file,
    'pid':
        lambda statusfile: print(get_field('pid', statusfile)),
    'return_code':
        lambda statusfile: print(get_field('return_code', statusfile)),
    'terminate':
        lambda statusfile: send_signal(signal.SIGTERM, statusfile),
    'kill':
        lambda statusfile: send_signal(signal.SIGKILL, statusfile),
}


def parse_args():
    parser = argparse.ArgumentParser(description='Action Status for Tron')
    parser.add_argument(
        'output_dir',
        help='The directory where the state of the action run is',
    )
    parser.add_argument(
        'command',
        help='the command to run',
    )
    parser.add_argument(
        'run_id',
        help='run_id of the action',
    )
    return parser.parse_args()


def run_command(command, status_file):
    commands[command](status_file)


if __name__ == "__main__":
    logging.basicConfig()
    args = parse_args()
    with open(os.path.join(args.output_dir, STATUS_FILE)) as f:
        run_command(args.command, f)
