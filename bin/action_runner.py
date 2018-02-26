#!/usr/bin/env python
"""
Write pid and stdout/stderr to a standard location before execing a command.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import contextlib
import logging
import os
import subprocess
import sys

import yaml

log = logging.getLogger("tron.action_runner")


STATUS_FILE = 'status'


class StatusFile(object):
    """Manage a status file."""

    def __init__(self, filename):
        self.filename = filename

    def get_content(self, run_id, command, proc):
        return {
            'run_id':       run_id,
            'command':      command,
            'pid':          proc.pid,
            'return_code':  proc.returncode,
        }

    @contextlib.contextmanager
    def wrap(self, command, run_id, proc):
        with open(self.filename, 'w') as fh:
            yaml.safe_dump(
                self.get_content(
                    run_id=run_id,
                    command=command,
                    proc=proc,
                ), fh,
            )
            try:
                yield
            finally:
                yaml.safe_dump(
                    self.get_content(
                        run_id=run_id,
                        command=command,
                        proc=proc,
                    ), fh,
                )


def get_status_file(output_path):
    if os.path.isdir(output_path):
        if not os.access(output_path, os.W_OK):
            raise OSError("Output dir %s not writable" % output_path)
        return StatusFile(os.path.join(output_path, STATUS_FILE))
    else:
        try:
            os.makedirs(output_path)
        except OSError:
            raise OSError("Could not create output dir %s" % output_path)
        return StatusFile(os.path.join(output_path, STATUS_FILE))


def run_proc(output_path, command, run_id, proc):
    status_file = get_status_file(output_path)
    with status_file.wrap(
        command=command,
        run_id=run_id,
        proc=proc,
    ):
        proc.wait()
    sys.exit(proc.returncode)


def parse_args():
    parser = argparse.ArgumentParser(description='Action Runner for Tron')
    parser.add_argument(
        'output_dir',
        help='an integer for the accumulator',
    )
    parser.add_argument(
        'command',
        help='the command to run',
    )
    parser.add_argument(
        'run_id',
        help='run_id of the process',
    )
    return parser.parse_args()


def run_command(command):
    return subprocess.Popen(
        command, shell=True, stdout=sys.stdout, stderr=sys.stderr,
    )


if __name__ == "__main__":
    logging.basicConfig()
    args = parse_args()
    proc = run_command(args.command)
    run_proc(
        output_path=args.output_dir,
        run_id=args.run_id,
        command=args.command,
        proc=proc,
    )
