#!/usr/bin/env python
"""
Write pid and stdout/stderr to a standard location before execing a command.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib
import logging
import os
import subprocess
import sys

import yaml


log = logging.getLogger("tron.action_runner")


STATUS_FILE = 'status'


opener = open


class StatusFile(object):
    """Manage a status file."""

    def __init__(self, filename):
        self.filename = filename

    def write(self, command, proc):
        with opener(self.filename, 'w') as fh:
            yaml.dump(self.get_content(command, proc), fh)

    def get_content(self, command, proc):
        return {
            'command':      command,
            'pid':          proc.pid,
            'return_code':  proc.returncode,
        }

    @contextlib.contextmanager
    def wrap(self, command, proc):
        self.write(command, proc)
        try:
            yield
        finally:
            self.write(command, proc)


class NoFile(object):

    @classmethod
    @contextlib.contextmanager
    def wrap(self, _command, _proc):
        yield


def get_status_file(output_path):
    if not os.path.isdir(output_path):
        try:
            os.makedirs(output_path)
        except OSError:
            log.warn("Output path %s does not exist", output_path)
            return NoFile
    return StatusFile(os.path.join(output_path, STATUS_FILE))


def register(output_path, command, proc):
    status_file = get_status_file(output_path)
    with status_file.wrap(command, proc):
        proc.wait()
    sys.exit(proc.returncode)


def parse_args(args):
    if len(args) != 3:
        raise SystemExit("Requires both output_path and command.")
    return args[1:]


def run_command(command):
    return subprocess.Popen(
        command, shell=True, stdout=sys.stdout, stderr=sys.stderr,
    )


if __name__ == "__main__":
    logging.basicConfig()
    output_path, command = parse_args(sys.argv)
    proc = run_command(command)
    register(output_path, command, proc)
