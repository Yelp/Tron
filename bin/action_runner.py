#!/usr/bin/env python3.6
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
import threading
import time

import yaml

log = logging.getLogger("tron.action_runner")

STATUS_FILE = 'status'


class StatusFile(object):
    """Manage a status file."""

    def __init__(self, filename):
        self.filename = filename

    def get_content(self, run_id, command, proc):
        return {
            'run_id': run_id,
            'command': command,
            'pid': proc.pid,
            'return_code': proc.returncode,
            'runner_pid': os.getpid(),
            'timestamp': time.time(),
        }

    @contextlib.contextmanager
    def wrap(self, command, run_id, proc):
        with open(self.filename, 'w') as fh:
            yaml.safe_dump(
                self.get_content(
                    run_id=run_id,
                    command=command,
                    proc=proc,
                ),
                fh,
                explicit_start=True,
                width=1000000,
            )
            try:
                yield
            finally:
                yaml.safe_dump(
                    self.get_content(
                        run_id=run_id,
                        command=command,
                        proc=proc,
                    ),
                    fh,
                    explicit_start=True,
                    width=1000000,
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
        returncode = proc.wait()
        log.warn(f'pid {proc.pid} exited with returncode {returncode}')
        sys.exit(returncode)


def parse_args():
    parser = argparse.ArgumentParser(description='Action Runner for Tron')
    parser.add_argument(
        'output_dir',
        help='The directory to store the state of the action run',
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


def run_command(command):
    return subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def stdout_reader(proc):
    for line in iter(proc.stdout.readline, b''):
        sys.stdout.write(line.decode('utf-8'))
        sys.stdout.flush()


def stderr_reader(proc):
    for line in iter(proc.stderr.readline, b''):
        sys.stderr.write(line.decode('utf-8'))
        sys.stdout.flush()


if __name__ == "__main__":
    logging.basicConfig()
    args = parse_args()
    proc = run_command(args.command)
    stdout_printer_t = threading.Thread(
        target=stdout_reader, args=(proc, ), daemon=True
    )
    stderr_printer_t = threading.Thread(
        target=stderr_reader, args=(proc, ), daemon=True
    )
    stdout_printer_t.start()
    stderr_printer_t.start()
    run_proc(
        output_path=args.output_dir,
        run_id=args.run_id,
        command=args.command,
        proc=proc,
    )
