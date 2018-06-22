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
    logging.warning(f'{run_id} running as pid {proc.pid}')
    status_file = get_status_file(output_path)
    with status_file.wrap(
        command=command,
        run_id=run_id,
        proc=proc,
    ):
        returncode = proc.wait()
        logging.warning(f'pid {proc.pid} exited with returncode {returncode}')
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


def stream(source, dst):
    is_connected = True
    logging.warning(f'streaming {source.name} to {dst.name}')
    for line in iter(source.readline, b''):
        if is_connected:
            try:
                dst.write(line.decode('utf-8'))
                dst.flush()
                logging.warning(f'{dst.name}: {line}')
            except Exception as e:
                logging.warning(f'failed writing to {dst}: {e}')
                logging.warning(f'{dst.name}: {line}')
                is_connected = False
        else:
            logging.warning(f'{dst.name}: {line}')
            is_connected = False


def configure_logging(run_id):
    logging.basicConfig(filename=f'/tmp/{run_id}.{os.getpid()}.log')


if __name__ == "__main__":
    args = parse_args()
    configure_logging(args.run_id)
    proc = run_command(args.command)
    for p in [(proc.stdout, sys.stdout), (proc.stderr, sys.stderr)]:
        t = threading.Thread(target=stream, args=p, daemon=True)
        t.start()

    run_proc(
        output_path=args.output_dir,
        run_id=args.run_id,
        command=args.command,
        proc=proc,
    )
