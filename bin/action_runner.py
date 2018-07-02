#!/usr/bin/env python3.6
"""
Write pid and stdout/stderr to a standard location before execing a command.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import contextlib
import functools
import logging
import os
import signal
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


def validate_output_dir(path):
    if os.path.isdir(path):
        if not os.access(path, os.W_OK):
            raise OSError("Output dir %s not writable" % path)
        return
    else:
        try:
            os.makedirs(path)
        except OSError:
            raise OSError("Could not create output dir %s" % path)


def run_proc(output_path, command, run_id, proc, kill_info):
    logging.warning(f'{run_id} running as pid {proc.pid}')
    status_file = StatusFile(os.path.join(output_path, STATUS_FILE))
    with status_file.wrap(
        command=command,
        run_id=run_id,
        proc=proc,
    ):
        while True:
            if proc.poll():
                returncode = proc.returncode
                logging.warning(
                    f'pid {proc.pid} exited with returncode {returncode}'
                )
                return
            elif kill_info[0].is_set():
                print('is set!')
                proc.send_signal(kill_info[1])
                return


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


def stream(source, dst, close_event):
    is_connected = True
    logging.warning(f'streaming {source.name} to {dst.name}')
    source = iter(source.readline, b'')
    while True:
        if close_event.is_set():
            print('should finish here')
            break
        try:
            line = next(source)
            time.sleep(1)
        except StopIteration:
            continue

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

    print(f'stopping {dst}')
    logging.warning(f'stopping streaming of {dst}')
    dst.flush()


def configure_logging(run_id, output_dir):
    output_file = os.path.join(output_dir, f'{run_id}-{os.getpid()}.log')
    logging.basicConfig(
        filename=output_file,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S%z'
    )


def exit_handler(killtuple, signum, frame):
    killtuple[0].set()
    killtuple[1] = signum


if __name__ == "__main__":
    args = parse_args()
    validate_output_dir(args.output_dir)
    configure_logging(run_id=args.run_id, output_dir=args.output_dir)
    proc = run_command(args.command)

    close_event = threading.Event()

    kill_info = [close_event, None]

    _exit_handler = functools.partial(exit_handler, kill_info)

    signal.signal(signal.SIGTERM, _exit_handler)
    signal.signal(signal.SIGKILL, exit_handler)

    threads = [
        threading.Thread(target=stream, args=p)
        for p in [(proc.stdout, sys.stdout,
                   close_event), (proc.stderr, sys.stderr, close_event)]
    ]
    for t in threads:
        t.start()

    run_proc(
        output_path=args.output_dir,
        run_id=args.run_id,
        command=args.command,
        proc=proc,
        kill_info=kill_info
    )

    for t in threads:
        t.join()
