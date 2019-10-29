#!/usr/bin/env python3.6
"""
Write pid and stdout/stderr to a standard location before execing a command.
"""
import argparse
import contextlib
import logging
import os
import subprocess
import sys
import threading
import time

from tron import yaml

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
            with open(self.filename, 'a') as fh:
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


def build_environment(run_id, original_env=None):
    if original_env is None:
        original_env = dict(os.environ)
    try:
        namespace, job, run_num, action = run_id.split('.', maxsplit=3)
    except ValueError:
        # if we can't parse the run_id, we don't want to abort, so just
        # set these semi-arbitrarily
        namespace, job, run_num, action = ['UNKNOWN'] * 4

    new_env = dict(original_env)
    new_env['TRON_JOB_NAMESPACE'] = namespace
    new_env['TRON_JOB_NAME'] = job
    new_env['TRON_RUN_NUM'] = run_num
    new_env['TRON_ACTION'] = action

    logging.debug(new_env)
    return new_env


def run_proc(output_path, command, run_id, proc):
    logging.warning(f'{run_id} running as pid {proc.pid}')
    status_file = StatusFile(os.path.join(output_path, STATUS_FILE))
    with status_file.wrap(
        command=command,
        run_id=run_id,
        proc=proc,
    ):
        returncode = proc.wait()
        logging.warning(f'pid {proc.pid} exited with returncode {returncode}')
        return returncode


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


def run_command(command, run_id):
    return subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=build_environment(run_id=run_id),
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


def configure_logging(run_id, output_dir):
    output_file = os.path.join(output_dir, f'{run_id}-{os.getpid()}.log')
    logging.basicConfig(
        filename=output_file,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S%z'
    )


def main():
    args = parse_args()
    validate_output_dir(args.output_dir)
    configure_logging(run_id=args.run_id, output_dir=args.output_dir)
    proc = run_command(command=args.command, run_id=args.run_id)
    threads = [
        threading.Thread(target=stream, args=p, daemon=True)
        for p in [(proc.stdout, sys.stdout), (proc.stderr, sys.stderr)]
    ]
    for t in threads:
        t.start()
    returncode = run_proc(
        output_path=args.output_dir,
        run_id=args.run_id,
        command=args.command,
        proc=proc,
    )

    for t in threads:
        t.join()

    return returncode


if __name__ == "__main__":
    sys.exit(main())
