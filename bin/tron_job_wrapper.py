#!/usr/bin/env python
"""
Write pid and stdout/stderr to a standard location before execing a command.
"""
import logging
import os.path
import shlex
import subprocess
import sys
import yaml

logging.basicConfig()


log = logging.getLogger("tron.job_wrapper")


STATUS_FILE = 'status'
STDOUT = 'stdout'
STDERR = 'stderr'


# TODO: move to lib?
class Tee(object):
    """Write data to two files."""
    def __init__(self, first, second):
        self.first = first
        self.second = second

    def write(self, data):
        self.first.write(data)
        self.second.write(data)

    def close(self):
        try:
            self.first.close()
        finally:
            self.second.close()


def create_status_file_content(command, pid, return_code=None):
    return {
        'command': command,
        'pid': pid,
        'return_code': return_code
    }

def write_status_file(status_filename, content):
    with open(status_filename, 'w') as fh:
        yaml.dump(content, fh)


# TODO: buffering
def tee_output_streams(output_path, proc):
    stdout_path = os.path.join(output_path, STDOUT)
    stderr_path = os.path.join(output_path, STDERR)
    stdout = Tee(open(stdout_path, 'w'), sys.stdout)
    stderr = Tee(open(stderr_path, 'w'), sys.stderr)
    while True:
        stdout.write(proc.stdout.read())
        stderr.write(proc.stderr.read())
        if proc.poll() is not None:
            return
    # TODO: close


def register(output_path, command, proc):
    if not os.path.isdir(output_path):
        log.warn("Output path %s does not exist", output_path)
        return

    status_filename = os.path.join(output_path, STATUS_FILE)
    status_content = create_status_file_content(command, proc.pid)
    write_status_file(status_filename, status_content)

    tee_output_streams(output_path, proc)

    status_content = create_status_file_content(command, None, proc.returncode)
    write_status_file(status_filename, status_content)



def parse_args(args):
    if len(args) != 3:
        raise SystemExit("Requires both output_path and command.")
    return args[1:]


def run_command(command):
    command = shlex.split(command)
    return subprocess.Popen(command,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)


if __name__ == "__main__":
    output_path, command = parse_args(sys.argv)
    proc = run_command(command)
    register(output_path, command, proc)
