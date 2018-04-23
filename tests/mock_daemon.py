"""
 A mock daemon for testing service handling.
"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import sys
import time

import daemon

from tron.trondaemon import PIDFile


def do_main_program():
    while True:
        print("ok")
        time.sleep(2)


if __name__ == "__main__":
    filename = sys.argv[1] if len(sys.argv) > 1 else None
    pidfile = pidfile = PIDFile(filename or '/tmp/mock_daemon.pid')
    with daemon.DaemonContext(
        pidfile=pidfile,
        files_preserve=[pidfile.lock.file],
    ):
        do_main_program()
