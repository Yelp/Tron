"""
 A mock daemon for testing service handling.
"""
import daemon
import sys
import time
from tron.trondaemon import PIDFile

def do_main_program():
    while True:
        print "ok"
        time.sleep(2)


if __name__ == "__main__":
    pid_file = sys.argv[1]
    with daemon.DaemonContext(pidfile=PIDFile(pid_file)):
        do_main_program()
