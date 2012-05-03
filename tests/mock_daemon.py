"""
 A mock daemon for testing service handling.
"""
import daemon
import time
from tron.trondaemon import PIDFile




def do_main_program():
    while True:
        print "ok"
        time.sleep(2)


with daemon.DaemonContext(pidfile=PIDFile('/tmp/mock_daemon.pid')):
    do_main_program()