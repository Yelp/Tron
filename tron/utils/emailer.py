"""General email sending utilities"""

import logging
import smtplib
from email.mime.text import MIMEText
import getpass
import socket

log = logging.getLogger("tron.emailer")


class Error(Exception):
    pass


class Emailer(object):
    def __init__(self, smtp_host, notification_address):
        self.smtp_host = smtp_host
        self.to_addr = notification_address

    @property
    def from_addr(self):
        # TODO: Should probably allow this to be configured
        username = getpass.getuser()
        hostname = socket.gethostname()
        return "@".join((username, hostname))

    def send(self, content):
        msg = MIMEText(content)
        msg['Subject'] = "Tron Exception"
        msg['To'] = self.to_addr

        port = 25
        host_parts = self.smtp_host.split(":")
        host = host_parts.pop(0)
        if host_parts:
            port = int(host_parts.pop(0))
        if host_parts:
            raise Error("Invalid host name")

        log.info("Connecting to SMTP host %r %r", self.smtp_host, (host, port))

        s = smtplib.SMTP()
        s.connect(host, port)
        s.sendmail(self.from_addr, self.to_addr, msg.as_string())
        s.close()
