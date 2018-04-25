from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from twisted.python import log

from tron import event

logger = logging.getLogger(__name__)


class CrashReporter(object):
    """Observer for twisted events that can send emails on crashes

    Based on twisted.log.PythonLoggingObserver
    """

    def __init__(self, emailer):
        self.emailer = emailer
        self.event_recorder = event.get_recorder(str(self))

    def _get_level(self, event_dict):
        """Returns the logging level for an event."""
        if 'logLevel' in event_dict:
            return event_dict['logLevel']
        if event_dict['isError']:
            return logging.ERROR
        return logging.INFO

    def emit(self, event_dict):
        text = log.textFromEventDict(event_dict)
        if text is None:
            return

        if text == "Unhandled error in Deferred:":
            # This annoying error message is just a pre-cursor to an actual
            # error message, so filter it out.
            return None

        if self._get_level(event_dict) < logging.ERROR:
            return

        try:
            self.event_recorder.critical("crash", msg=text)
            self.emailer.send(text)

        except Exception:
            logger.exception("Error sending notification")
            self.event_recorder.critical("email_failure", msg=text)

    def start(self):
        log.addObserver(self.emit)

    def stop(self):
        log.removeObserver(self.emit)

    def __str__(self):
        return 'CrashReporter'
