import logging
import weakref

from twisted.python import log

from tron import event


logger = logging.getLogger("tron.monitor")


class CrashReporter(object):
    """Observer for twisted events that can send emails on crashes

    Based on twisted.log.PythonLoggingObserver
    """

    def __init__(self, emailer, mcp):
        self.emailer = emailer
        self.event_recorder = event.EventRecorder(
            self, parent=mcp.event_recorder)

    def emit(self, eventDict):
        if 'logLevel' in eventDict:
            level = eventDict['logLevel']
        elif eventDict['isError']:
            level = logging.ERROR
        else:
            level = logging.INFO
        text = log.textFromEventDict(eventDict)

        if text is None:
            return

        if text == "Unhandled error in Deferred:":

            # This annoying error message is just a pre-cursor to an actual
            # error message, so filter it out.
            return None

        if level >= logging.ERROR:
            try:
                self.event_recorder.emit_critical("crash", msg=text)

                self.emailer.send(text)
            except Exception:
                logger.exception("Error sending notification")
                self.event_recorder.emit_critical("email_failure", msg=text)

    def start(self):
        log.addObserver(self.emit)

    def stop(self):
        log.removeObserver(self.emit)

    def __str__(self):
        return "CRASH"
