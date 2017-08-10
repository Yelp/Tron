from __future__ import absolute_import
from __future__ import unicode_literals

from twisted.python import log

observer = log.PythonLoggingObserver()
observer.start()
