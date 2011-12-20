"""The default logging module doesn't provide a way to re-open log files based
on SIGHUP
"""

import logging


class ReOpeningFileHandler(logging.FileHandler):

    def __init__(self, filename, mode='a', encoding=None):
        self.filename = filename
        self.mode = mode
        self.stream = None
        self.open_stream()

        logging.StreamHandler.__init__(self, self.stream)

    def open_stream(self):
        assert self.stream is None
        self.stream = open(self.filename, self.mode)

    def close_stream(self):
        self.flush()
        self.stream.close()
        self.stream = None

    def reopen(self):
        self.close_stream()
        self.open_stream()
