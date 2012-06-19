"""
 Controllers for the API to perform actions on POSTs.
"""
import logging

log = logging.getLogger(__name__)


class JobController(object):
    """Control Jobs."""

    # TODO: just take a list of Jobs
    def __init__(self, mcp):
        self.mcp = mcp

    def disable_all(self):
        for job_scheduler in self.mcp.get_jobs():
            job_scheduler.disable()

    def enable_all(self):
        for job_scheduler in self.mcp.get_jobs():
            job_scheduler.enable()


class ConfigController(object):
    """Control config."""

    def __init__(self, filepath):
        self.filepath = filepath

    def read_config(self):
        try:
            with open(self.filepath, 'r') as config:
                return config.read()
        except (OSError, IOError), e:
            log.error("Failed to open configuration file: %s" % e)

    def rewrite_config(self, content):
        try:
            with open(self.filepath, 'w') as config:
                config.write(content)
            return True
        except (OSError, IOError), e:
            log.error("Failed to write to configuration file: %s" % e)
