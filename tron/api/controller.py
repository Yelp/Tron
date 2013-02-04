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

    def __init__(self, mcp):
        self.mcp = mcp
        self.config_manager = mcp.get_config_manager()

    def read_config(self, name):
        return self.config_manager.read_raw_config(name)

    def update_config(self, name, content):
        """Update a configuration fragment and reload the MCP."""
        try:
            self.config_manager.write_config(name, content)
            self.mcp.reconfigure()
        except Exception, e:
            log.error("Configuration update failed: %s" % e)
            return str(e)