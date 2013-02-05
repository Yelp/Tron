"""
Web Controllers for the API.
"""
import logging
import pkg_resources
import tron
from tron.config import schema


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


def format_seq(seq):
    return "\n# ".join(sorted(seq))

def format_mapping(mapping):
    seq = ("%-30s: %s" % (k, v) for k, v in sorted(mapping.iteritems()))
    return format_seq(seq)


class ConfigController(object):
    """Control config."""

    TEMPLATE_FILE = 'named_config_template.yaml'

    TEMPLATE = pkg_resources.resource_string(tron.__name__, TEMPLATE_FILE)

    HEADER_END = '#' * 80 + '\n'

    DEFAULT_NAMED_CONFIG =  "\njobs:\n\nservices:\n"

    def __init__(self, mcp):
        self.mcp = mcp
        self.config_manager = mcp.get_config_manager()

    def render_template(self, config_content):
        container = self.config_manager.load()
        command_context = container.get_master().command_context
        context = {
            'node_names': format_seq(container.get_node_names()),
            'command_context': format_mapping(command_context)}
        return self.TEMPLATE % context + config_content

    def strip_header(self, name, content):
        if name == schema.MASTER_NAMESPACE:
            return content

        header_end_index = content.find(self.HEADER_END)
        if header_end_index > -1:
            return content[header_end_index + len(self.HEADER_END):]
        return content

    def _get_config_content(self, name):
        if name not in self.config_manager:
            return self.DEFAULT_NAMED_CONFIG
        return self.config_manager.read_raw_config(name)

    def read_config(self, name):
        config_content = self._get_config_content(name)

        if name == schema.MASTER_NAMESPACE:
            return config_content
        return self.render_template(config_content)

    def update_config(self, name, content):
        """Update a configuration fragment and reload the MCP."""
        content = self.strip_header(name, content)
        try:
            self.config_manager.write_config(name, content)
            self.mcp.reconfigure()
        except Exception, e:
            log.error("Configuration update failed: %s" % e)
            return str(e)