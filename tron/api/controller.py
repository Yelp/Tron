"""
Web Controllers for the API.
"""
import logging
import pkg_resources
import tron
from tron.config import schema


log = logging.getLogger(__name__)


class UnknownCommandError(Exception):
    """Exception raised when a controller received an unknown command."""


class JobCollectionController(object):

    # TODO: Use a JobCollection
    def __init__(self, mcp):
        self.mcp = mcp

    def handle_command(self, command):
        if command == 'disableall':
            self.disable_all()
            return "Disabled all jobs."

        if command == 'enableall':
            self.enable_all()
            return "Enabled all jobs."

        raise UnknownCommandError("Unknown command %s" % command)

    def disable_all(self):
        for job_scheduler in self.mcp.get_jobs():
            job_scheduler.disable()

    def enable_all(self):
        for job_scheduler in self.mcp.get_jobs():
            job_scheduler.enable()


# TODO: test
class ServiceInstanceController(object):

    def __init__(self, service_instance):
        self.service_instance = service_instance

    def handle_command(self, command):
        error_msg = "Instance could not be %s from state %s."
        if command == 'stop':
            if self.service_instance.stop():
                return "%s stopping." % self.service_instance
            return error_msg % ("stopped", self.service_instance.get_state())

        if command == 'start':
            if self.service_instance.start():
                return "%s starting." % self.service_instance
            return error_msg % ("started", self.service_instance.get_state())

        raise UnknownCommandError("Unknown command %s" % command)


# TODO: test
class ServiceController(object):

    def __init__(self, service):
        self.service = service

    def handle_command(self, command):
        if command == 'stop':
            self.service.disable()
            return "%s stopping." % self.service

        if command == 'start':
            self.service.enable()
            return "%s starting." % self.service

        raise UnknownCommandError("Unknown command %s" % command)

def format_seq(seq):
    return "\n# ".join(sorted(seq))

def format_mapping(mapping):
    seq = ("%-30s: %s" % (k, v) for k, v in sorted(mapping.iteritems()))
    return format_seq(seq)


class ConfigController(object):
    """Control config. Return config contents and accept updated configuration
    from the API.
    """

    TEMPLATE_FILE = 'named_config_template.yaml'

    TEMPLATE = pkg_resources.resource_string(tron.__name__, TEMPLATE_FILE)

    HEADER_END = TEMPLATE.split('\n')[-2] + '\n'

    DEFAULT_NAMED_CONFIG =  "\njobs:\n\nservices:\n"

    def __init__(self, mcp):
        self.mcp = mcp
        self.config_manager = mcp.get_config_manager()

    def render_template(self, config_content):
        container = self.config_manager.load()
        command_context = container.get_master().command_context or {}
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
        config_hash = self.config_manager.get_hash(name)

        if name != schema.MASTER_NAMESPACE:
            config_content = self.render_template(config_content)
        return dict(config=config_content, hash=config_hash)

    def update_config(self, name, content, config_hash):
        """Update a configuration fragment and reload the MCP."""
        if self.config_manager.get_hash(name) != config_hash:
            return "Configuration has changed. Please try again."
        content = self.strip_header(name, content)
        try:
            self.config_manager.write_config(name, content)
            self.mcp.reconfigure()
        except Exception, e:
            log.error("Configuration update failed: %s" % e)
            return str(e)
