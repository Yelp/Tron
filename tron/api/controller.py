"""
 Controllers for the API to perform actions on POSTs.
"""
import logging
import os

import yaml

from tron.config import ConfigError
from tron.config.config_parse import valid_config
from tron.config.schema import MASTER_NAMESPACE

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

    # TODO: This could really use a permissions manager. The fact that
    # this can flatten existing configuration files without validation
    # is more than somewhat worrying.
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
            # Parse the original config and the update
            if os.path.exists(self.filepath):
                with open(self.filepath, 'r') as config:
                    original = yaml.safe_load(config)

                    # Forward-convert legacy configurations
                    # TODO: Make legacy detection non-reliant on side
                    # effects
                    if MASTER_NAMESPACE not in original:
                        original = {MASTER_NAMESPACE: original}
            else:
                original = {}
            update = yaml.safe_load(content)

            # Verify the update is a valid configuration
            assert valid_config(update)

            # Get the namespace for the update
            namespace = update.get("config_name")
            if not namespace:
                namespace = MASTER_NAMESPACE

                # TODO: Remove the duplicate entry for config_name, by
                # relaxing the __new__ needs of our class builder.
                update['config_name'] = MASTER_NAMESPACE

            # Update the namespace key within the original object
            original[namespace] = update
            
            # Write it back to the original file location
            with open(self.filepath, 'w') as config:
                yaml.dump(original, config)

            return True
        except (OSError, IOError, ConfigError, yaml.YAMLError), e:
            log.error("Configuration update failed: %s" % e)
            return False
