"""
Common code for command line utilities (see bin/)
"""
from __future__ import with_statement
import logging
import os
import os.path
import sys

import yaml


GLOBAL_CONFIG_FILE_NAME = os.environ.get('TRON_CONFIG') or "/etc/tron/tron.yaml"
CONFIG_FILE_NAME = "~/.tron"

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8089

DEFAULT_CONFIG = {
    'server':           "http://%s:%d" % (DEFAULT_HOST, DEFAULT_PORT),
    'display_color':    False,
}

log = logging.getLogger("tron.cmd")


def load_config(options):
    """Attempt to load a user specific configuration or a global config file
    and set any unset options based on values from the config. Finally fallback
    to DEFAULT_CONFIG for those settings.
    """
    config = {}
    config_file_list = [CONFIG_FILE_NAME, GLOBAL_CONFIG_FILE_NAME]
    for config_file in config_file_list:
        file_name = os.path.expanduser(config_file)
        if os.access(file_name, os.R_OK):
            try:
                with open(file_name, "r") as config_file:
                    config = yaml.load(config_file)
                break
            except IOError, e:
                log.error("Failure loading config file: %r", e)

    else:
        log.debug("Could not find a config in: %s." % ", ".join(config_file_list))

    for opt_name in DEFAULT_CONFIG.keys():
        if not hasattr(options, opt_name):
            continue

        if getattr(options, opt_name) is not None:
            continue

        default_value = DEFAULT_CONFIG[opt_name]
        setattr(options, opt_name, config.get(opt_name, default_value))


def save_config(options):
    file_name = os.path.expanduser(CONFIG_FILE_NAME)

    try:
        with open(file_name, "r") as config_file:
            config = yaml.load(config_file)
    except IOError:
        log.info("Failed to locate an existing config file %s" % file_name)
        config = None

    config = config or {}
    for opt_name in DEFAULT_CONFIG.keys():
        if not hasattr(options, opt_name):
            continue
        config[opt_name] = getattr(options, opt_name)

    with open(file_name, "w") as config_file:
        yaml.dump(config, config_file)


def setup_logging(options):
    level = logging.INFO if options.verbose else logging.WARNING

    logging.basicConfig(
        level=level,
        format='%(name)s %(levelname)s %(message)s',
        stream=sys.stdout
    )
