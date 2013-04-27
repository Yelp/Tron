"""
Common code for command line utilities (see bin/)
"""
from __future__ import with_statement
import logging
import optparse
import os
import sys

import yaml
import tron


log = logging.getLogger("tron.commands")


class ExitCode(object):
    """Enumeration of exit status codes."""
    success =           0
    fail =              1


GLOBAL_CONFIG_FILE_NAME = os.environ.get('TRON_CONFIG') or "/etc/tron/tron.yaml"
CONFIG_FILE_NAME = os.path.expanduser('~/.tron')

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8089

DEFAULT_CONFIG = {
    'server':           "http://%s:%d" % (DEFAULT_HOST, DEFAULT_PORT),
    'display_color':    False,
}


opener = open


def build_option_parser(usage, parser_class=optparse.OptionParser):
    parser = parser_class(usage, version="%%prog %s" % tron.__version__)

    parser.add_option("-v", "--verbose", action="count",
        help="Verbose logging", default=None)
    parser.add_option("--server", default=None,
        help="Url including scheme, host and port, Default: %default")
    parser.add_option("-s", "--save", action="store_true", dest="save_config",
        help="Save options used on this job for next time.")

    return parser


def get_client_config():
    config_file_list = [CONFIG_FILE_NAME, GLOBAL_CONFIG_FILE_NAME]
    for config_file in config_file_list:
        filename = os.path.expanduser(config_file)
        if os.access(filename, os.R_OK):
            config = read_config(filename)
            if config:
                return config

    log.debug("Could not find a config in: %s." % ', '.join(config_file_list))
    return {}


def load_config(options):
    """Attempt to load a user specific configuration or a global config file
    and set any unset options based on values from the config. Finally fallback
    to DEFAULT_CONFIG for those settings.

    Also save back options to the config if options.save_config is True.
    """
    config = get_client_config()

    for opt_name in DEFAULT_CONFIG.keys():
        if not hasattr(options, opt_name):
            continue

        if getattr(options, opt_name) is not None:
            continue

        default_value = DEFAULT_CONFIG[opt_name]
        setattr(options, opt_name, config.get(opt_name, default_value))

    if options.save_config:
        save_config(options)


def read_config(filename=CONFIG_FILE_NAME):
    try:
        with opener(filename, 'r') as config_file:
            return yaml.load(config_file)
    except (IOError, OSError):
        log.info("Failed to read config file: %s" % CONFIG_FILE_NAME)
    return {}


def write_config(config):
    with open(CONFIG_FILE_NAME, "w") as config_file:
        yaml.dump(config, config_file)


def save_config(options):
    config = read_config()
    for opt_name in DEFAULT_CONFIG.keys():
        if not hasattr(options, opt_name):
            continue
        config[opt_name] = getattr(options, opt_name)
    write_config(config)


def setup_logging(options):
    level = logging.INFO if options.verbose else logging.WARNING

    logging.basicConfig(
        level=level,
        format='%(name)s %(levelname)s %(message)s',
        stream=sys.stdout)