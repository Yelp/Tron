"""
Common code for command line utilities (see bin/)
"""

import logging
import os
import os.path
import sys

import yaml


GLOBAL_CONFIG_FILE_NAME = os.environ.get('TRON_CONFIG') or "/etc/tron/tron.yaml"
CONFIG_FILE_NAME = "~/.tron"

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8089

DEFAULT_SERVER = "http://%s:%d" % (DEFAULT_HOST, DEFAULT_PORT)

log = logging.getLogger("tron.cmd")


def load_config(options):
    for config_file in [CONFIG_FILE_NAME, GLOBAL_CONFIG_FILE_NAME]:
        file_name = os.path.expanduser(config_file)
        if os.access(file_name, os.R_OK):
            break
    else:
        log.debug("Could not find a config.")
        options.server = options.server or DEFAULT_SERVER
        return

    try:
        config = yaml.load(open(file_name, "r"))
        options.server = options.server or config.get('server', DEFAULT_SERVER)
    except IOError, e:
        log.error("Failure loading config file: %r", e)


def save_config(options):
    file_name = os.path.expanduser(CONFIG_FILE_NAME)

    try:
        config_file = open(file_name, "r")
        config = yaml.load(config_file)
        config_file.close()
    except IOError:
        config = {}

    config['server'] = options.server

    config_file = open(file_name, "w")
    yaml.dump(config, config_file)
    config_file.close()


def setup_logging(options):
    if options.verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    logging.basicConfig(level=level,
                        format='%(name)s %(levelname)s %(message)s',
                        stream=sys.stdout)
