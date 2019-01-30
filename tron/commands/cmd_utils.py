"""
Common code for command line utilities (see bin/)
"""
from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import with_statement

import argparse
import difflib
import logging
import os
import sys

import tron
from tron import yaml
from tron.commands.client import Client

log = logging.getLogger("tron.commands")


class ExitCode(object):
    """Enumeration of exit status codes."""
    success = 0
    fail = 1


GLOBAL_CONFIG_FILE_NAME = os.environ.get(
    'TRON_CONFIG',
) or "/etc/tron/tron.yaml"
CONFIG_FILE_NAME = os.path.expanduser('~/.tron')

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8089

DEFAULT_CONFIG = {
    'server': "http://%s:%d" % (DEFAULT_HOST, DEFAULT_PORT),
    'display_color': False,
}

TAB_COMPLETE_FILE = '/var/cache/tron_tab_completions'

opener = open


def get_default_server():
    return DEFAULT_CONFIG['server']


def filter_jobs_actions_runs(prefix, inputs):
    dots = prefix.count('.')
    if prefix == "":
        # If the user hasn't begun to type anything, we need to get them started with all jobs
        return [i for i in inputs if i.count('.') == 1]
    elif dots == 0:
        # If the user hasn't completed a job, we need to get them started with all jobs
        # that start with what they have
        return [
            i for i in inputs if i.count('.') == 1 and i.startswith(prefix)
        ]
    elif prefix in inputs:
        # If what a user typed is exactly what is already in a suggestion, then we need to give them
        # Even more suggestions (+1)
        return [
            i for i in inputs if i.startswith(prefix) and
            (i.count('.') == dots or i.count('.') == dots + 1)
        ]
    else:
        # Otherwise we only want to scope our suggestions to those that are on the same "level"
        # which in string form means they have the same number of dots
        return [
            i for i in inputs if i.startswith(prefix) and i.count('.') == dots
        ]


def tron_jobs_completer(prefix, **kwargs):
    if os.path.isfile(TAB_COMPLETE_FILE):
        with opener(TAB_COMPLETE_FILE, 'r') as f:
            jobs = f.readlines()
        return filter_jobs_actions_runs(
            prefix=prefix,
            inputs=[job.strip('\n\r') for job in jobs],
        )
    else:
        if 'client' not in kwargs:
            client = Client(get_default_server())
        else:
            client = kwargs['client']
        return filter_jobs_actions_runs(
            prefix=prefix,
            inputs=[job['name'] for job in client.jobs()],
        )


def build_option_parser(usage=None, epilog=None):
    parser = argparse.ArgumentParser(
        usage=usage,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--version',
        action='version',
        version="%s %s" % (parser.prog, tron.__version__),
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        help="Verbose logging",
        default=None,
    )
    parser.add_argument(
        "--server",
        default=None,
        help="Url including scheme, host and port, Default: %(default)s",
    )
    parser.add_argument(
        "-s",
        "--save",
        action="store_true",
        dest="save_config",
        help="Save options used on this job for next time.",
    )

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
    if options.verbose is None:
        level = logging.CRITICAL
    elif options.verbose == 1:
        level = logging.WARNING
    elif options.verbose == 2:
        level = logging.INFO
    else:
        level = logging.NOTSET

    logging.basicConfig(
        level=level,
        format='%(name)s %(levelname)s %(message)s',
        stream=sys.stdout,
    )


def suggest_possibilities(word, possibilities, max_suggestions=6):
    suggestions = difflib.get_close_matches(
        word=word, possibilities=possibilities, n=max_suggestions
    )
    if len(suggestions) == 1:
        return f"\nDid you mean: {suggestions[0]}?"
    elif len(suggestions) >= 1:
        return f"\nDid you mean one of: {', '.join(suggestions)}?"
    else:
        return ""
