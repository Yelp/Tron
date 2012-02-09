"""Rewritten config system. Functionality is limited to parsing and schema
validation.
"""

import logging
import os
import re

import pytz
from twisted.conch.client import options
import yaml

log = logging.getLogger("tron.config")

CLEANUP_ACTION_NAME = "cleanup"


TAG_RE = re.compile(r'!\w+\b')

def load_config(string_or_file):
    # Hackishly strip !Tags from the file so PyYAML doesn't try to make them
    # into Python objects
    if not isinstance(string_or_file, basestring):
        s_tags = string_or_file.read()
    else:
        s_tags = string_or_file
    s_notags = TAG_RE.sub('', s_tags)

    if len(s_tags) > len(s.notags):
        log.warn('Tron no longer uses !Tags to parse config files. Please'
                 ' remove them from yours.')

    # safe_load disables python classes
    config = yaml.safe_load(s_notags)

    return valid_config(config)


class ConfigError(Exception):
    pass


### VALIDATION ###


def type_converter(convert, error_fmt):
    def f(path, value):
        try:
            return convert(value)
        except TypeError:
            raise ConfigError(error_fmt % (path, value))
        return value
    return f

valid_float = type_converter(
    float, 'Value at %s is not a number: %s')

valid_int = type_converter(
    int, 'Value at %s is not an integer: %s')


def type_validator(validator, error_fmt):
    def f(path, value):
        if not validator(value):
            raise ConfigError(error_fmt, (path, value))
        return value
    return f

valid_str = type_validator(
    lambda s: isinstance(s, basestring),
    'Value at %s is not a string: %s')

valid_list = type_validator(
    lambda s: isinstance(s, list),
    'Value at %s is not a list: %s')

valid_dict = type_validator(
    lambda s: isinstance(s, dict),
    'Value at %s is not a dictionary: %s')


### LOADING THE SCHEMA ###


def valid_config(config):
    """Given a parsed config file (should be only basic literals and
    containers), return a fully populated dictionary with all defaults filled
    in, all valid values, and no unused values. Throws a ConfigError if any
    part of the input dict is invalid.
    """
    final_config = {}

    def store(key, validate_function):
        """If key is in config, pass it through validate_function(), store the
        result in final_config, and delete the key from config. Otherwise, pass
        None through validate_function() and store that result in final_config.
        This way we can keep the defaults logic where it makes sense.
        """
        if key in config:
            final_config[key] = validate_function(key)
            del final_config[key]
        else:
            final_config[key] = validate_function(None)

    store('working_dir', valid_working_dir)
    store('syslog_address', valid_syslog)
    store('command_context', valid_command_context)
    store('ssh_options', valid_ssh_options)
    store('notification_options', valid_notification_options)
    store('time_zone', valid_time_zone)

    final_config['nodes'] = []
    final_config['node_pools'] = []

    # If no nodes, use localhost
    if 'nodes' not in config:
        config['nodes'] = [{'hostname': 'localhost'}]

    # 'nodes' may contain nodes or node pools (for now). Internally split them
    # into 'nodes' and 'node_pools'.
    for node in config['nodes']:
        if isinstance(node, list):
            log.warn('Node pools should be moved from "nodes" to "node_pools"'
                     ' before upgrading to Tron 0.5.')
            # this is actually a node pool, process it later
            config['node_pools'].append(node)
            continue
        else:
            final_config['nodes'].append(valid_node(node))
    del config['nodes']

    # process node pools
    for node_pool in config['node_pools']:
        final_config['node_pools'].append(valid_node_pool(node_pool))
    del config['node_pools']

    # process jobs
    final_config['jobs'] = [valid_job(job)
                            for job in config['jobs']]
    del config['jobs']

    # process services
    final_config['services'] = [valid_service(service)
                                for service in config['services']]
    del config['services']

    # Make sure we used everything
    if len(config) > 0:
        raise ConfigError("Unknown options: %s" % ', '.join(config.keys()))

    return final_config


def valid_working_dir(wd):
    """Given a working directory or None, return a valid working directory.
    If wd=None, try os.environ['TMPDIR']. If that doesn't exist, use /tmp.
    """
    if not wd:
        if 'TMPDIR' in os.environ:
            wd = os.environ['TMPDIR']
        else:
            wd = '/tmp'

    if not os.path.isdir(wd):
        raise ConfigError("Specified working directory \'%s\' is not a"
                          " directory" % wd)

    if not os.access(wd, os.W_OK):
        raise ConfigError("Specified working directory \'%s\' is not"
                          " writable" % wd)

    return wd


def valid_syslog(syslog):
    if syslog:
        syslog = valid_str('syslog', syslog)

    return syslog


def valid_command_context(context):
    return valid_dict('command_context', context or {})


def valid_ssh_options(opts):
    ssh_options = options.ConchOptions()
    if opts.get('agent', False):
        if 'SSH_AUTH_SOCK' in os.environ:
            ssh_options['agent'] = True
        else:
            raise ConfigError("No SSH Agent available ($SSH_AUTH_SOCK)")
    else:
        ssh_options['noagent'] = True

    if 'identities' in opts:
        for file_name in opts['identities']:
            file_path = os.path.expanduser(file_name)
            if not os.path.exists(file_path):
                raise ConfigError("Private key file '%s' doesn't exist" %
                                  file_name)
            if not os.path.exists(file_path + ".pub"):
                raise ConfigError("Public key '%s' doesn't exist" %
                                  (file_name + ".pub"))

            ssh_options.opt_identity(file_name)

    extra_keys = set(opts.keys()) - set(['agent', 'identities'])
    if extra_keys:
        raise ConfigError("Unknown SSH options: %s" %
                          ', '.join(list(extra_keys)))

    return ssh_options


def valid_notification_options(options):
    if options is None:
        return None
    else:
        if 'smtp_host' not in options:
            raise ConfigError("smtp_host required")
        if 'notification_addr' not in options:
            raise ConfigError("notification_addr required")

    return options


def valid_time_zone(tz):
    if tz is None:
        return None
    else:
        try:
            return pytz.timezone(valid_str('time_zone', tz))
        except pytz.exceptions.UnknownTimeZoneError:
            raise ConfigError('%s is not a valid time zone' % tz)


def valid_node(node):
    return node


def valid_job(job):
    return job


def valid_service(service):
    return service
