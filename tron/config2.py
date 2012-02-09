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
        s = string_or_file.read()
    else:
        s = string_or_file
    s = TAG_RE.sub('', s)

    config = yaml.safe_load(s)
    return tron_config(config)


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


def tron_config(config):
    valid_config = {}

    def store(key, validate_function):
        valid_config[key] = validate_function(config.get(key, None))

    store('working_dir', valid_working_dir)
    store('syslog_address', valid_syslog)
    store('command_context', valid_command_context)
    store('ssh_options', valid_ssh_options)
    store('notification_options', valid_notification_options)
    store('time_zone', valid_time_zone)
    store('nodes', valid_nodes)
    store('jobs', valid_jobs)
    store('services', valid_services)

    return valid_config


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


def valid_nodes(nodes):
    return nodes


def valid_jobs(jobs):
    return jobs


def valid_services(services):
    return services
