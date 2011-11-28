"""
Common code for command line utilities (see bin/)
"""

import logging
import os
import os.path
import urllib
import urllib2
import urlparse
import sys

try:
    import simplejson
except ImportError:
    import json as simplejson

import yaml

USER_AGENT = "Tron Command/1.0 +http://github.com/Yelp/Tron"
CONFIG_FILE_NAME = "~/.tron"

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8089

DEFAULT_SERVER = "http://%s:%d" % (DEFAULT_HOST, DEFAULT_PORT)

# Result Codes
OK = "OK"
REDIRECT = "REDIRECT"
ERROR = "ERROR"


log = logging.getLogger("tron.cmd")


def load_config(options):
    file_name = os.path.expanduser(CONFIG_FILE_NAME)
    if not os.access(file_name, os.R_OK):
        log.debug("Config file %s doesn't yet exist", file_name)
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


def request(serv, path, data=None):
    enc_data = None
    if data:
        enc_data = urllib.urlencode(data)

    uri = urlparse.urljoin(serv, path)
    request = urllib2.Request(uri, enc_data)
    log.info("Request to %r", uri)
    # Which is the proper way to encode data ?
    # if data:
    #     request.add_data(urllib.urlencode(data))

    request.add_header("User-Agent", USER_AGENT)
    opener = urllib2.build_opener()
    try:
        output = opener.open(request)
    except urllib2.HTTPError, e:
        log.error("Recieved error response: %s" % e)
        return ERROR, e.code
    except urllib2.URLError, e:
        log.error("Recieved error response: %s" % e)
        return ERROR, e.reason

    result = simplejson.load(output)
    return OK, result


def setup_logging(options):
    if options.verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    logging.basicConfig(level=level,
                        format='%(name)s %(levelname)s %(message)s',
                        stream=sys.stdout)


def make_job_to_uri(content):
    """Use ``content`` (the result of the '/' API call) to generate a dict
    mapping job names to URIs
    """
    return dict([(job['name'], job['href']) for job in content['jobs']])


def make_service_to_uri(content):
    """Use ``content`` (the result of the '/' API call) to generate a dict
    mapping service names to URIs
    """
    return dict([(service['name'], service['href'])
                 for service in content['services']])


def obj_spec_to_uri(obj_spec, job_to_uri, service_to_uri):
    """Convert a string of the form job_name[.run_number[.action]] to its
    corresponding URL
    """
    obj_name_elements = obj_spec.split('.')
    obj_name = obj_name_elements[0]
    obj_rel_path = "/".join(obj_name_elements[1:])

    obj_uri = None
    if obj_name in job_to_uri:
        obj_uri = job_to_uri[obj_name]
    elif obj_name in service_to_uri:
        obj_uri = service_to_uri[obj_name]

    if not obj_uri:
        raise Exception("Unknown identifier")

    return '/'.join((obj_uri, obj_rel_path))
