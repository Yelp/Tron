"""
Common code for command line utilities (see bin/)
"""
import sys
import os.path
import urllib2
import urllib
import urlparse
import logging

import simplejson
import yaml

USER_AGENT = "Tron View/1.0 +http://github.com/Yelp/Tron"
CONFIG_FILE_NAME = "~/.tron"

# Result Codes
OK = "OK"
REDIRECT = "REDIRECT"
ERROR = "ERROR"

log = logging.getLogger("tron.cmd")

def load_config(options):
    file_name = os.path.expanduser(CONFIG_FILE_NAME)
    
    try:
        config = yaml.load(open(file_name, "r"))
        options.server = options.server or config.get('server')
    except IOError:
        log.error("Cannot open config file")

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
    
    
    
