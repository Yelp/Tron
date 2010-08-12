"""
Common code for command line utilities (see bin/)
"""
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
    
    if os.path.exists(file_name):
        config = yaml.load(open(file_name, "r"))
        options.server = options.server or config.get('server')

def save_config(options):
    file_name = os.path.expanduser(CONFIG_FILE_NAME)
    
    if os.path.exists(file_name):
        config_file = open(file_name, "r")
        config = yaml.load(config_file)
        config_file.close()
    else:
        config = {}

    config['server'] = options.server
    
    config_file = open(file_name, "w")
    yaml.dump(config, config_file)
    config_file.close()

def request(serv, path, data=None):
    enc_data = None
    if data:
        enc_data = urllib.urlencode(data)

    request = urllib2.Request(urlparse.urljoin(serv, path), enc_data)

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

