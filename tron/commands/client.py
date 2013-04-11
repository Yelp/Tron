"""
A command line http client used by tronview, tronctl, and tronfig
"""
from collections import namedtuple
import logging
import urllib
import urllib2
import urlparse
import itertools
import tron
from tron.config.schema import MASTER_NAMESPACE

try:
    import simplejson
    assert simplejson # Pyflakes
except ImportError:
    import json as simplejson

log = logging.getLogger(__name__)

USER_AGENT = "Tron Command/%s +http://github.com/Yelp/Tron" % tron.__version__

# Result Codes
OK          = "OK"
ERROR       = "ERROR"


def request(host, path, data=None):
    enc_data = urllib.urlencode(data) if data else data

    uri = urlparse.urljoin(host, path)
    req = urllib2.Request(uri, enc_data)
    log.info("Request to %r", uri)

    req.add_header("User-Agent", USER_AGENT)
    opener = urllib2.build_opener()
    try:
        page = opener.open(req)
        contents = page.read()
    except urllib2.HTTPError, e:
        log.error("Received error response: %s" % e)
        return ERROR, e.code
    except urllib2.URLError, e:
        log.error("Received error response: %s" % e)
        return ERROR, e.reason

    try:
        result = simplejson.loads(contents)
    except ValueError, e:
        log.error("Failed to decode response: %s, %s" % (e, contents))
        return ERROR, str(e)
    return OK, result


class RequestError(ValueError):
    """Raised when there is a connection failure."""


# TODO: remove options, replace with explicit args
class Client(object):
    """A client used in commands to make requests to the tron.www """

    def __init__(self, options):
        self.options = options

    def status(self):
        return self.request('/status')

    def events(self):
        return self.request('/events')['data']

    def config(self, config_name, config_data=None, config_hash=None):
        """This may be a post or a get, depending on data."""
        if config_data:
            request_data = dict(
                        config=config_data, name=config_name, hash=config_hash)
            return self.request('/config', request_data)
        return self.request('/config?name=%s' % config_name)

    def home(self):
        return self.request('/')

    index = home

    def get_url(self, identifier):
        return get_object_type_from_identifier(self.index(), identifier).url

    def services(self):
        return self.request('/services').get('services')

    def service(self, service_url):
        return self.request(service_url)

    def _get_job_params(self, include_job_runs=False, include_action_runs=False):
        # TODO: remove
        if self.options.warn:
            return "?include_job_runs=1&include_action_runs=1"
        # TODO: test, todo, parse bool
        params = {
            'include_job_runs': int(include_job_runs),
            'include_action_runs': int(include_action_runs) }
        return '?' + urllib.urlencode(params)

    def jobs(self):
        params = self._get_job_params()
        return self.request('/jobs' + params).get('jobs')

    def job(self, job_url):
        params = self._get_job_params(include_job_runs=True)
        return self.request('%s%s' % (job_url, params))

    def job_runs(self, job_run_url):
        params = self._get_job_params()
        return self.request('%s%s' % (job_run_url, params))

    def action(self, action_run_url):
        url = "%s?num_lines=%s" % (action_run_url, self.options.num_displays)
        return self.request(url)

    def object_events(self, item_url):
        return self.request('%s/_events' % item_url)['data']

    def request(self, url, data=None):
        server = self.options.server
        log.info("Request: %s, %s, %s", server, url, data)
        status, content = request(server, url, data)
        if not status == OK:
            err_msg = "%s%s: %s"
            raise RequestError(err_msg % (server, url, content))
        log.info("Response: %s", content)
        return content


class TronObjectType(object):
    """Constants to identify a Tron object type."""
    job              = 'JOB'
    job_run          = 'JOB_RUN'
    action_run       = 'ACTION_RUN'
    service          = 'SERVICE'
    service_instance = 'SERVICE_INSTANCE'

    groups = {
        'jobs':     [job, job_run, action_run],
        'services': [service, service_instance]
    }


TronObjectIdentifier = namedtuple('TronObjectIdentifier', 'type name url')

IdentifierParts = namedtuple('IdentifierParts', 'name path length')


def get_object_type_from_identifier(url_index, identifier):
    """Given a string identifier, return a TronObjectIdentifier.
    """
    def get_name_parts(identifier, namespace=None):
        if namespace:
            identifier = '%s.%s' % (namespace, identifier)

        name_elements       = identifier.split('.')
        name                = '.'.join(name_elements[:2])
        length              = len(name_elements) - 2
        relative_path       = '/'.join(name_elements[2:])
        return IdentifierParts(name, relative_path, length)

    def find_by_type(id_parts, index_name):
        url_type_index = url_index[index_name]
        if id_parts.name in url_type_index:
            tron_type = TronObjectType.groups[index_name][id_parts.length]
            url = '%s/%s' % (url_type_index[id_parts.name], id_parts.path)
            return TronObjectIdentifier(tron_type, id_parts.name, url)

    def find_by_name(name, namespace=None):
        id = get_name_parts(name, namespace)
        return find_by_type(id, 'jobs') or find_by_type(id, 'services')

    def first(seq):
        for item in itertools.ifilter(None, seq):
            return item

    namespaces = [None, MASTER_NAMESPACE] + url_index['namespaces']
    id_obj = first(find_by_name(identifier, name) for name in namespaces)
    if id_obj:
        return id_obj

    raise ValueError("Unknown identifier: %s" % identifier)
