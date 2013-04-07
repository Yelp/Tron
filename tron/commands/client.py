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


USER_AGENT   = "Tron Command/%s +http://github.com/Yelp/Tron" % tron.__version__
DECODE_ERROR = "DECODE_ERROR"
URL_ERROR    = 'URL_ERROR'


class RequestError(ValueError):
    """Raised when the request to tron API fails."""


Response = namedtuple('Response', 'error msg content')

default_headers = {
    "User-Agent": USER_AGENT
}


def build_url_request(uri, data, headers=None):
    headers     = headers or default_headers
    enc_data    = urllib.urlencode(data) if data else None
    return urllib2.Request(uri, enc_data, headers)


def load_response_content(http_response):
    content = http_response.read()
    try:
        return Response(None, None, simplejson.loads(content))
    except ValueError, e:
        log.error("Failed to decode response: %s, %s", e, content)
        return Response(DECODE_ERROR, str(e), content)


def build_http_error_response(exc):
    content = exc.read() if hasattr(exc, 'read') else None
    return Response(exc.code, exc.reason, content)


def request(uri, data=None):
    log.info("Request to %s with %s", uri, data)
    request = build_url_request(uri, data)
    try:
        response = urllib2.urlopen(request)
    except urllib2.HTTPError, e:
        log.error("Received error response: %s" % e)
        return build_http_error_response(e)
    except urllib2.URLError, e:
        log.error("Received error response: %s" % e)
        return Response(URL_ERROR, e.reason, None)

    return load_response_content(response)


def build_get_url(url, data=None):
     return '%s?%s' % (url, urllib.urlencode(data)) if data else url


class Client(object):
    """An HTTP client used to issue commands to the Tron API.
    """

    def __init__(self, url_base):
        """Create a new client.
            url_base - A url with a schema, hostname and port
        """
        self.url_base = url_base

    def status(self):
        return self.http_get('/status')

    def events(self):
        return self.http_get('/events')['data']

    def config(self, config_name, config_data=None, config_hash=None):
        """Retrieve or update the configuration."""
        if config_data:
            request_data = dict(
                        config=config_data, name=config_name, hash=config_hash)
            return self.request('/config', request_data)
        return self.http_get('/config', dict(name=config_name))

    def home(self):
        return self.http_get('/')

    index = home

    def get_url(self, identifier):
        return get_object_type_from_identifier(self.index(), identifier).url

    def services(self):
        return self.http_get('/services').get('services')

    def service(self, service_url):
        return self.http_get(service_url)

    def jobs(self, include_job_runs=False, include_action_runs=False):
        params = {'include_job_runs': int(include_job_runs),
                  'include_action_runs': int(include_action_runs)}
        return self.http_get('/jobs', params).get('jobs')

    def job(self, job_url, include_action_runs=False):
        params = {'include_action_runs': int(include_action_runs)}
        return self.http_get(job_url, params)

    def job_runs(self, url, include_runs=True, include_graph=False):
        params = {
            'include_action_runs': int(include_runs),
            'include_action_graph': int(include_graph)}
        return self.http_get(url, params)

    def action_runs(self, action_run_url, num_lines=0):
        return self.http_get(action_run_url, dict(num_lines=num_lines))

    def object_events(self, item_url):
        return self.http_get('%s/_events' % item_url)['data']

    def http_get(self, url, data=None):
        return self.request(build_get_url(url, data))

    def request(self, url, data=None):
        uri = urlparse.urljoin(self.url_base, url)
        log.info("Request: %s, %s", uri, data)
        response = request(uri, data)
        if response.error:
            raise RequestError("%s: %s" % (uri, response))
        return response.content


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
