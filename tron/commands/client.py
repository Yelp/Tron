"""
A command line http client used by tronview, tronctl, and tronfig
"""
import logging
import urllib.error
import urllib.parse
import urllib.request
from collections import namedtuple

import tron
from tron.config.schema import MASTER_NAMESPACE

try:
    import simplejson
    assert simplejson  # Pyflakes
except ImportError:
    import json as simplejson

log = logging.getLogger(__name__)

USER_AGENT = f"Tron Command/{tron.__version__} +http://github.com/Yelp/Tron"
DECODE_ERROR = "DECODE_ERROR"
URL_ERROR = 'URL_ERROR'


class RequestError(ValueError):
    """Raised when the request to tron API fails."""


Response = namedtuple('Response', 'error msg content')

default_headers = {
    "User-Agent": USER_AGENT,
}


def build_url_request(uri, data, headers=None, method=None):
    headers = headers or default_headers
    enc_data = urllib.parse.urlencode(data).encode() if data else None
    return urllib.request.Request(uri, enc_data, headers=headers, method=method)


def load_response_content(http_response):
    encoding = http_response.headers.get_content_charset()
    if encoding is None:
        encoding = 'utf8'
    content = http_response.read().decode(encoding)
    try:
        return Response(None, None, simplejson.loads(content))
    except ValueError as e:
        log.error("Failed to decode response: %s, %s", e, content)
        return Response(DECODE_ERROR, str(e), content)


def build_http_error_response(exc):
    content = exc.read() if hasattr(exc, 'read') else None
    if content:
        encoding = exc.headers.get_content_charset()
        if encoding is None:
            encoding = 'utf8'
        content = content.decode(encoding)
        try:
            content = simplejson.loads(content)
            content = content['error']
        except ValueError:
            log.warning(
                "Incorrectly formatted error response: {}".format(content),
            )
    return Response(exc.code, exc.msg, content)


def request(uri, data=None, headers=None, method=None):
    log.info("Request to %s with %s", uri, data)
    request = build_url_request(uri, data, headers=headers, method=method)
    try:
        response = urllib.request.urlopen(request)
    except urllib.error.HTTPError as e:
        log.error("Received error response: %s" % e)
        return build_http_error_response(e)
    except urllib.error.URLError as e:
        log.error("Received error response: %s" % e)
        return Response(URL_ERROR, e.reason, None)

    return load_response_content(response)


def build_get_url(url, data=None):
    if data:
        query_str = urllib.parse.urlencode(sorted(data.items()))
        return f'{url}?{query_str}'
    else:
        return url


class Client(object):
    """An HTTP client used to issue commands to the Tron API.
    """

    def __init__(self, url_base):
        """Create a new client.
            url_base - A url with a schema, hostname and port
        """
        self.url_base = url_base

    def status(self):
        return self.http_get('/api/status')

    def metrics(self):
        return self.http_get('/api/metrics')

    def config(
        self,
        config_name,
        config_data=None,
        config_hash=None,
        check=False,
    ):
        """Retrieve or update the configuration."""
        if config_data is not None:
            data_check = 1 if check else 0
            request_data = dict(
                config=config_data,
                name=config_name,
                hash=config_hash,
                check=data_check,
            )
            return self.request('/api/config', request_data)
        request_data = dict(name=config_name)
        return self.http_get('/api/config', request_data)

    def home(self):
        return self.http_get('/api/')

    index = home

    def get_url(self, identifier):
        return get_object_type_from_identifier(self.index(), identifier).url

    def jobs(
        self,
        include_job_runs=False,
        include_action_runs=False,
        include_action_graph=True,
        include_node_pool=True,
    ):
        params = {
            'include_job_runs': int(include_job_runs),
            'include_action_runs': int(include_action_runs),
            'include_action_graph': int(include_action_graph),
            'include_node_pool': int(include_node_pool),
        }
        return self.http_get('/api/jobs', params).get('jobs')

    def job(self, job_url, include_action_runs=False, count=0):
        params = {
            'include_action_runs': int(include_action_runs),
            'num_runs': count,
        }
        return self.http_get(job_url, params)

    def job_runs(self, url, include_runs=True, include_graph=False):
        params = {
            'include_action_runs': int(include_runs),
            'include_action_graph': int(include_graph),
        }
        return self.http_get(url, params)

    def action_runs(self, action_run_url, num_lines=0):
        params = {
            'num_lines': num_lines,
            'include_stdout': 1,
            'include_stderr': 1,
        }
        return self.http_get(action_run_url, params)

    def http_get(self, url, data=None):
        return self.request(build_get_url(url, data))

    def request(self, url, data=None):
        log.info(f'Request: {self.url_base}, {url}, {data}')
        uri = urllib.parse.urljoin(self.url_base, url)
        response = request(uri, data)
        if response.error:
            if response.content:
                raise RequestError(response.content)
            else:
                raise RequestError(f'{response.error} {response.msg}')
        return response.content


def build_api_url(resource, identifier_parts):
    return '/api/%s/%s' % (resource, '/'.join(identifier_parts))


def split_identifier(identifier):
    return identifier.rsplit('.', identifier.count('.') - 1)


def get_job_url(identifier):
    return build_api_url('jobs', split_identifier(identifier))


class TronObjectType(object):
    """Constants to identify a Tron object type."""
    job = 'JOB'
    job_run = 'JOB_RUN'
    action_run = 'ACTION_RUN'

    url_builders = {
        'jobs': get_job_url,
    }

    groups = {
        'jobs': [job, job_run, action_run],
    }


TronObjectIdentifier = namedtuple('TronObjectIdentifier', 'type url')

IdentifierParts = namedtuple('IdentifierParts', 'name full_id length')


def first(seq):
    for item in filter(None, seq):
        return item


def get_object_type_from_identifier(url_index, identifier):
    """Given a string identifier, return a TronObjectIdentifier. """
    name_mapping = {
        'jobs': set(url_index['jobs']),
    }

    def get_name_parts(identifier, namespace=None):
        if namespace:
            identifier = f'{namespace}.{identifier}'

        name_elements = identifier.split('.')
        name = '.'.join(name_elements[:2])
        length = len(name_elements) - 2
        return IdentifierParts(name, identifier, length)

    def find_by_type(id_parts, index_name):
        url_type_index = name_mapping[index_name]
        if id_parts.name in url_type_index:
            tron_type = TronObjectType.groups[index_name][id_parts.length]
            url = TronObjectType.url_builders[index_name](id_parts.full_id)
            return TronObjectIdentifier(tron_type, url)

    def find_by_name(name, namespace=None):
        id = get_name_parts(name, namespace)
        return find_by_type(id, 'jobs')

    namespaces = [None, MASTER_NAMESPACE] + url_index['namespaces']
    id_obj = first(find_by_name(identifier, name) for name in namespaces)
    if id_obj:
        return id_obj

    raise ValueError("Unknown job identifier: %s" % identifier)
