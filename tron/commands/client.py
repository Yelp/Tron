"""
A command line http client used by tronview, tronctl, and tronfig
"""
from collections import namedtuple
import logging
import urllib
import urllib2
import urlparse
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


# TODO: remove options, plreace with explicit args
class Client(object):
    """A client used in commands to make requests to the tron.www """

    def __init__(self, options):
        self.options = options

    def status(self):
        return self.request('/status')

    def events(self):
        return self.request('/events')['data']

    def config(self, config_name, data=None):
        """This may be a post or a get, depending on data."""
        if data:
            return self.request('/config', dict(config=data, name=config_name))
        return self.request('/config?name=%s' % config_name)['config']

    def home(self):
        return self.request('/')

    def index(self):
        content = self.home()

        def name_href_dict(source):
            return dict((i['name'], i['href']) for i in source)

        return {
            'jobs':     name_href_dict(content['jobs']),
            'services': name_href_dict(content['services'])
        }

    def services(self):
        return self.request('/services').get('services')

    def service(self, service_id):
        service_url = "/services/%s" % service_id
        return self.request(service_url)

    def service_events(self, service_id):
        service_url = "/services/%s/_events" % service_id
        return self.request(service_url)['data']

    def _get_job_params(self):
        if self.options.warn:
            return "?include_job_runs=1&include_action_runs=1"
        return ''

    def jobs(self):
        params = self._get_job_params()
        return self.request('/jobs' + params).get('jobs')

    def job(self, job_id):
        params = self._get_job_params()
        return self.request('/jobs/%s%s' % (job_id, params))

    def job_events(self, job_id):
        return self.request('/jobs/%s/_events' % job_id)['data']

    def job_runs(self, action_id):
        params = self._get_job_params()
        action_id = action_id.replace('.', '/')
        return self.request('/jobs/%s%s' % (action_id, params))

    def action(self, action_id):
        url = "/jobs/%s?num_lines=%s" % (action_id.replace('.', '/'),
            self.options.num_displays)
        return self.request(url)

    def action_events(self, action_id):
        action_id = action_id.replace('.', '/')
        return self.request('/jobs/%s/_events' % action_id)['data']

    def request(self, url, data=None):
        server = self.options.server
        status, content = request(server, url, data)
        if not status == OK:
            err_msg = "%s%s: %s"
            raise RequestError(err_msg % (server, url, content))
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


def get_object_type_from_identifier(url_index, identifier):
    """Given a string identifier, return a TronObjectIdentifier.
    """
    name_elements       = identifier.split('.')
    identifier_length   = len(name_elements) - 1
    obj_name            = name_elements[0]
    relative_path       = '/'.join(name_elements[1:])

    def full_url(obj_url):
        return '%s/%s' % (obj_url, relative_path)

    def find_by_type(name, index_name):
        url_type_index = url_index[index_name]
        if name in url_type_index:
            tron_type = TronObjectType.groups[index_name][identifier_length]
            url = full_url(url_type_index[name])
            return TronObjectIdentifier(tron_type, name, url)

    def find_by_name(name):
        return find_by_type(name, 'jobs') or find_by_type(name, 'services')

    # TODO: include a list of namespaces in the index so that a job can be
    # found in any namespace
    default_name = '%s_%s' % (MASTER_NAMESPACE, obj_name)
    type_url = find_by_name(obj_name) or find_by_name(default_name)
    if type_url:
        return type_url

    raise ValueError("Unknown identifier: %s" % identifier)
