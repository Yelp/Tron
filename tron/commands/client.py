"""
A command line http client used by tronview, tronctl, and tronfig
"""
import logging
import urllib
import urllib2
import urlparse

try:
    import simplejson
    assert simplejson # Pyflakes
except ImportError:
    import json as simplejson

log = logging.getLogger("tron.commands.client")

USER_AGENT = "Tron Command/1.0 +http://github.com/Yelp/Tron"

# Result Codes
OK = "OK"
REDIRECT = "REDIRECT"
ERROR = "ERROR"


def request(host, path, data=None):
    enc_data = urllib.urlencode(data) if data else data

    uri = urlparse.urljoin(host, path)
    req = urllib2.Request(uri, enc_data)
    log.info("Request to %r", uri)

    req.add_header("User-Agent", USER_AGENT)
    opener = urllib2.build_opener()
    try:
        output = opener.open(req)
    except urllib2.HTTPError, e:
        log.error("Recieved error response: %s" % e)
        return ERROR, e.code
    except urllib2.URLError, e:
        log.error("Recieved error response: %s" % e)
        return ERROR, e.reason

    result = simplejson.load(output)
    return OK, result


# TODO: tests
class Client(object):
    """A client used in commands to make requests to the tron.www """

    def __init__(self, options):
        self.options = options

    def status(self):
        return self.request('/status')

    def events(self):
        return self.request('/events')['data']

    def config(self, data=None):
        """This may be a post or a get, depending on data."""
        if data:
            return self.request('/config', dict(config=data))
        return self.request('/config', )['config']

    def index(self):
        content = self.request('/')

        def name_href_dict(source):
            return dict((i['name'], i['href']) for i in source)

        return {
            'jobs':     name_href_dict(content['jobs']),
            'services': name_href_dict(content['services'])
        }

    def get_url_from_identifier(self, iden):
        """Convert a string of the form job_name[.run_number[.action]] to its
        corresponding URL.
        """
        content = self.index()
        obj_name_elements = iden.split('.')
        obj_name = obj_name_elements[0]
        obj_rel_path = "/".join(obj_name_elements[1:])

        def full_url(obj_url):
            return '/'.join((obj_url, obj_rel_path))

        if obj_name in content['jobs']:
            return full_url(content['jobs'][obj_name])
        if obj_name in content['services']:
            return full_url(content['services'][obj_name])

        raise ValueError("Unknown identifier: %s" % iden)

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
        status, content = request(self.options.server, url, data)
        if not status == OK:
            err_msg = "Failed to request %s%s: %s %s" % (
                self.options.server, url, content, data or '')
            raise ValueError(err_msg)
        return content
