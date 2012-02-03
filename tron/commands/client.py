"""
A command line http client used by tronview.
"""
from tron import cmd


class Client(object):
    """A client used in commands to make requests to the tron.www """
    
    def __init__(self, options):
        self.options = options

    def status(self):
        return self.request('/status')

    def events(self):
        return self.request('/events')['data']

    def index(self):
        return self.request('/')

    def services(self):
        return self.index().get('services')

    def service(self, service_id):
        service_url = "/services/%s" % service_id
        return self.request(service_url)

    def service_events(self, service_id):
        service_url = "/services/%s/_events" % service_id
        return self.request(service_url)['data']

    def jobs(self):
        return self.request('/jobs').get('jobs')

    def job(self, job_id):
        return self.request('/jobs/%s' % job_id)

    def job_events(self, job_id):
        return self.request('/jobs/%s/_events' % job_id)['data']

    def actions(self, action_id):
        action_id = action_id.replace('.', '/')
        return self.request('/jobs/%s' % action_id)

    def action(self, action_id):
        url = "/jobs/%s?num_lines=%s" % (action_id.replace('.', '/'),
            self.options.num_displays)
        return self.request(url)

    def action_events(self, action_id):
        action_id = action_id.replace('.', '/')
        return self.request('/jobs/%s/_events' % action_id)['data']

    def request(self, url):
        status, content = cmd.request(self.options.server, url)
        assert status == cmd.OK, "Failed to retrieve %s%s: %s" % (
            self.options.server, url, content)
        return content

