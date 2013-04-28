import urllib2

import mock
from testify import TestCase, setup, assert_equal, run, setup_teardown
from testify.assertions import assert_in
from tests.assertions import assert_raises
from tests.testingutils import autospec_method

from tron.commands import client
from tron.commands.client import get_object_type_from_identifier, TronObjectType


def build_file_mock(content):
    return mock.Mock(read=mock.Mock(return_value=content))


class RequestTestCase(TestCase):

    @setup
    def setup_options(self):
        self.url = 'http://localhost:8089/services/'

    @setup_teardown
    def patch_urllib(self):
        patcher = mock.patch('tron.commands.client.urllib2.urlopen',
                             autospec=True)
        with patcher as self.mock_urlopen:
            yield

    def test_build_url_request_no_data(self):
        request = client.build_url_request(self.url, None)
        assert request.has_header('User-agent')
        assert_equal(request.get_method(), 'GET')
        assert_equal(request.get_full_url(), self.url)

    def test_build_url_request_with_data(self):
        data = {'param': 'is_set', 'other': 1}
        request = client.build_url_request(self.url, data)
        assert request.has_header('User-agent')
        assert_equal(request.get_method(), 'POST')
        assert_equal(request.get_full_url(), self.url)
        assert_in('param=is_set', request.get_data())
        assert_in('other=1', request.get_data())

    def test_load_response_content_success(self):
        content = 'not:valid:json'
        http_response = build_file_mock(content)
        response = client.load_response_content(http_response)
        assert_equal(response.error, client.DECODE_ERROR)
        assert_in('No JSON object', response.msg)
        assert_equal(response.content, content)

    def test_request_http_error(self):
        self.mock_urlopen.side_effect = urllib2.HTTPError(
            self.url, 500, 'broke', {}, build_file_mock('oops'))
        response = client.request(self.url)
        expected = client.Response(500, 'broke', 'oops')
        assert_equal(response, expected)

    def test_request_url_error(self):
        self.mock_urlopen.side_effect = urllib2.URLError('broke')
        response = client.request(self.url)
        expected = client.Response(client.URL_ERROR, 'broke', None)
        assert_equal(response, expected)

    def test_request_success(self):
        self.mock_urlopen.return_value = build_file_mock('{"ok": "ok"}')
        response = client.request(self.url)
        expected = client.Response(None, None, {'ok': 'ok'})
        assert_equal(response, expected)


class ClientRequestTestCase(TestCase):

    @setup
    def setup_client(self):
        self.url = 'http://localhost:8089/'
        self.client = client.Client(self.url)

    @setup_teardown
    def patch_request(self):
        with mock.patch('tron.commands.client.request') as self.mock_request:
            yield

    def test_request_error(self):
        exception = assert_raises(client.RequestError,
            self.client.request, '/jobs')
        assert_in(self.url, str(exception))

    def test_request_success(self):
        ok_response = {'ok': 'ok'}
        client.request.return_value = client.Response(None, None, ok_response)
        response = self.client.request('/jobs')
        assert_equal(response, ok_response)


class ClientTestCase(TestCase):

    @setup
    def setup_client(self):
        self.url = 'http://localhost:8089/'
        self.client = client.Client(self.url)
        autospec_method(self.client.request)

    def test_config_post(self):
        name, data, hash = 'name', 'stuff', 'hash'
        self.client.config(name, config_data=data, config_hash=hash)
        expected_data =  {'config': data, 'name': name, 'hash': hash}
        self.client.request.assert_called_with('/api/config', expected_data)

    def test_config_get_default(self):
        self.client.config('config_name')
        self.client.request.assert_called_with('/api/config?name=config_name')

    def test_http_get(self):
        self.client.http_get('/api/jobs', {'include': 1})
        self.client.request.assert_called_with('/api/jobs?include=1')

    def test_action_runs(self):
        self.client.action_runs('/api/jobs/name/0/act', num_lines=40)
        self.client.request.assert_called_with(
            '/api/jobs/name/0/act?include_stdout=1&num_lines=40&include_stderr=1')

    def test_job_runs(self):
        self.client.job_runs('/api/jobs/name/0')
        self.client.request.assert_called_with(
            '/api/jobs/name/0?include_action_runs=1&include_action_graph=0')

    def test_job(self):
        self.client.job('/api/jobs/name', count=20)
        self.client.request.assert_called_with(
            '/api/jobs/name?include_action_runs=0&num_runs=20')

    def test_jobs(self):
        self.client.jobs()
        self.client.request.assert_called_with(
            '/api/jobs?include_job_runs=0&include_action_runs=0')


class GetUrlTestCase(TestCase):

    def test_get_job_url_for_action_run(self):
        url = client.get_job_url('MASTER.name.1.act')
        assert_equal(url, '/api/jobs/MASTER.name/1/act')

    def test_get_job_url_for_job(self):
        url = client.get_job_url('MASTER.name')
        assert_equal(url, '/api/jobs/MASTER.name')

    def test_get_service_url(self):
        url = client.get_service_url('MASTER.name.2')
        assert_equal(url, '/api/services/MASTER.name/2')


class GetContentFromIdentifierTestCase(TestCase):

    @setup
    def setup_client(self):
        self.options = mock.Mock()
        self.index = {
            'namespaces': ['OTHER', 'MASTER'],
            'jobs': {
                'MASTER.namea': '',
                'MASTER.nameb': '',
                'OTHER.nameg':  '',
            },
            'services': ['MASTER.foo']
        }

    def test_get_url_from_identifier_job_no_namespace(self):
        identifier = get_object_type_from_identifier(self.index, 'namea')
        assert_equal(identifier.url, '/api/jobs/MASTER.namea')
        assert_equal(identifier.type, TronObjectType.job)

    def test_get_url_from_identifier_service_no_namespace(self):
        identifier = get_object_type_from_identifier(self.index, 'foo')
        assert_equal(identifier.url, '/api/services/MASTER.foo')
        assert_equal(identifier.type, TronObjectType.service)

    def test_get_url_from_identifier_job(self):
        identifier = get_object_type_from_identifier(self.index, 'MASTER.namea')
        assert_equal(identifier.url, '/api/jobs/MASTER.namea')
        assert_equal(identifier.type, TronObjectType.job)

    def test_get_url_from_identifier_service(self):
        identifier = get_object_type_from_identifier(self.index, 'MASTER.foo')
        assert_equal(identifier.url, '/api/services/MASTER.foo')
        assert_equal(identifier.type, TronObjectType.service)

    def test_get_url_from_identifier_service_instance(self):
        identifier = get_object_type_from_identifier(self.index, 'MASTER.foo.1')
        assert_equal(identifier.url, '/api/services/MASTER.foo/1')
        assert_equal(identifier.type, TronObjectType.service_instance)

    def test_get_url_from_identifier_job_run(self):
        identifier = get_object_type_from_identifier(self.index, 'MASTER.nameb.7')
        assert_equal(identifier.url, '/api/jobs/MASTER.nameb/7')
        assert_equal(identifier.type, TronObjectType.job_run)

    def test_get_url_from_identifier_action_run(self):
        identifier = get_object_type_from_identifier(self.index, 'MASTER.nameb.7.run')
        assert_equal(identifier.url, '/api/jobs/MASTER.nameb/7/run')
        assert_equal(identifier.type, TronObjectType.action_run)

    def test_get_url_from_identifier_job_no_namespace_not_master(self):
        identifier = get_object_type_from_identifier(self.index, 'nameg')
        assert_equal(identifier.url, '/api/jobs/OTHER.nameg')
        assert_equal(identifier.type, TronObjectType.job)

    def test_get_url_from_identifier_no_match(self):
        exc = assert_raises(ValueError,
            get_object_type_from_identifier, self.index, 'MASTER.namec')
        assert_in('namec', str(exc))

if __name__ == "__main__":
    run()
