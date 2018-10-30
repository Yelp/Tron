from __future__ import absolute_import
from __future__ import unicode_literals

import mock
from six.moves.urllib.error import HTTPError
from six.moves.urllib.error import URLError

from testifycompat import assert_equal
from testifycompat import assert_in
from testifycompat import run
from testifycompat import setup
from testifycompat import setup_teardown
from testifycompat import TestCase
from tests.assertions import assert_raises
from tests.testingutils import autospec_method
from tron.commands import client
from tron.commands.client import get_object_type_from_identifier
from tron.commands.client import Response
from tron.commands.client import TronObjectType


def build_file_mock(content):
    return mock.Mock(
        read=mock.Mock(return_value=content),
        headers=mock.Mock(get_content_charset=mock.Mock(return_value='utf-8')),
    )


class TestRequest(TestCase):
    @setup
    def setup_options(self):
        self.url = 'http://localhost:8089/jobs/'

    @setup_teardown
    def patch_urllib(self):
        patcher = mock.patch(
            'tron.commands.client.urllib.request.urlopen',
            autospec=True,
        )
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
        assert_in('param=is_set', request.data.decode())
        assert_in('other=1', request.data.decode())

    @mock.patch('tron.commands.client.log', autospec=True)
    def test_load_response_content_success(self, _):
        content = b'not:valid:json'
        http_response = build_file_mock(content)
        response = client.load_response_content(http_response)
        assert_equal(response.error, client.DECODE_ERROR)
        assert_equal(response.content, content.decode('utf-8'))

    @mock.patch('tron.commands.client.log', autospec=True)
    def test_request_http_error(self, _):
        self.mock_urlopen.side_effect = HTTPError(
            self.url,
            500,
            'broke',
            mock.Mock(get_content_charset=mock.Mock(return_value='utf-8'), ),
            build_file_mock(b'oops'),
        )
        response = client.request(self.url)
        expected = client.Response(500, 'broke', 'oops')
        assert_equal(response, expected)

    @mock.patch('tron.commands.client.log', autospec=True)
    def test_request_url_error(self, _):
        self.mock_urlopen.side_effect = URLError('broke')
        response = client.request(self.url)
        expected = client.Response(client.URL_ERROR, 'broke', None)
        assert_equal(response, expected)

    def test_request_success(self):
        self.mock_urlopen.return_value = build_file_mock(b'{"ok": "ok"}')
        response = client.request(self.url)
        expected = client.Response(None, None, {'ok': 'ok'})
        assert_equal(response, expected)


class TestClientRequest(TestCase):
    @setup
    def setup_client(self):
        self.url = 'http://localhost:8089/'
        self.client = client.Client(self.url)

    @setup_teardown
    def patch_request(self):
        with mock.patch(
            'tron.commands.client.request', autospec=True
        ) as self.mock_request:
            yield

    def test_request_error(self):
        error_response = Response(
            error='404', msg='Not Found', content='big kahuna error'
        )
        client.request = mock.Mock(return_value=error_response)
        exception = assert_raises(
            client.RequestError,
            self.client.request,
            '/jobs',
        )

        assert str(exception) == error_response.content

    def test_request_success(self):
        ok_response = {'ok': 'ok'}
        client.request.return_value = client.Response(None, None, ok_response)
        response = self.client.request('/jobs')
        assert_equal(response, ok_response)


class TestClient(TestCase):
    @setup
    def setup_client(self):
        self.url = 'http://localhost:8089/'
        self.client = client.Client(self.url)
        autospec_method(self.client.request)

    def test_config_post(self):
        name, data, hash = 'name', 'stuff', 'hash'
        self.client.config(name, config_data=data, config_hash=hash)
        expected_data = {
            'config': data,
            'name': name,
            'hash': hash,
            'check': 0,
        }
        self.client.request.assert_called_with('/api/config', expected_data)

    def test_config_get_default(self):
        self.client.config('config_name')
        self.client.request.assert_called_with(
            '/api/config?name=config_name',
        )

    def test_http_get(self):
        self.client.http_get('/api/jobs', {'include': 1})
        self.client.request.assert_called_with('/api/jobs?include=1')

    def test_action_runs(self):
        self.client.action_runs('/api/jobs/name/0/act', num_lines=40)
        self.client.request.assert_called_with(
            '/api/jobs/name/0/act?include_stderr=1&include_stdout=1&num_lines=40',
        )

    def test_job_runs(self):
        self.client.job_runs('/api/jobs/name/0')
        self.client.request.assert_called_with(
            '/api/jobs/name/0?include_action_graph=0&include_action_runs=1',
        )

    def test_job(self):
        self.client.job('/api/jobs/name', count=20)
        self.client.request.assert_called_with(
            '/api/jobs/name?include_action_runs=0&num_runs=20',
        )

    def test_jobs(self):
        self.client.jobs()
        self.client.request.assert_called_with(
            '/api/jobs?include_action_graph=1&include_action_runs=0&include_job_runs=0&include_node_pool=1',
        )


class TestGetUrl(TestCase):
    def test_get_job_url_for_action_run(self):
        url = client.get_job_url('MASTER.name.1.act')
        assert_equal(url, '/api/jobs/MASTER.name/1/act')

    def test_get_job_url_for_job(self):
        url = client.get_job_url('MASTER.name')
        assert_equal(url, '/api/jobs/MASTER.name')


class TestGetContentFromIdentifier(TestCase):
    @setup
    def setup_client(self):
        self.options = mock.Mock()
        self.index = {
            'namespaces': ['OTHER', 'MASTER'],
            'jobs': {
                'MASTER.namea': '',
                'MASTER.nameb': '',
                'OTHER.nameg': '',
            },
        }

    def test_get_url_from_identifier_job_no_namespace(self):
        identifier = get_object_type_from_identifier(self.index, 'namea')
        assert_equal(identifier.url, '/api/jobs/MASTER.namea')
        assert_equal(identifier.type, TronObjectType.job)

    def test_get_url_from_identifier_job(self):
        identifier = get_object_type_from_identifier(
            self.index,
            'MASTER.namea',
        )
        assert_equal(identifier.url, '/api/jobs/MASTER.namea')
        assert_equal(identifier.type, TronObjectType.job)

    def test_get_url_from_identifier_job_run(self):
        identifier = get_object_type_from_identifier(
            self.index,
            'MASTER.nameb.7',
        )
        assert_equal(identifier.url, '/api/jobs/MASTER.nameb/7')
        assert_equal(identifier.type, TronObjectType.job_run)

    def test_get_url_from_identifier_action_run(self):
        identifier = get_object_type_from_identifier(
            self.index,
            'MASTER.nameb.7.run',
        )
        assert_equal(identifier.url, '/api/jobs/MASTER.nameb/7/run')
        assert_equal(identifier.type, TronObjectType.action_run)

    def test_get_url_from_identifier_job_no_namespace_not_master(self):
        identifier = get_object_type_from_identifier(self.index, 'nameg')
        assert_equal(identifier.url, '/api/jobs/OTHER.nameg')
        assert_equal(identifier.type, TronObjectType.job)

    def test_get_url_from_identifier_no_match(self):
        exc = assert_raises(
            ValueError,
            get_object_type_from_identifier,
            self.index,
            'MASTER.namec',
        )
        assert_in('namec', str(exc))


if __name__ == "__main__":
    run()
