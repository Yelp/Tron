import mock
from testify import TestCase, setup, assert_equal, run
from testify.assertions import assert_in
from tests.assertions import assert_raises
from tron.commands import client
from tron.commands.client import get_object_type_from_identifier, TronObjectType


class ClientTestCase(TestCase):

    @setup
    def setup_client(self):
        self.options = mock.Mock()
        self.client = client.Client(self.options)

    def test_config_post(self):
        self.client.request = mock.create_autospec(self.client.request)
        name, data = 'name', 'stuff'
        self.client.config(name, data=data)
        expected_data =  {'config': data, 'name': name}
        self.client.request.assert_called_with('/config', expected_data)

    def test_config_get_default(self):
        self.client.request = mock.create_autospec(self.client.request)
        self.client.config('config_name')
        self.client.request.assert_called_with('/config?name=config_name')


class GetContentFromIdentifierTestCase(TestCase):

    @setup
    def setup_client(self):
        self.options = mock.Mock()
        self.index = {
            'jobs': {
                'MASTER.namea': '/jobs/MASTER.namea',
                'MASTER.nameb': '/jobs/MASTER.nameb'
            },
            'services': {
                'MASTER.foo': '/services/MASTER.foo'
            }
        }

    def test_get_url_from_identifier_job_no_namespace(self):
        identifier = get_object_type_from_identifier(self.index, 'namea')
        assert_equal(identifier.url, self.index['jobs']['MASTER.namea'] + '/')
        assert_equal(identifier.type, TronObjectType.job)
        assert_equal(identifier.name, 'MASTER.namea')

    def test_get_url_from_identifier_service_no_namespace(self):
        identifier = get_object_type_from_identifier(self.index, 'foo')
        assert_equal(identifier.url, self.index['services']['MASTER.foo'] + '/')
        assert_equal(identifier.type, TronObjectType.service)
        assert_equal(identifier.name, 'MASTER.foo')

    def test_get_url_from_identifier_job(self):
        identifier = get_object_type_from_identifier(self.index, 'MASTER.namea')
        assert_equal(identifier.url, self.index['jobs']['MASTER.namea'] + '/')
        assert_equal(identifier.type, TronObjectType.job)
        assert_equal(identifier.name, 'MASTER.namea')

    def test_get_url_from_identifier_service(self):
        identifier = get_object_type_from_identifier(self.index, 'MASTER.foo')
        assert_equal(identifier.url, self.index['services']['MASTER.foo'] + '/')
        assert_equal(identifier.type, TronObjectType.service)

    def test_get_url_from_identifier_service_instance(self):
        identifier = get_object_type_from_identifier(self.index, 'MASTER.foo.1')
        assert_equal(identifier.url, self.index['services']['MASTER.foo'] + '/1')
        assert_equal(identifier.type, TronObjectType.service_instance)

    def test_get_url_from_identifier_job_run(self):
        identifier = get_object_type_from_identifier(self.index, 'MASTER.nameb.7')
        assert_equal(identifier.url, self.index['jobs']['MASTER.nameb'] + '/7')
        assert_equal(identifier.type, TronObjectType.job_run)

    def test_get_url_from_identifier_action_run(self):
        identifier = get_object_type_from_identifier(self.index, 'MASTER.nameb.7.run')
        assert_equal(identifier.url, self.index['jobs']['MASTER.nameb'] + '/7/run')
        assert_equal(identifier.type, TronObjectType.action_run)

    def test_get_url_from_identifier_no_match(self):
        exc = assert_raises(ValueError,
            get_object_type_from_identifier, self.index, 'MASTER.namec')
        assert_in('namec', str(exc))

if __name__ == "__main__":
    run()
