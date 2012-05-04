from testify import TestCase, setup, assert_equal, run
from testify.assertions import assert_in
from tests.assertions import assert_raises
from tests.testingutils import Turtle
from tron.commands import client


class ClientGetUrlFromIdentifierTestCase(TestCase):

    @setup
    def setup_client(self):
        self.options = Turtle()
        self.client = client.Client(self.options)
        self.index_dict = {
            'jobs': {
                'namea': '/jobs/namea',
                'nameb': '/jobs/nameb'
            },
            'services': {
                'foo': '/services/foo'
            }
        }
        self.client.index = lambda: self.index_dict

    def test_get_url_from_identifier_job(self):
        url = self.client.get_url_from_identifier('namea')
        assert_equal(url, self.index_dict['jobs']['namea'] + '/')

    def test_get_url_from_identifier_service(self):
        url = self.client.get_url_from_identifier('foo')
        assert_equal(url, self.index_dict['services']['foo'] + '/')

    def test_get_url_from_identifier_job_run(self):
        url = self.client.get_url_from_identifier('nameb.7')
        assert_equal(url, self.index_dict['jobs']['nameb'] + '/7')

    def test_get_url_from_identifier_action_run(self):
        url = self.client.get_url_from_identifier('nameb.7.run')
        assert_equal(url, self.index_dict['jobs']['nameb'] + '/7/run')

    def test_get_url_from_identifier_no_match(self):
        exc = assert_raises(ValueError,
                self.client.get_url_from_identifier, 'namec')
        assert_in('namec', str(exc))


if __name__ == "__main__":
    run()