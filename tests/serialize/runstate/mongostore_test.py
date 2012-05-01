from testify import TestCase, run, setup, suite, assert_equal, teardown
from tron.serialize import runstate
mongostore = None # pyflakes

class MongoStateStoreTestCase(TestCase):

    @setup
    def setup_store(self):
        # Defer import
        from tron.serialize.runstate import mongostore
        global mongostore
        self.db_name = 'test_base'
        self.store = mongostore.MongoStateStore(self.db_name, None)

    @teardown
    def teardown_store(self):
        # Clear out records
        self.store.connection.drop_database(self.db_name)
        self.store.cleanup()

    def _create_doc(self, key, doc):
        import pymongo
        db = pymongo.Connection()[self.db_name]
        doc['_id'] = key.key
        db[key.collection].save(doc)
        db.connection.disconnect()

    @suite('mongodb')
    def test__init__(self):
        assert_equal(self.store.db_name, self.db_name)

    @suite('mongodb')
    def test_connect(self):
        assert self.store.connection
        assert_equal(self.store.connection.host, 'localhost')
        assert_equal(self.store.db.name, self.db_name)

    @suite('mongodb')
    def test_parse_connection_details(self):
        details = "hostname=mongoserver&port=55555"
        params = self.store._parse_connection_details(details)
        assert_equal(params, {'hostname': 'mongoserver', 'port': '55555'})

    @suite('mongodb')
    def test_parse_connection_details_with_user_creds(self):
        details = "hostname=mongoserver&port=55555&username=ted&password=sam"
        params = self.store._parse_connection_details(details)
        expected = {
            'hostname': 'mongoserver',
            'port':     '55555',
            'username': 'ted',
            'password': 'sam'}
        assert_equal(params, expected)

    @suite('mongodb')
    def test_parse_connection_details_none(self):
        params = self.store._parse_connection_details(None)
        assert_equal(params, {})

    @suite('mongodb')
    def test_parse_connection_details_empty(self):
        params = self.store._parse_connection_details("")
        assert_equal(params, {})

    @suite('mongodb')
    def test_build_key(self):
        key = self.store.build_key(runstate.JOB_STATE, 'stars')
        assert_equal(key.collection, self.store.JOB_COLLECTION)
        assert_equal(key.key, 'stars')

    @suite('mongodb')
    def test_save(self):
        import pymongo
        doc0, doc1 = {'a':"Hey there"}, {'a': "Howsit"}
        key_value_pairs = [
            (mongostore.MongoStateKey(self.store.JOB_COLLECTION, "1"), doc0),
            (mongostore.MongoStateKey(self.store.SERVICE_COLLECTION, "2"), doc1)
        ]
        self.store.save(key_value_pairs)
        self.store.cleanup()

        db = pymongo.Connection()[self.db_name]
        assert_equal(db[self.store.JOB_COLLECTION].find()[0], doc0)
        assert_equal(db[self.store.SERVICE_COLLECTION].find()[0], doc1)

    @suite('mongodb')
    def test_restore(self):
        keys = [
            mongostore.MongoStateKey(runstate.JOB_STATE, "1"),
            mongostore.MongoStateKey(runstate.SERVICE_STATE, "2")
        ]
        docs = [
            {'ahh': 'first doc'},
            {'bzz': 'second doc'}
        ]
        for i in xrange(2):
            self._create_doc(keys[i], docs[i])
        restored_data = self.store.restore(keys)
        assert_equal(restored_data[keys[0]], docs[0])
        assert_equal(restored_data[keys[1]], docs[1])

    @suite('mongodb')
    def test_restore_not_found(self):
        keys = [mongostore.MongoStateKey(runstate.JOB_STATE, "1")]
        restored_data = self.store.restore(keys)
        assert_equal(restored_data, {})

    @suite('mongodb')
    def test_restore_partial(self):
        keys = [
            mongostore.MongoStateKey(runstate.JOB_STATE, "1"),
            mongostore.MongoStateKey(runstate.SERVICE_STATE, "2")
        ]
        docs = [{'ahh': 'first doc'}]
        self._create_doc(keys[0], docs[0])
        restored_data = self.store.restore(keys)
        assert_equal(restored_data[keys[0]], docs[0])

    @suite('mongodb')
    def test_cleanup(self):
        self.store.cleanup()
        assert not self.store.connection.host
        assert not self.store.connection.port


if __name__ == "__main__":
    run()