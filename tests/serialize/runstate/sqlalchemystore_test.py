from testify import TestCase, run, setup, suite, assert_equal, teardown
from tests.assertions import assert_length
from tron.serialize import runstate
sqlalchemystore = None # pyflakes

class SQLAlchmeyStateStoreTestCase(TestCase):

    @setup
    def setup_store(self):
        from tron.serialize.runstate import sqlalchemystore
        global sqlalchemystore
        assert sqlalchemystore # pyflakes
        details = 'sqlite:///:memory:'
        self.store = sqlalchemystore.SQLAlchemyStateStore('name', details)
        self.store.create_tables()

    @teardown
    def teardown_store(self):
        self.store.cleanup()

    @suite('sqlalchemy')
    def test_create_engine(self):
        assert_equal(self.store.engine.url.database, ':memory:')

    @suite('sqlalchemy')
    def test_create_tables(self):
        assert self.store.job_table.name
        assert self.store.service_table.name
        assert self.store.metadata_table.name

    @suite('sqlalchemy')
    def test_build_key(self):
        key = self.store.build_key(runstate.SERVICE_STATE, 'blah')
        assert_equal(key.table, self.store.service_table)
        assert_equal(key.id, 'blah')

    @suite('sqlalchemy')
    def test_save(self):
        key = sqlalchemystore.SQLStateKey(self.store.job_table, 'stars')
        doc = {'docs': 'blocks'}
        items = [(key, doc)]
        self.store.save(items)

        rows = self.store.engine.execute(self.store.job_table.select())
        assert_equal(rows.fetchone(), ('stars', "{docs: blocks}\n"))

    @suite('sqlalchemy')
    def test_restore_missing(self):
        key = sqlalchemystore.SQLStateKey(self.store.job_table, 'stars')
        docs = self.store.restore([key])
        assert_equal(docs, {})

    @suite('sqlalchemy')
    def test_restore_many(self):
        keys = [
            sqlalchemystore.SQLStateKey(self.store.job_table, 'stars'),
            sqlalchemystore.SQLStateKey(self.store.service_table, 'foo')
        ]
        items = [
            {'docs': 'builder', 'a': 'b'},
            {'docks': 'helper', 'c': 'd'}
        ]
        self.store.save(zip(keys, items))

        docs = self.store.restore(keys)
        assert_equal(docs[keys[0]], items[0])
        assert_equal(docs[keys[1]], items[1])

    @suite('sqlalchemy')
    def test_restore_partial(self):
        keys = [
            sqlalchemystore.SQLStateKey(self.store.job_table, 'stars'),
            sqlalchemystore.SQLStateKey(self.store.service_table, 'foo')
        ]
        item = {'docs': 'builder', 'a': 'b'}
        self.store.save([(keys[0], item)])

        docs = self.store.restore(keys)
        assert_length(docs, 1)
        assert_equal(docs[keys[0]], item)


if __name__ == "__main__":
    run()