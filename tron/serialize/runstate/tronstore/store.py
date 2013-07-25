import shelve
import urlparse
import os
from contextlib import contextmanager

from tron.serialize.runstate.tronstore.transport import serialize_class_map
from tron.serialize import runstate
from tron.config.config_utils import MAX_IDENTIFIER_LENGTH


class NullStore(object):

    def save(self, key, state_data, data_type):
        return False

    def restore(self, key, data_type):
        return (False, None)

    def cleanup(self):
        pass

    def __repr__(self):
        return "NullStateStore"


class ShelveStore(object):
    """Store state using python's built-in shelve module."""

    def __init__(self, name, connection_details=None, serializer=None):
        self.fname = name
        self.shelve = shelve.open(self.fname)

    def save(self, key, state_data, data_type):
        self.shelve['(%s__%s)' % (data_type, key)] = state_data
        self.shelve.sync()
        return True

    def restore(self, key, data_type):
        value = self.shelve.get('(%s__%s)' % (data_type, key))
        return (True, value) if value else (False, None)

    def cleanup(self):
        self.shelve.close()

    def __repr__(self):
        return "ShelveStateStore('%s')" % self.filename


class SQLStore(object):
    """Store state using SQLAlchemy. Creates tables if needed."""

    def __init__(self, name, connection_details, serializer):
        import sqlalchemy as sql
        global sql
        assert sql

        self.name = name
        self._connection = None
        self.serializer = serializer
        self.engine = sql.create_engine(connection_details)
        self._setup_tables()

    def _setup_tables(self):
        self._metadata = sql.MetaData()
        self.job_state_table = sql.Table('job_state_data', self._metadata,
            sql.Column('key', sql.String(MAX_IDENTIFIER_LENGTH), primary_key=True),
            sql.Column('state_data', sql.Text))
        self.service_table = sql.Table('service_data', self._metadata,
            sql.Column('key', sql.String(MAX_IDENTIFIER_LENGTH), primary_key=True),
            sql.Column('state_data', sql.Text))
        self.job_run_table = sql.Table('job_run_data', self._metadata,
            sql.Column('key', sql.String(MAX_IDENTIFIER_LENGTH), primary_key=True),
            sql.Column('state_data', sql.Text))
        self.metadata_table = sql.Table('metadata_table', self._metadata,
            sql.Column('key', sql.String(MAX_IDENTIFIER_LENGTH), primary_key=True),
            sql.Column('state_data', sql.Text))

        self._metadata.create_all(self.engine)

    @contextmanager
    def connect(self):
        if not self._connection or self._connection.closed:
            self._connection = self.engine.connect()
        yield self._connection

    def _get_table(self, data_type):
        if data_type == runstate.JOB_STATE:
            return self.job_state_table
        elif data_type == runstate.JOB_RUN_STATE:
            return self.job_run_table
        elif data_type == runstate.SERVICE_STATE:
            return self.service_table
        elif data_type == runstate.MCP_STATE:
            return self.metadata_table
        else:
            return None

    def save(self, key, state_data, data_type):
        with self.connect() as conn:
            table = self._get_table(data_type)
            if table is None:
                return False
            state_data = self.serializer.serialize(state_data)
            update_result = conn.execute(
                table.update()
                .where(table.c.key == key)
                .values(state_data=state_data))
            if not update_result.rowcount:
                conn.execute(
                    table.insert()
                    .values(key=key, state_data=state_data))
            return True

    def restore(self, key, data_type):
        with self.connect() as conn:
            table = self._get_table(data_type)
            if table is None:
                return (False, None)
            result = conn.execute(sql.sql.select(
                [table.c.state_data],
                table.c.key == key)
            ).fetchone()
            return (True, self.serializer.deserialize(result[0])) if result else (False, None)

    def cleanup(self):
        if self._connection:
            self._connection.close()

    def __repr__(self):
        return "SQLStore(%s)" % self.name


class MongoStore(object):
    """Store state using mongoDB."""

    JOB_COLLECTION              = 'job_state_collection'
    JOB_RUN_COLLECTION          = 'job_run_state_collection'
    SERVICE_COLLECTION          = 'service_state_collection'
    METADATA_COLLECTION         = 'metadata_collection'

    TYPE_TO_COLLECTION_MAP = {
        runstate.JOB_STATE:     JOB_COLLECTION,
        runstate.JOB_RUN_STATE: JOB_RUN_COLLECTION,
        runstate.SERVICE_STATE: SERVICE_COLLECTION,
        runstate.MCP_STATE:     METADATA_COLLECTION
    }

    def __init__(self, name, connection_details, serializer=None):
        import pymongo
        global pymongo
        assert pymongo

        self.db_name      = name
        connection_params = self._parse_connection_details(connection_details)
        self._connect(connection_params)

    def _connect(self, params):
        hostname        = params.get('hostname')
        port            = int(params.get('port'))
        username        = params.get('username')
        password        = params.get('password')
        self.connection = pymongo.Connection(hostname, port)
        self.db         = self.connection[self.db_name]
        if username and password:
            self.db.authenticate(username, password)

    def _parse_connection_details(self, connection_details):
        return dict(urlparse.parse_qsl(connection_details)) if connection_details else {}

    def save(self, key, state_data, data_type):
        collection = self.db[self.TYPE_TO_COLLECTION_MAP[data_type]]
        state_data['_id'] = key
        collection.save(state_data)
        return True

    def restore(self, key, data_type):
        value = self.db[self.TYPE_TO_COLLECTION_MAP[data_type]].find_one(key)
        return (True, value) if value else (False, None)

    def cleanup(self):
        self.connection.disconnect()

    def __repr__(self):
        return "MongoStore(%s)" % self.db_name


class YamlStore(object):
    # TODO: Deprecate this, it's bad
    """Store state in a local YAML file.

    WARNING: Using this is NOT recommended, even moreso than the previous
    version of this (yamlstore.py), since key/value pairs are now saved
    INDIVIDUALLY rather than in batches, meaning saves are SLOOOOOOOW.

    Seriously, you probably shouldn't use this unless you're doing something
    really trivial and/or want a readable Yaml file.
    """

    TYPE_MAPPING = {
        runstate.JOB_STATE:     'jobs',
        runstate.JOB_RUN_STATE: 'job_runs',
        runstate.SERVICE_STATE: 'services',
        runstate.MCP_STATE:     runstate.MCP_STATE
    }

    def __init__(self, filename, connection_details=None, serializer=None):
        import yaml
        global yaml
        assert yaml

        self.filename = filename
        if not os.path.exists(self.filename):
            self.buffer = {}
        else:
            with open(self.filename, 'r') as fh:
                self.buffer = yaml.load(fh)

    def save(self, key, state_data, data_type):
        self.buffer.setdefault(self.TYPE_MAPPING[data_type], {})[key] = state_data
        self._write_buffer()
        return True

    def _write_buffer(self):
        with open(self.filename, 'w') as fh:
            yaml.dump(self.buffer, fh)

    def restore(self, key, data_type):
        value = self.buffer.get(self.TYPE_MAPPING[data_type], {}).get(key)
        return (True, value) if value else (False, None)

    def cleanup(self):
        pass

    def __repr__(self):
        return "YamlStore('%s')" % self.filename


store_class_map = {
    "sql": SQLStore,
    "shelve": ShelveStore,
    "mongo": MongoStore,
    "yaml": YamlStore
}


def build_store(name, store_type, connection_details, db_store_method):
    serial_class = serialize_class_map[db_store_method] if db_store_method != "None" else None
    return store_class_map[store_type](name, connection_details, serial_class)
