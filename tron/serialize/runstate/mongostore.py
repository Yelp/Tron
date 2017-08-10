"""
 State storage using mongoDB.
 Tested with pymongo 2.2
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import itertools
import operator
import urlparse
from collections import namedtuple

from tron.serialize import runstate
pymongo = None  # pyflakes


MongoStateKey = namedtuple('MongoStateKey', ['collection', 'key'])


class MongoStateStore(object):

    JOB_COLLECTION = 'job_state_collection'
    SERVICE_COLLECTION = 'service_state_collection'
    METADATA_COLLECTION = 'metadata_collection'

    TYPE_TO_COLLECTION_MAP = {
        runstate.JOB_STATE:     JOB_COLLECTION,
        runstate.SERVICE_STATE: SERVICE_COLLECTION,
        runstate.MCP_STATE:     METADATA_COLLECTION,
    }

    def __init__(self, db_name, connection_details):
        import pymongo
        global pymongo
        assert pymongo

        self.db_name = db_name
        connection_params = self._parse_connection_details(connection_details)
        self._connect(db_name, connection_params)

    def _connect(self, db_name, params):
        """Connect to MongoDB."""
        hostname = params.get('hostname')
        port = params.get('port')
        username = params.get('username')
        password = params.get('password')
        self.connection = pymongo.Connection(hostname, port)
        self.db = self.connection[db_name]
        if username and password:
            self.db.authenticate(username, password)

    def _parse_connection_details(self, connection_details):
        if not connection_details:
            return {}
        return dict(urlparse.parse_qsl(connection_details))

    def build_key(self, type, iden):
        return MongoStateKey(self.TYPE_TO_COLLECTION_MAP[type], iden)

    def save(self, key_value_pairs):
        for key, state_data in key_value_pairs:
            state_data['_id'] = key.key
            collection = self.db[key.collection]
            collection.save(state_data)

    def restore(self, keys):
        items = [
            (key, self.db[key.collection].find_one(key.key)) for key in keys
        ]
        return dict(itertools.ifilter(operator.itemgetter(1), items))

    def cleanup(self):
        self.connection.disconnect()

    def __str__(self):
        return "MongoStateStore(%s)" % self.db_name
