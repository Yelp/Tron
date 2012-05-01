from collections import namedtuple
from tron.serialize import runstate


MongoStateKey = namedtuple('MongoStateKey', ['collection', 'key'])


class MongoStateStore(object):

    JOB_COLLECTION              = 'job_state_collection'
    SERVICE_COLLECTION          = 'service_state_collection'
    METADATA_COLLECTION         = 'metadata_collection'

    TYPE_TO_COLLECTION_MAP = {
        runstate.JOB_STATE:     JOB_COLLECTION,
        runstate.SERVICE_STATE: SERVICE_COLLECTION,
        runstate.MCP_STATE:     METADATA_COLLECTION
    }

    def __init__(self, db_name, connection_details):
        import pymongo
        global pymongo
        assert pymongo

        self.db_name        = db_name
        connection_params   = self._parse_connection_details(connection_details)
        self._connect(db_name, connection_params)

    def _connect(self, db_name, params):
        """Connect to MongoDB."""
        hostname            = params.get('hostname')
        port                = params.get('port')
        self.connection     = pymongo.Connection(hostname, port)
        self.db             = self.connection[db_name]

    def _parse_connection_details(self, connection_details):
        # TODO:
        return {}

    def build_key(self, type, iden):
        return MongoStateKey(self.TYPE_TO_COLLECTION_MAP[type], iden)

    def save(self, key, state_data):
        collection = self.db[key.collection]

    def restore(self, keys):
        pass

    def cleanup(self):
        self.connection.disconnect()

    def __str__(self):
        return "MongoStateStore(%s)" % self.db_name
