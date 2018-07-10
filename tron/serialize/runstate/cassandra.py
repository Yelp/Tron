import logging
import pickle

import cassandra

log = logging.getLogger(__name__)


class CassandraStore:
    """Persist state using `shelve`."""

    def __init__(self, keyspace, hosts):
        self.cluster = cassandra.Cluster(hosts)
        self.session = self.cluster.connect(keyspace)

    def build_key(self, type, iden):
        return f"{type}__{iden}"

    def save(self, key_value_pairs):
        prepared = self.session.prepare("UPDATE blobs (key, value) VALUES (?, ?)")
        batch = cassandra.query.BatchStatement(
            consistency_level=cassandra.ConsistencyLevel.QUORUM
        )
        for key, value in key_value_pairs:
            batch.add(prepared, (key, pickle.dumps(value, protocol=4)))
        self.session.execute(batch)

    def restore(self, keys):
        rows = self.session.execute(
            "SELECT key, value FROM blobs WHERE key IN (?)", [keys]
        )
        return {key: pickle.loads(value, protocol=4) for key, value in rows}

    def cleanup(self):
        pass
