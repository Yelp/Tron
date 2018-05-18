from __future__ import absolute_import
from __future__ import unicode_literals

import operator
from collections import namedtuple
from contextlib import contextmanager

from six.moves import filter

from tron import yaml
from tron.config.config_utils import MAX_IDENTIFIER_LENGTH
from tron.serialize import runstate
sqlalchemy = None  # pyflakes

SQLStateKey = namedtuple('SQLStateKey', ['table', 'id'])


class SQLAlchemyStateStore(object):
    def __init__(self, name, connection_details):
        import sqlalchemy
        global sqlalchemy
        assert sqlalchemy  # pyflakes

        self.name = name
        self._connection = None
        self.encoder = yaml.dump
        self.decoder = yaml.load
        self._create_engine(connection_details)
        self._build_tables()
        self.create_tables()

    def _create_engine(self, connection_details):
        """Connect to the configured database."""
        self.engine = sqlalchemy.create_engine(connection_details)

    def _build_tables(self):
        """Build table objects."""
        from sqlalchemy import Table, Column, String, Text
        self._metadata = sqlalchemy.MetaData()
        self.job_table = Table(
            'job_state_data',
            self._metadata,
            Column(
                'id',
                String(MAX_IDENTIFIER_LENGTH, ),
                primary_key=True,
            ),
            Column('state_data', Text),
        )

        self.metadata_table = Table(
            'metadata_table',
            self._metadata,
            Column(
                'id',
                String(MAX_IDENTIFIER_LENGTH, ),
                primary_key=True,
            ),
            Column('state_data', Text),
        )

    def create_tables(self):
        """Execute the create table statements."""
        self._metadata.create_all(self.engine)

    @contextmanager
    def connect(self):
        """Yield a connection."""
        # TODO: handle 'mysql has gone away' and similar exceptions
        if not self._connection or self._connection.closed:
            self._connection = self.engine.connect()
        yield self._connection

    def build_key(self, type, iden):
        table = None
        if type == runstate.JOB_STATE:
            table = self.job_table
        if type == runstate.MCP_STATE:
            table = self.metadata_table
        return SQLStateKey(table, iden)

    def save(self, key_value_pairs):
        with self.connect() as conn:
            for key, state_data in key_value_pairs:
                state_data = self.encoder(state_data)

                # The first state update requires an insert
                if not self._update(conn, key, state_data):
                    self._insert(conn, key, state_data)

    def _update(self, conn, key, data):
        """Attempt to update the state_data."""
        update = key.table.update()
        where = key.table.c.id == key.id
        results = conn.execute(update.where(where).values(state_data=data))
        return results.rowcount

    def _insert(self, conn, key, state_data):
        """Attempt to insert the state_data."""
        insert = key.table.insert()
        conn.execute(insert.values(id=key.id, state_data=state_data))

    def restore(self, keys):
        with self.connect() as conn:
            items = [(key, self._select(conn, key)) for key in keys]
            return dict(filter(operator.itemgetter(1), items))

    def _select(self, conn, key):
        cols = [key.table.c.state_data]
        select = sqlalchemy.sql.select(cols, key.table.c.id == key.id)
        result = conn.execute(select).fetchone()
        return self.decoder(result[0]) if result else None

    def cleanup(self):
        if self._connection:
            self._connection.close()

    def __str__(self):
        return "SQLAlchemyStateStore(%s)" % self.name
