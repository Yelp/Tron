from collections import namedtuple
from contextlib import contextmanager
import itertools
import operator

import yaml
sqlalchemy = None # pyflakes

from tron.serialize import runstate


SQLStateKey = namedtuple('SQLStateKey', ['table', 'id'])


class SQLAlchemyStateStore(object):

    def __init__(self, name, connection_details):
        import sqlalchemy
        global sqlalchemy
        assert sqlalchemy # pyflakes

        self.name               = name
        self._connection        = None
        self.encoder            = yaml.dump
        self.decoder            = yaml.load
        self._create_engine(connection_details)
        self._create_tables()

    def _create_engine(self, connection_details):
        """Connect to the configured database."""
        self.engine = sqlalchemy.create_engine(connection_details)

    def _create_tables(self):
        """Create table objects."""
        from sqlalchemy import Table, Column, String, Text
        self._metadata = sqlalchemy.MetaData()
        self.job_table = Table('job_state_data', self._metadata,
            Column('id', String, primary_key=True),
            Column('state_data', Text)
        )

        self.service_table = Table('service_state_data', self._metadata,
            Column('id', String, primary_key=True),
            Column('state_data', Text)
        )

        self.metadata_table = Table('metadata_table', self._metadata,
            Column('id', String, primary_key=True),
            Column('state_data', Text)
        )

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
        if type == runstate.SERVICE_STATE:
            table = self.service_table
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
        update  = key.table.update()
        where   = key.table.c.id==key.id
        results = conn.execute(update.where(where).values(state_data=data))
        return results.rowcount

    def _insert(self, conn, key, state_data):
        """Attempt to insert the state_data."""
        insert = key.table.insert()
        conn.execute(insert.values(id=key.id, state_data=state_data))

    def restore(self, keys):
        with self.connect() as conn:
            items = [(key, self._select(conn, key)) for key in keys]
            return dict(itertools.ifilter(operator.itemgetter(1), items))

    def _select(self, conn, key):
        select = key.table.select(key.table.c.state_data)
        result = conn.execute(select.where(key.table.c.id==key.id)).fetchone()
        return self.decoder(result) if result else None

    def cleanup(self):
        if self._connection:
            self._connection.close()

    def create_tables(self):
        """Create the database tables."""
        self._metadata.create_all(self.engine)

    def __str__(self):
        return "SQLAlchemyStateStore(%s)" % self.name
