import itertools
import operator
import logging

from twisted.internet import reactor
from tron.serialize.runstate.tronstore.process import StoreProcessProtocol
from tron.serialize.runstate.tronstore.messages import StoreRequestFactory, StoreResponseFactory
from tron.serialize.runstate.tronstore import msg_enums

log = logging.getLogger(__name__)


class ParallelKey(object):
    __slots__ = ['type', 'iden']

    def __init__(self, type, iden):
        self.type               = type
        self.iden               = iden

    @property
    def key(self):
        return str(self.iden)

    def __str__(self):
        return "%s %s" % (self.type, self.iden)

    def __eq__(self, other):
        return self.type == other.type and self.iden == other.iden

    def __hash__(self):
        return hash(self.key)

class ParallelStore(object):
    """Persist state using a paralleled storing mechanism, tronstore. This uses
    the Twisted library to run the tronstore executable in a separate
    process, and handles all communication between trond and tronstore.

    This class handles construction of all messages that need to be sent
    to tronstore based on requests given by the MCP."""

    def __init__(self, config):
        self.config = config
        self.request_factory = StoreRequestFactory(config.transport_method)
        self.response_factory = StoreResponseFactory(config.transport_method)
        self.process = StoreProcessProtocol(self.response_factory)
        self.start_process()

    def start_process(self):
        """Use twisted to spawn the tronstore process.

        The command line arguments given to spawnProcess are in a
        HARDCODED ORDER that MUST match the order that tronstore parses them.
        """
        reactor.spawnProcess(self.process, "serialize/runstate/tronstore/tronstore",
            ["tronstore",
            self.config.name,
            self.config.transport_method,
            self.config.store_type,
            self.config.connection_details,
            self.config.db_store_method]
        )
        reactor.run()

    def build_key(self, type, iden):
        return ParallelKey(type, iden)

    def save(self, key_value_pairs):
        for key, state_data in key_value_pairs:
            request = self.request_factory.build(msg_enums.REQUEST_SAVE, key.type, (key.key, state_data))
            self.process.send_request(request)

    def restore_single(self, key):
        request = self.request_factory.build(msg_enums.REQUEST_RESTORE, key.type, key.key)
        response = self.process.send_request_get_response(request)
        return response.data if response.successful else None

    def restore(self, keys):
        items = itertools.izip(keys, (self.restore_single(key) for key in keys))
        return dict(itertools.ifilter(operator.itemgetter(1), items))

    def cleanup(self):
        self.process.shutdown()
    shutdown = cleanup

    # This method may not be needed. From looking at the StateChangeWatcher
    # implementation, it looks like it makes a completely new instance of a
    # PersistentStateManager whenever the config is updated, which removes
    # the need for changing config related things here (since a new instance
    # of this class will be created anyway).
    def load_config(self, new_config):
        """Reconfigure the storing mechanism to use a new configuration
        by shutting down and restarting tronstore."""
        self.config = new_config
        self.request_factory.update_method(new_config.transport_method)
        self.process.shutdown()
        self.response_factory.update_method(new_config.transport_method)
        self.process = StoreProcessProtocol(self.response_factory)
        self.start_process()

    def __repr__(self):
        return "ParallelStore"
