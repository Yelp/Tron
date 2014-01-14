import itertools
import operator
import logging
import os

from tron.serialize.runstate.tronstore.process import StoreProcessProtocol
from tron.serialize.runstate.tronstore.messages import StoreRequestFactory
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
    """Persist state using a parallel storing mechanism, tronstore. This uses
    the python mulitprocessing module to run the tronstore executable in
    another process, and handles all communication between trond and tronstore.

    This class handles construction of all messages that need to be sent
    to tronstore based on whatever method was called."""

    def __init__(self):
        self.request_factory = StoreRequestFactory()
        self.process = StoreProcessProtocol()

    def build_key(self, type, iden):
        return ParallelKey(type, iden)

    def save(self, key_value_pairs):
        for key, state_data in key_value_pairs:
            request = self.request_factory.build(msg_enums.REQUEST_SAVE, key.type, (key.key, state_data))
            self.process.send_request(request)

    def restore_single(self, key):
        request = self.request_factory.build(msg_enums.REQUEST_RESTORE, key.type, key.key)
        response = self.process.send_request_get_response(request)
        return response.data if response.success else None

    def restore(self, keys):
        items = itertools.izip(keys, (self.restore_single(key) for key in keys))
        return dict(itertools.ifilter(operator.itemgetter(1), items))

    def cleanup(self):
        shutdown_req = self.request_factory.build(msg_enums.REQUEST_SHUTDOWN, '', '')
        self.process.send_request_shutdown(shutdown_req)
    shutdown = cleanup

    def load_config(self, new_config):
        """Reconfigure the storing mechanism to use a new configuration
        by shutting down and restarting tronstore. THIS MUST BE CALLED
        AT LEAST ONCE, as tronstore is started with a null configuration
        whenever a ParallelStore object is created."""
        log.info("Loading new state persistence configuration into Tronstore...")
        config_req = self.request_factory.build(msg_enums.REQUEST_CONFIG, '', new_config)
        response = self.process.send_request_get_response(config_req)
        if response.success:
            log.info('Successfully loaded new configuration into Tronstore.')
            self.process.update_config(new_config)
            return True
        else:
            log.warn("Failed to load new configuration into Tronstore.")
            return False

    def __repr__(self):
        store = self.process.config.store_type if self.process.config else None
        return "ParallelStore(%s)" % store
