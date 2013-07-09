from tron.serialize.runstate.tronstore.transport import JSONTransport, cPickleTransport, MsgPackTransport, YamlTransport

transport_class_map = {
    'json': JSONTransport,
    'pickle': cPickleTransport,
    'msgpack': MsgPackTransport,
    'yaml': YamlTransport
}
# a simple max integer to prevent ids from growing indefinitely
MAX_MSG_ID = 2**32 - 1


class StoreRequestFactory(object):
    """A factory to generate requests that need to be converted to serialized
    strings and back. All the factory itself does is keep track of what
    serialization method was set by the configuration, and then constructs
    specific StoreRequest objects using that method.
    """

    def __init__(self, method):
        self.serializer = transport_class_map[method]
        self.id_counter = 1

    def _increment_counter(self):
        """A simple method to make sure that we don't indefinitely increase
        the id assigned to StoreRequests.
        """
        return self.id+1 if not self.id == MAX_MSG_ID else 0

    def build(self, req_type, data_type, data):
        new_request = StoreRequest(self.id_counter, req_type, data, self.serializer)
        self.id_counter = self._increment_counter()
        return new_request

    def rebuild(self, msg):
        return StoreRequest.from_message(self.serializer.deserialize(msg), self.serializer)

    def update_method(self, new_method):
        """Update the method used for message serialization."""
        self.serializer = transport_class_map[new_method]

    def get_method(self):
        return self.serializer


class StoreResponseFactory(object):
    """A factory to generate responses that need to be converted to serialized
    strings and back. The factory itself just keeps track of what serialization
    method was specified by the configuration, and then constructs specific
    StoreResponse objects using that method.
    """

    def __init__(self, method):
        self.serializer = transport_class_map[method]

    def build(self, success, req_id, data):
        new_request = StoreResponse(req_id, success, data, self.serializer)
        return new_request

    def rebuild(self, msg):
        return StoreResponse.from_message(self.serializer.deserialize(msg), self.serializer)

    def update_method(self, new_method):
        """Update the method used for message serialization."""
        self.serializer = transport_class_map[new_method]

    def get_method(self):
        return self.serializer


class StoreRequest(object):
    """An object representing a request to tronstore. The request has four
    essential attributes:
        id - an integer identifier, used for matching requests with responses
        req_type - the request type from msg_enums.py, such as save/restore
        data_type - the type of data the request is for. there are four kinds
            of saved state_data: job, jobrun, service, and meta state_data.
        data - the data required for the request, like a name or state_data
    """

    def __init__(self, req_id, req_type, data_type, data, method):
        self.id         = req_id
        self.req_type   = req_type
        self.data       = data
        self.data_type  = data_type
        self.method     = method
        self.serialized = self.get_serialized()

    @classmethod
    def from_message(cls, msg_data, method):
        req_id, req_type, data_type, data = msg_data
        return cls(req_id, req_type, data_type, data, method)

    def update_method(self, new_method):
        """Update the method used for message serialization."""
        self.method = transport_class_map['new_method']
        self.serialized = self.get_serialized()

    def get_serialized(self):
        return self.method.serialize((self.id, self.req_type, self.data_type, self.data))


class StoreResponse(object):
    """An object representing a response from tronstore. The response has three
    essential attributes:
        id - matches the id of some request so this can be matched with it
        success - shows if the request matching this response was successful
        data - data requested by a request, if any
    """

    def __init__(self, req_id, success, data, method):
        self.id = req_id
        self.success = success
        self.data = data
        self.method = method
        self.serialized = self.get_serialized()

    @classmethod
    def from_message(cls, msg_data, method):
        req_id, success, data = msg_data
        return cls(req_id, success, data, method)

    def update_method(self, new_method):
        """Update the method used for message serialization."""
        self.method = transport_class_map['new_method']
        self.serialized = self.get_serialized()

    def get_serialized(self):
        return self.method.serialize((self.id, self.success, self.data))
