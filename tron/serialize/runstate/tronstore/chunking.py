CHUNK_SIGNING_STR = '\xDE\xAD\xBE\xEF\x00'

class StoreChunkHandler(object):
    """A simple chunk handler for dealing with string based I/O stream
    messaging. Works by one end using the sign() function to sign the
    serialized string and then using handle() on the opposite end of the wire.

    This is used by tronstore, as the pipes can get muddled."""

    def __init__(self):
        self.chunk = ''

    def sign(self, data):
        """Sign a string to be sent."""
        return (data + CHUNK_SIGNING_STR)

    def handle(self, data):
        """Handle a signed string, returning all individual strings that were
        signed as a list.
        """
        self.chunk += data
        chunks = self.chunk.split(CHUNK_SIGNING_STR)
        # split actually has this nice behavior where it makes the last element
        # of the returned list an empty string if the original string
        # ended with the sequence that was used to split with. This allows
        # a nice, simple way to either get any remaining characters, or
        # simply setting the chunk to '' again, without any extra code.
        self.chunk = chunks[-1] if chunks else ''
        chunks = chunks[:-1]
        return chunks
