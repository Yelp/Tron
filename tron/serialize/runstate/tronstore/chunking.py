CHUNK_SIGNING_STR = '\xDE\xAD\xBE\xEF\x00'

class StoreChunkHandler(object):

    def __init__(self):
        self.chunk = ''

    def sign(self, data):
        return (data + CHUNK_SIGNING_STR)

    def handle(self, data):
        self.chunk += data
        chunks = self.chunk.split(CHUNK_SIGNING_STR)
        # split actually has this nice behavior where it makes the last element
        # of the list of split strings an empty string if the original string
        # ended with the sequence that was used to split with. This allows
        # a nice, simple way to either get any remaining characters, or
        # simply setting the chunk to '' again.
        self.chunk = chunks[-1] if chunks else ''
        chunks = chunks[:-1]
        return chunks
