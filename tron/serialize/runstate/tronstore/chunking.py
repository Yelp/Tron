CHUNK_SIGNING_STR = '\xDE\xAD\xBE\xEF\x00'

class StoreChunkHandler(object):

    def __init__(self):
        self.chunk = ''

    def sign(self, data):
        return (data + CHUNK_SIGNING_STR)

    def handle(self, data):
        self.chunk += data
        chunks = self.chunk.split(CHUNK_SIGNING_STR)
        # if the chunk doesn't end with the signed string, the message was
        # incomplete and should be saved for the next time .handle is called.
        if not self.chunk.endswith(CHUNK_SIGNING_STR):
            self.chunk = chunks[-1:]
            chunks = chunks[:-1]
        return chunks
