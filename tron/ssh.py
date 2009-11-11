import os
import struct

from twisted.internet import protocol, reactor, defer

from twisted.conch import error
from twisted.conch.ssh import channel, common
from twisted.conch.ssh import connection
from twisted.conch.ssh import keys, userauth, agent
from twisted.conch.ssh import transport
from twisted.conch.client import default, options
from twisted.python import log


class ClientUserAuth(userauth.SSHUserAuthClient):
    def __init__(self, user, instance):
        super(ClientUserAuth, self).__init__(user, instance)
        self.agent = SSHAgentClient()
        self.agent.requestIdentities().addCallback(self._cbAgentIdentities)

    def _cbAgentIdentities(self, data):
        self.identities = data

    def getPassword(self, prompt = None):
        # this says we won't do password authentication
        return

    def getPublicKey(self):
        # public_key_data = open("/Users/rhettg/.ssh/identity.pub").read()
        # return keys.getPublicKeyString(data = public_key_data)
        return keys.Key.fromFile("/Users/rhettg/.ssh/identity.pub").blob()

    def signData(self, publicKey, signData):
        return self.agent.signData(publicKey, signData)
        

class ClientTransport(transport.SSHClientTransport):

    def verifyHostKey(self, pubKey, fingerprint):
        return defer.succeed(1)
        # if fingerprint != 'b1:94:6a:c9:24:92:d2:34:7c:62:35:b4:d2:61:11:84':
        #     return defer.fail(error.ConchError('bad key'))
        # else:
        #     return defer.succeed(1)

    def connectionSecure(self):
        self.requestService(default.SSHUserAuthClient(os.getlogin(), options.ConchOptions(), ClientConnection()))
        #self.requestService(default.SSHUserAuthClient('user', ClientConnection()))

class ClientConnection(connection.SSHConnection):
    def serviceStarted(self):
        self.openChannel(CatChannel(conn=self))

class CatChannel(channel.SSHChannel):
    name = 'session'

    def channelOpen(self, data):
        env = common.NS('TEST_ENV') + common.NS("hello")
        # env setting doesn't appear to work. We get a "channel request failed"
        #self.conn.sendRequest(self, 'env', env, wantReply=True).addCallback(self._cbEnvSendRequest)
        self.conn.sendRequest(self, 'exec', common.NS('env'), wantReply=True).addCallback(self._cbExecSendRequest)

    # def _cbEnvSendRequest(self, ignored):
    #     self.conn.sendEOF(self)
    # 
    #     self.conn.sendRequest(self, 'exec', common.NS('env'), wantReply=True).addCallback(self._cbExecSendRequest)

    def _cbExecSendRequest(self, ignored):
        #self.write('This data will be echoed back to us by "cat."\r\n')
        self.conn.sendEOF(self)

    def request_exit_status(self, data):
        # exit status is a 32-bit unsigned int in network byte format
        status = struct.unpack_from(">L", data, 0)[0]

        print "Received exit status request: %d" % (status,)
        return True

    def dataReceived(self, data):
        print data

    def closed(self):
        self.loseConnection()
        if reactor.running:
            reactor.stop()

def main():
    log.startLogging(open('ssh.log', 'w'))
    
    factory = protocol.ClientFactory()
    factory.protocol = ClientTransport
    reactor.connectTCP('dev01', 22, factory)
    reactor.run()

if __name__ == "__main__":
    main()
