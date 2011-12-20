import os
import pwd
import struct
import logging

from twisted.internet import protocol, reactor, defer
from twisted.cred import credentials
from twisted.conch import error
from twisted.conch.ssh import channel, common
from twisted.conch.ssh import connection
from twisted.conch.ssh import keys, userauth, agent
from twisted.conch.ssh import transport
from twisted.conch.client import default, options
from twisted.python import log, failure

log = logging.getLogger('tron.ssh')


class Error(Exception):
    pass


class ChannelClosedEarlyError(Error):
    """Indicates the SSH Channel has closed before we were done handling the
    command"""
    pass


# We need to sub-class and redefine the AuthClient here because there is no way
# with the default client to force it to not try certain authentication
# methods. If things get much worse I'll just have to make our own custom
# version of the default module.
class NoPasswordAuthClient(default.SSHUserAuthClient):
    def tryAuth(self, kind):
        kind = kind.replace('-', '_')
        if kind != 'publickey':
            log.info('skipping auth method %s (not supported)' % kind)
            return

        log.info('trying to auth with %s!' % kind)
        f = getattr(self, 'auth_%s' % kind, None)
        if f:
            return f()
        else:
            return


class ClientTransport(transport.SSHClientTransport):

    connection_defer = None

    def __init__(self, *args, **kwargs):
        # These silly twisted classes tend not to have init functions, and
        # they're all old style classes
        # transport.SSHClientTransport.__init__(self, *args, **kwargs)

        if 'options' in kwargs:
            self.options = kwargs['options']

    def verifyHostKey(self, pubKey, fingerprint):
        return defer.succeed(1)

    def connectionSecure(self):
        conn = ClientConnection()
        conn.service_defer = defer.Deferred()

        self.connection_defer.callback(conn)

        auth_service = NoPasswordAuthClient(pwd.getpwuid(os.getuid())[0],
                                            self.options, conn)

        self.requestService(auth_service)


class ClientConnection(connection.SSHConnection):

    service_start_defer = None
    service_stop_defer = None

    def serviceStarted(self):
        log.info("Service started")
        connection.SSHConnection.serviceStarted(self)
        if not self.service_stop_defer.called:
            self.service_start_defer.callback(self)

    def serviceStopped(self):
        log.info("Service stopped")
        connection.SSHConnection.serviceStopped(self)
        if not self.service_stop_defer.called:
            self.service_stop_defer.callback(self)

    def channelClosed(self, channel):
        if not channel.conn:
            log.warning("Channel %r failed to open", channel.id)
            # Channel has no connection, so we were still trying to open it The
            # normal error handling won't notify us since the channel never
            # successfully opened.
            channel.openFailed(None)

        connection.SSHConnection.channelClosed(self, channel)


class ExecChannel(channel.SSHChannel):

    name = 'session'
    exit_defer = None
    start_defer = None

    command = None
    exit_status = None
    running = False

    def __init__(self, *args, **kwargs):
        channel.SSHChannel.__init__(self, *args, **kwargs)
        self.output_callbacks = []
        self.end_callbacks = []
        self.error_callbacks = []
        self.data = []

    def channelOpen(self, data):
        self.data = []
        self.running = True

        if self.start_defer:
            log.debug("Channel %s is open, calling deferred", self.id)
            self.start_defer.callback(self)
            req = self.conn.sendRequest(self, 'exec',
                                        common.NS(self.command),
                                        wantReply=True)
            req.addCallback(self._cbExecSendRequest)
        else:
            # A missing start defer means that we are no longer expected to do
            # anything when the channel opens It probably means we gave up on
            # this connection and failed the job, but later the channel opened
            # up correctly.
            log.warning("Channel open delayed, giving up and closing")
            self.loseConnection()

    def addOutputCallback(self, output_callback):
        self.output_callbacks.append(output_callback)

    def addErrorCallback(self, error_callback):
        self.error_callbacks.append(error_callback)

    def addEndCallback(self, end_callback):
        self.end_callbacks.append(end_callback)

    def openFailed(self, reason):
        log.error("Open failed due to %r", reason)
        if self.start_defer:
            self.start_defer.errback(self)

    def _cbExecSendRequest(self, ignored):
        self.conn.sendEOF(self)

    def request_exit_status(self, data):
        # exit status is a 32-bit unsigned int in network byte format
        status = struct.unpack_from(">L", data, 0)[0]

        log.debug("Received exit status request: %d", status)
        self.exit_status = status
        self.exit_defer.callback(self)
        self.running = False
        return True

    def dataReceived(self, data):
        self.data.append(data)
        for callback in self.output_callbacks:
            callback(data)

    def extReceived(self, dataType, data):
        self.data.append(data)
        for callback in self.error_callbacks:
            callback(data)

    def getStdout(self):
        return "".join(self.data)

    def closed(self):
        if (self.exit_status is None and
            self.running and
            self.exit_defer and
            not self.exit_defer.called):
            log.warning("Channel has been closed without receiving an exit"
                        " status")
            f = failure.Failure(exc_value=ChannelClosedEarlyError())
            self.exit_defer.errback(f)

        for callback in self.end_callbacks:
            callback()
        self.loseConnection()
