import struct
import logging

from twisted.internet import defer
from twisted.conch.ssh import channel, common, keys
from twisted.conch.ssh import connection
from twisted.conch.ssh import transport
from twisted.conch.client import default
from twisted.python import failure

log = logging.getLogger('tron.ssh')


class Error(Exception):
    pass


class ChannelClosedEarlyError(Error):
    """Indicates the SSH Channel has closed before we were done handling the
    command"""
    pass


class NoPasswordAuthClient(default.SSHUserAuthClient):
    """Only support passwordless auth."""
    preferredOrder              = ['publickey']
    auth_password               = None
    auth_keyboard_interactive   = None


class ClientTransport(transport.SSHClientTransport):

    connection_defer = None

    def __init__(self, username, options, expected_pub_key):
        self.username         = username
        self.options          = options
        self.expected_pub_key = expected_pub_key

    # TODO: test
    def verifyHostKey(self, public_key, fingerprint):
        if not self.expected_pub_key:
            return defer.succeed(1)

        if self.expected_pub_key == keys.Key.fromString(public_key):
            return defer.succeed(1)

        msg = "Public key mismatch got %s expected %s"
        log.error(msg, fingerprint, self.expected_pub_key.fingerprint())
        return defer.fail(ValueError("public key mismatch"))

    def connectionSecure(self):
        conn = ClientConnection()
        conn.service_defer = defer.Deferred()
        self.connection_defer.callback(conn)

        auth_service = NoPasswordAuthClient(self.username, self.options, conn)
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

    def ssh_CHANNEL_CLOSE(self, packet):
        """The other side is closing its end.
            Payload:
                uint32  local channel number

        We've noticed many occasions when this is called but `local_channel`
        does not exist in self.channels.
        """
        local_channel = struct.unpack('>L', packet[:4])[0]
        if local_channel in self.channels:
            return connection.SSHConnection.ssh_CHANNEL_CLOSE(self, packet)


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

            # Unicode commands will cause the connection to fail
            self.command = str(self.command)

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
