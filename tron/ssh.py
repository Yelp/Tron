from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import struct

from twisted.conch.client import default
from twisted.conch.ssh import channel
from twisted.conch.ssh import common
from twisted.conch.ssh import connection
from twisted.conch.ssh import keys
from twisted.conch.ssh import transport
from twisted.internet import defer
from twisted.python import failure

log = logging.getLogger('tron.ssh')


class Error(Exception):
    pass


class ChannelClosedEarlyError(Error):
    """Indicates the SSH Channel has closed before we were done handling the
    command"""
    pass


class SSHAuthOptions(object):
    """An options class which can be used by NoPasswordAuthClient. This supports
    the interface provided by: twisted.conch.client.options.ConchOptions.
    """

    def __init__(self, identitys, use_agent):
        self.use_agent = use_agent
        self.identitys = identitys

    @classmethod
    def from_config(cls, ssh_config):
        return cls(ssh_config.identities, ssh_config.agent)

    def __getitem__(self, item):
        if item != 'noagent':
            raise KeyError(item)
        return not self.use_agent

    def __eq__(self, other):
        return other and (
            self.use_agent == other.use_agent and
            self.identitys == other.identitys
        )

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        context = self.__class__.__name__, self.identitys, self.use_agent
        return "%s(%s, %s)" % context


class NoPasswordAuthClient(default.SSHUserAuthClient):
    """Only support passwordless auth."""
    preferredOrder = ['publickey']
    auth_password = None
    auth_keyboard_interactive = None


class ClientTransport(transport.SSHClientTransport):

    connection_defer = None

    def __init__(self, username, options, expected_pub_key):
        self.username = username
        self.options = options
        self.expected_pub_key = expected_pub_key

    def verifyHostKey(self, public_key, fingerprint):
        if not self.expected_pub_key:
            return defer.succeed(1)

        if self.expected_pub_key == keys.Key.fromString(public_key):
            return defer.succeed(2)

        msg = "Public key mismatch got %s expected %s" % (
            fingerprint,
            self.expected_pub_key.fingerprint(),
        )
        log.error(msg)
        return defer.fail(ValueError(msg))

    def connectionSecure(self):
        conn = ClientConnection()
        # TODO: this should be initialized by the ClientConnection constructor
        conn.service_defer = defer.Deferred()
        # TODO: this should be initialized by the constructor
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
        if channel.id in self.deferreds:
            del self.deferreds[channel.id]

    def ssh_CHANNEL_REQUEST(self, packet):
        """
        The other side is sending a request to a channel.  Payload::
            uint32  local channel number
            string  request name
            bool    want reply
            <request specific data>

        Handles missing local channel.
        """
        localChannel = struct.unpack('>L', packet[:4])[0]
        if localChannel not in self.channels:
            requestType, _ = common.getNS(packet[4:])
            host = self.transport.transport.getPeer()
            msg = "Missing channel: %s, request_type: %s, host: %s"
            log.warn(msg, localChannel, requestType, host)
            return
        connection.SSHConnection.ssh_CHANNEL_REQUEST(self, packet)


class ExecChannel(channel.SSHChannel):

    name = b'session'
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

            self.command = self.command.encode('utf-8')

            req = self.conn.sendRequest(
                self,
                b'exec',
                common.NS(self.command),
                wantReply=True,
            )
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
        status = struct.unpack_from(b'>L', data, 0)[0]

        log.debug("Received exit status request: %d", status)
        self.exit_status = status
        self.exit_defer.callback(self)
        self.running = False
        return True

    def dataReceived(self, data):
        self.data = [data]
        for callback in self.output_callbacks:
            callback(data)

    def extReceived(self, dataType, data):
        self.data = [data]
        for callback in self.error_callbacks:
            callback(data)

    def getStdout(self):
        return "".join(self.data)

    def closed(self):
        if (
            self.exit_status is None and self.running and self.exit_defer and
            not self.exit_defer.called
        ):
            log.warning(
                "Channel has been closed without receiving an exit"
                " status",
            )
            f = failure.Failure(exc_value=ChannelClosedEarlyError())
            self.exit_defer.errback(f)

        for callback in self.end_callbacks:
            callback()
        # TODO: this is triggered by loseConnection, we shouldn't need to call it
        # again here
        self.loseConnection()
