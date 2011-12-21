import logging
import itertools
import random

from twisted.internet import protocol, defer, reactor
from twisted.python import failure

from tron import ssh
from tron.utils import twistedutils


log = logging.getLogger('tron.node')

# We should also only wait a certain amount of time for a connection to be
# established.
CONNECT_TIMEOUT = 30

IDLE_CONNECTION_TIMEOUT = 3600

# We should also only wait a certain amount of time for a new channel to be
# established when we already have an open connection.  This timeout will
# usually get triggered prior to even a TCP timeout, so essentially it's our
# shortcut to discovering the connection died.
RUN_START_TIMEOUT = 20

# Love to run this, but we need to finish connecting to our node first
RUN_STATE_CONNECTING = 0

# We are connected and trying to open a channel to exec the process
RUN_STATE_STARTING = 5

# Process has been exec'ed, just waiting for it to exit
RUN_STATE_RUNNING = 10

# Process has exited
RUN_STATE_COMPLETE = 100


class Error(Exception):
    pass


class ConnectError(Error):
    """There was a problem connecting, run was never started"""
    pass


class ResultError(Error):
    """There was a problem retrieving the result from this run

    We did try to execute the command, but we don't know if it succeeded or
    failed.
    """
    pass


class RunState(object):
    def __init__(self, run):
        self.run = run
        self.state = RUN_STATE_CONNECTING
        self.deferred = defer.Deferred()
        self.channel = None


class NodePool(object):
    def __init__(self, hostname=None):
        self.nodes = []
        self.iter = None
        if hostname:
            self.nodes.append(Node(hostname))

    def __eq__(self, other):
        return isinstance(other, NodePool) and self.nodes == other.nodes

    def __ne__(self, other):
        return not self == other

    def next(self):
        return self.nodes[random.randrange(len(self.nodes))]

    def next_round_robin(self):
        if not self.iter:
            self.iter = itertools.cycle(self.nodes)

        return self.iter.next()

    def __getitem__(self, value):
        for node in self.nodes:
            if node.hostname == value:
                return node
        else:
            raise KeyError(value)


class Node(object):
    """A node is tron's interface to communicating with an actual machine"""

    def __init__(self, hostname=None):
        # Host we are to connect to
        self.hostname = hostname

        # The SSH connection we use to open channels on. If present, means we
        # are connected.
        self.connection = None

        # If present, means we are trying to connect
        self.connection_defer = None

        # Map of run id to instance of RunState
        self.run_states = {}

        self.idle_timeout = None
        self.idle_timer = None

    def __cmp__(self, other):
        if not isinstance(other, self.__class__):
            return -1

        CMP_KEYS = ('hostname')
        self_dict = dict((key, value)
                         for key, value in self.__dict__.iteritems()
                         if key in CMP_KEYS)
        other_dict = dict((key, value)
                          for key, value in other.__dict__.iteritems()
                          if key in CMP_KEYS)
        return cmp(self_dict, other_dict)

    def _determine_fudge_factor(self):
        """We want to introduce some amount of delay to node exec commands

        We see issues where a service may have many instances, and they all
        start at once, and their monitor steps are syncronized for ever. This
        is bad.
        """
        outstanding_runs = len(self.run_states)
        fudge_factor = max(0.0, outstanding_runs - 4)
        return random.random() * float(fudge_factor)

    def run(self, run):
        """Execute the specified run

        A run consists of a very specific set of interfaces which allow us to
        execute a command on this remote machine and return results.
        """
        log.info("Running %s on %s", run.id, self.hostname)

        # When this run completes, for good or bad, we'll inform the caller by
        # calling 'succeed' or 'fail' on the run Since the definined interface
        # is on these specific callbacks, we won't bother returning the
        # deferred here. This allows the caller to not really care about
        # twisted specific stuff at all, all it needs to know is that one of
        # those functions will eventually be called back

        if run.id in self.run_states:
            raise Error("Run %s already running !?!", run.id)

        if self.idle_timer is not None:
            self.idle_timer.cancel()
            self.idle_timer = None

        fudge_factor = self._determine_fudge_factor()

        self.run_states[run.id] = RunState(run)

        if fudge_factor == 0.0:
            self._do_run(run)
        else:
            log.info("Delaying execution of %s for %.2f secs",
                     run.id, fudge_factor)
            reactor.callLater(fudge_factor, lambda: self._do_run(run))

        # We return the deferred here, but really we're trying to keep the rest
        # of the world from getting too involved with twisted. We will call
        # back to mark the action success/fail directly, so using this deferred
        # isn't strictly necessary.
        return self.run_states[run.id].deferred

    def _do_run(self, run):
        """Finish starting to execute a run

        This step may have been delayed.
        """

        # Now let's see if we need to start this off by establishing a
        # connection or if we are already connected
        if self.connection is None:
            self._connect_then_run(run)
        else:
            self._open_channel(run)

    def _cleanup(self, run):
        self.run_states[run.id].channel = None
        del self.run_states[run.id]

        if not self.run_states:
            self.idle_timer = reactor.callLater(IDLE_CONNECTION_TIMEOUT,
                                                self._connection_idle_timeout)

    def _connection_idle_timeout(self):
        if self.connection:
            log.info("Connection to %s idle for %d secs. Closing.",
                     self.hostname, IDLE_CONNECTION_TIMEOUT)
            self.connection.transport.loseConnection()

        self.idle_timer = None

    def _fail_run(self, run, result):
        """Indicate the run has failed, and cleanup state"""
        log.debug("Run %s has failed", run.id)
        if run.id not in self.run_states:
            log.warning("Run %s no longer tracked (_fail_run)", run.id)
            return

        # Add a dummy errback handler to prevent Unhandled error messages.
        # Unless somone is explicitly caring about this defer the error will
        # have been reported elsewhere.
        self.run_states[run.id].deferred.addErrback(lambda failure: None)

        cb = self.run_states[run.id].deferred.errback

        self._cleanup(run)

        log.info("Calling fail_run callbacks")
        run.exited(None)
        cb(result)

    def _connect_then_run(self, run):
        # Have we started the connection process ?
        if self.connection_defer is None:
            self.connection_defer = self._connect()

        def call_open_channel(arg):
            self._open_channel(run)
            return arg

        def connect_fail(result):
            log.warning("Cannot run %s, Failed to connect to %s",
                        run.id, self.hostname)
            self.connection_defer = None
            self._fail_run(run, failure.Failure(
                exc_value=ConnectError("Connection to %s failed" %
                                       self.hostname)))

        self.connection_defer.addCallback(call_open_channel)
        self.connection_defer.addErrback(connect_fail)

    def _service_stopped(self, connection):
        """Called when the SSH service has disconnected fully.

        We should be in a state where we know there are no runs in progress
        because all the SSH channels should have disconnected them.
        """
        assert self.connection is connection
        self.connection = None

        log.info("Service to %s stopped", self.hostname)

        for run_id, run in self.run_states.iteritems():
            if run.state == RUN_STATE_CONNECTING:
                # Now we can trigger a reconnect and re-start any waiting runs.
                self._connect_then_run(run)
            elif run.state == RUN_STATE_RUNNING:
                self._fail_run(run, None)
            elif run.state == RUN_STATE_STARTING:
                if run.channel and run.channel.start_defer is not None:

                    # This means our run IS still waiting to start. There
                    # should be an outstanding timeout sitting on this guy as
                    # well. We'll just short circut it.
                    twistedutils.defer_timeout(run.channel.start_defer, 0)
                else:
                    # Doesn't seem like this should ever happen.
                    log.warning("Run %r caught in starting state, but"
                                " start_defer is over.", run_id)
                    self._fail_run(run, None)
            else:
                # Service ended. The open channels should know how to handle
                # this (and cleanup) themselves, so if there should not be any
                # runs except those waiting to connect
                raise Error("Run %s in state %s when service stopped",
                            run_id, run.state)

    def _connect(self):
        # This is complicated because we have to deal with a few different
        # steps before our connection is really available for us:
        #  1. Transport is created (our client creator does this)
        #  2. Our transport is secure, and we can create our connection
        #  3. The connection service is started, so we can use it

        client_creator = protocol.ClientCreator(reactor, ssh.ClientTransport,
                                                options=self.conch_options)
        create_defer = client_creator.connectTCP(self.hostname, 22)

        # We're going to create a deferred, returned to the caller, that will
        # be called back when we have an established, secure connection ready
        # for opening channels. The value will be this instance of node.
        connect_defer = defer.Deferred()
        twistedutils.defer_timeout(connect_defer, CONNECT_TIMEOUT)

        def on_service_started(connection):
            # Booyah, time to start doing stuff
            self.connection = connection
            self.connection_defer = None

            connect_defer.callback(self)
            return connection

        def on_connection_secure(connection):
            # We have a connection, but it might not be fully ready....
            connection.service_start_defer = defer.Deferred()
            connection.service_stop_defer = defer.Deferred()

            connection.service_start_defer.addCallback(on_service_started)
            connection.service_stop_defer.addCallback(self._service_stopped)
            return connection

        def on_transport_create(transport):
            transport.connection_defer = defer.Deferred()
            transport.connection_defer.addCallback(on_connection_secure)
            return transport

        def on_transport_fail(fail):
            log.warning("Cannot connect to %s", self.hostname)

        create_defer.addCallback(on_transport_create)
        create_defer.addErrback(on_transport_fail)

        return connect_defer

    def _open_channel(self, run):
        assert self.connection
        assert self.run_states[run.id].state < RUN_STATE_RUNNING

        self.run_states[run.id].state = RUN_STATE_STARTING

        chan = ssh.ExecChannel(conn=self.connection)

        chan.addOutputCallback(run.write_stdout)
        chan.addErrorCallback(run.write_stderr)
        chan.addEndCallback(run.write_done)

        chan.command = run.command
        chan.start_defer = defer.Deferred()
        chan.start_defer.addCallback(self._run_started, run)
        chan.start_defer.addErrback(self._run_start_error, run)

        chan.exit_defer = defer.Deferred()
        chan.exit_defer.addCallback(self._channel_complete, run)
        chan.exit_defer.addErrback(self._channel_complete_unknown, run)

        twistedutils.defer_timeout(chan.start_defer, RUN_START_TIMEOUT)

        self.run_states[run.id].channel = chan
        self.connection.openChannel(chan)

    def _channel_complete(self, channel, run):
        """Callback once our channel has completed it's operation

        This is how we let our run know that we succeeded or failed.
        """
        log.info("Run %s has completed with %r", run.id, channel.exit_status)
        if run.id not in self.run_states:
            log.warning("Run %s no longer tracked", run.id)
            return

        assert self.run_states[run.id].state < RUN_STATE_COMPLETE

        self.run_states[run.id].state = RUN_STATE_COMPLETE
        cb = self.run_states[run.id].deferred.callback
        self._cleanup(run)

        run.exited(channel.exit_status)
        cb(channel.exit_status)

    def _channel_complete_unknown(self, result, run):
        """Channel has closed on a running process without a proper exit

        We don't actually know if the run succeeded
        """
        log.error("Failure waiting on channel completion: %s", str(result))
        self._fail_run(run, failure.Failure(exc_value=ResultError()))

    def _run_started(self, channel, run):
        """Our run is actually a running process now, update the state"""
        log.info("Run %s started for %s", run.id, self.hostname)
        channel.start_defer = None
        assert self.run_states[run.id].state == RUN_STATE_STARTING
        self.run_states[run.id].state = RUN_STATE_RUNNING

        run.started()

    def _run_start_error(self, result, run):
        """We failed to even run the command due to communication difficulties

        Once all the runs have closed out we can try to reconnect.
        """
        log.error("Error running %s, disconnecting from %s: %s",
                  run.id, self.hostname, str(result))

        # We clear out the deferred that likely called us because there are
        # actually more than one error paths because of user timeouts.
        self.run_states[run.id].channel.start_defer = None

        self._fail_run(run, failure.Failure(
            exc_value=ConnectError("Connection to %s failed" % self.hostname)))

        # We want to hard hangup on this connection. It could theoretically
        # come back thanks to the magic of TCP, but something is up, best to
        # fail right now then limp along for and unknown amount of time.
        #self.connection.transport.connectionLost(failure.Failure())
