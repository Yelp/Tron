import itertools
import logging
import random

import six
from twisted.conch.client.knownhosts import KnownHostsFile
from twisted.internet import defer
from twisted.internet import protocol
from twisted.internet import reactor
from twisted.python import failure
from twisted.python.filepath import FilePath

from tron import ssh
from tron.utils import collections
from tron.utils import twistedutils

log = logging.getLogger(__name__)

# We should also only wait a certain amount of time for a new channel to be
# established when we already have an open connection.  This timeout will
# usually get triggered prior to even a TCP timeout, so essentially it's our
# shortcut to discovering the connection died.
RUN_START_TIMEOUT = 120

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


class NodePoolRepository(object):
    """A Singleton to store Node and NodePool objects."""

    _instance = None

    def __init__(self):
        if self._instance is not None:
            raise ValueError("NodePoolRepository is already instantiated.")
        super(NodePoolRepository, self).__init__()
        self.nodes = collections.MappingCollection('nodes')
        self.pools = collections.MappingCollection('pools')

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def filter_by_name(self, node_configs, node_pool_configs):
        self.nodes.filter_by_name(node_configs)
        self.pools.filter_by_name(
            list(node_configs.keys()) + list(node_pool_configs.keys()),
        )

    @classmethod
    def update_from_config(cls, node_configs, node_pool_configs, ssh_config):
        instance = cls.get_instance()
        ssh_options = ssh.SSHAuthOptions.from_config(ssh_config)
        known_hosts = KnownHosts.from_path(ssh_config.known_hosts_file)
        instance.filter_by_name(node_configs, node_pool_configs)
        instance._update_nodes(
            node_configs,
            ssh_options,
            known_hosts,
            ssh_config,
        )
        instance._update_node_pools(node_pool_configs)

    def _update_nodes(
        self,
        node_configs,
        ssh_options,
        known_hosts,
        ssh_config,
    ):
        for config in six.itervalues(node_configs):
            pub_key = known_hosts.get_public_key(config.hostname)
            node = Node.from_config(config, ssh_options, pub_key, ssh_config)
            self.add_node(node)

    def _update_node_pools(self, node_pool_configs):
        for config in six.itervalues(node_pool_configs):
            nodes = self._get_nodes_by_name(config.nodes)
            pool = NodePool.from_config(config, nodes)
            self.pools.replace(pool)

    def add_node(self, node):
        self.nodes.replace(node)
        self.pools.replace(NodePool.from_node(node))

    def get_node(self, node_name, default=None):
        return self.nodes.get(node_name, default)

    def __contains__(self, node):
        return node.get_name() in self.pools

    def get_by_name(self, name, default=None):
        return self.pools.get(name, default)

    def _get_nodes_by_name(self, names):
        return [self.nodes[name] for name in names]

    def clear(self):
        self.nodes.clear()
        self.pools.clear()


class NodePool(object):
    """A pool of Node objects."""

    def __init__(self, nodes, name):
        self.nodes = nodes
        self.disabled = False
        self.name = name or '_'.join(n.get_name() for n in nodes)
        self.iter = itertools.cycle(self.nodes)

    @classmethod
    def from_config(cls, node_pool_config, nodes):
        return cls(nodes, node_pool_config.name)

    @classmethod
    def from_node(cls, node):
        return cls([node], node.get_name())

    def __eq__(self, other):
        return isinstance(other, NodePool) and self.nodes == other.nodes

    def __ne__(self, other):
        return not self == other

    def get_name(self):
        return self.name

    def get_nodes(self):
        return self.nodes

    def next(self):
        """Return a random node from the pool."""
        return random.choice(self.nodes)

    def next_round_robin(self):
        """Return the next node cycling in a consistent order."""
        return next(self.iter)

    def disable(self):
        """Required for MappingCollection.Item interface."""
        self.disabled = True

    def get_by_hostname(self, hostname):
        for node in self.nodes:
            if node.hostname == hostname:
                return node

    def __str__(self):
        return "NodePool:%s" % self.name


class KnownHosts(KnownHostsFile):
    """Lookup host key for a hostname."""

    @classmethod
    def from_path(cls, file_path):
        if not file_path:
            return cls(None)
        return cls.fromPath(FilePath(file_path))

    def get_public_key(self, hostname):
        for entry in self.iterentries():
            if entry.matchesHost(hostname):
                return entry.publicKey
        log.warning("Missing host key for: %s", hostname)


class RunState(object):
    def __init__(self, action_run):
        self.run = action_run
        self.state = RUN_STATE_CONNECTING
        self.deferred = defer.Deferred()
        self.channel = None

    def __repr__(self):
        return "RunState(run: %r, state: %r, channel: %r)" % (
            self.run,
            self.state,
            self.channel,
        )


def determine_jitter(count, node_settings):
    """Return a pseudo-random number of seconds to delay a run."""
    count *= node_settings.jitter_load_factor
    min_count = node_settings.jitter_min_load
    max_jitter = max(0.0, count - min_count)
    max_jitter = min(node_settings.jitter_max_delay, max_jitter)
    return random.random() * float(max_jitter)


class Node(object):
    """A node is tron's interface to communicating with an actual machine.
    """

    def __init__(self, config, ssh_options, pub_key, node_settings):
        self.config = config
        self.node_settings = node_settings

        # SSH Options
        self.conch_options = ssh_options

        # The SSH connection we use to open channels on. If present, means we
        # are connected.
        self.connection = None

        # If present, means we are trying to connect
        self.connection_defer = None

        # Map of run id to instance of RunState
        self.run_states = {}

        self.idle_timer = None
        self.disabled = False
        self.pub_key = pub_key

    @property
    def hostname(self):
        return self.config.hostname

    @property
    def username(self):
        return self.config.username

    @property
    def port(self):
        return self.config.port

    @classmethod
    def from_config(cls, node_config, ssh_options, pub_key, node_settings):
        return cls(node_config, ssh_options, pub_key, node_settings)

    def get_name(self):
        return self.config.name

    name = property(get_name)

    def disable(self):
        """Required for MappingCollection.Item interface."""
        self.disabled = True

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (
            self.config == other.config and
            self.conch_options == other.conch_options and
            self.pub_key == other.pub_key and
            self.node_settings == other.node_settings
        )

    def __ne__(self, other):
        return not self == other

    # TODO: Test
    def submit_command(self, command):
        """Submit an ActionCommand to be run on this node. Optionally provide
        an error callback which will be called on error.
        """
        deferred = self.run(command)
        deferred.addErrback(command.handle_errback)
        return deferred

    def run(self, run):
        """Execute the specified run

        A run consists of a very specific set of interfaces which allow us to
        execute a command on this remote machine and return results.
        """
        log.info("Running %s for %s on %s", run.command, run.id, self.hostname)

        # When this run completes, for good or bad, we'll inform the caller by
        # calling 'succeed' or 'fail' on the run Since the defined interface
        # is on these specific callbacks, we won't bother returning the
        # deferred here. This allows the caller to not really care about
        # twisted specific stuff at all, all it needs to know is that one of
        # those functions will eventually be called back

        if run.id in self.run_states:
            log.warning(
                "Run %s(%r) already running !?!",
                run.id,
                self.run_states[run.id],
            )

        if self.idle_timer and self.idle_timer.active():
            self.idle_timer.cancel()

        self.run_states[run.id] = RunState(run)

        # TODO: have this return a runner instead of number
        fudge_factor = determine_jitter(
            len(self.run_states),
            self.node_settings,
        )
        if fudge_factor == 0.0:
            self._do_run(run)
        else:
            log.info(
                "Delaying execution of %s for %.2f secs",
                run.id,
                fudge_factor,
            )
            reactor.callLater(fudge_factor, self._do_run, run)

        # We return the deferred here, but really we're trying to keep the rest
        # of the world from getting too involved with twisted.
        return self.run_states[run.id].deferred

    def stop(self, command):
        """Stop this command by marking it as failed."""
        exc = failure.Failure(exc_value=ResultError("Run stopped"))
        self._fail_run(command, exc)

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
        # TODO: why set to None before deleting it?
        self.run_states[run.id].channel = None
        del self.run_states[run.id]

        if not self.run_states:
            self.idle_timer = reactor.callLater(
                self.node_settings.idle_connection_timeout,
                self._connection_idle_timeout,
            )

    def _connection_idle_timeout(self):
        if self.connection:
            log.info(
                "Connection to %s idle for %d secs. Closing.",
                self.hostname,
                self.node_settings.idle_connection_timeout,
            )
            self.connection.transport.loseConnection()

    def _fail_run(self, run, result):
        """Indicate the run has failed, and cleanup state"""
        log.debug("Run %s has failed", run.id)
        if not self._is_run_id_tracked(run):
            log.warning("Run %s no longer tracked (_fail_run)", run.id)
            return

        # Add a dummy errback handler to prevent Unhandled error messages.
        # Unless someone is explicitly caring about this defer the error will
        # have been reported elsewhere.
        self.run_states[run.id].deferred.addErrback(lambda failure: None)

        cb = self.run_states[run.id].deferred.errback

        self._cleanup(run)

        log.info("Calling fail_run callbacks")
        run.exited(None)
        cb(result)

    def _is_run_id_tracked(self, run):
        return run.id in self.run_states and self.run_states[run.id].run is run

    def _connect_then_run(self, run):
        # Have we started the connection process ?
        if self.connection_defer is None:
            self.connection_defer = self._connect()

        def call_open_channel(arg):
            self._open_channel(run)
            return arg

        def connect_fail(result):
            log.warning(
                "Cannot run %s, Failed to connect to %s",
                run,
                self.hostname,
            )
            self.connection_defer = None
            self._fail_run(
                run,
                failure.Failure(
                    exc_value=ConnectError(
                        "Connection to %s@%s:%d failed" % (
                            self.username,
                            self.hostname,
                            self.port,
                        ),
                    ),
                ),
            )

        self.connection_defer.addCallback(call_open_channel)
        self.connection_defer.addErrback(connect_fail)

    def _service_stopped(self, connection):
        """Called when the SSH service has disconnected fully.

        We should be in a state where we know there are no runs in progress
        because all the SSH channels should have disconnected them.
        """
        if self.connection is not connection:
            log.warning("Service stop has been called twice")
            return
        self.connection = None

        log.info("Service to %s stopped", self.hostname)

        for run_id, run in six.iteritems(self.run_states):
            if run.state == RUN_STATE_CONNECTING:
                # Now we can trigger a reconnect and re-start any waiting runs.
                self._connect_then_run(run)
            elif run.state == RUN_STATE_RUNNING:
                self._fail_run(run, None)
            elif run.state == RUN_STATE_STARTING:
                if run.channel and run.channel.start_defer is not None:

                    # This means our run IS still waiting to start. There
                    # should be an outstanding timeout sitting on this guy as
                    # well. We'll just short circuit it.
                    twistedutils.defer_timeout(run.channel.start_defer, 0)
                else:
                    # Doesn't seem like this should ever happen.
                    log.warning(
                        "Run %r caught in starting state, but"
                        " start_defer is over.",
                        run_id,
                    )
                    self._fail_run(run, None)
            else:
                # Service ended. The open channels should know how to handle
                # this (and cleanup) themselves, so if there should not be any
                # runs except those waiting to connect
                raise Error(
                    "Run %s in state %s when service stopped",
                    run_id,
                    run.state,
                )

    def _connect(self):
        # This is complicated because we have to deal with a few different
        # steps before our connection is really available for us:
        #  1. Transport is created (our client creator does this)
        #  2. Our transport is secure, and we can create our connection
        #  3. The connection service is started, so we can use it

        client_creator = protocol.ClientCreator(
            reactor,
            ssh.ClientTransport,
            self.username,
            self.conch_options,
            self.pub_key,
        )
        create_defer = client_creator.connectTCP(
            self.hostname,
            self.config.port,
        )

        # We're going to create a deferred, returned to the caller, that will
        # be called back when we have an established, secure connection ready
        # for opening channels. The value will be this instance of node.
        connect_defer = defer.Deferred()
        twistedutils.defer_timeout(
            connect_defer,
            self.node_settings.connect_timeout,
        )

        def on_service_started(connection):
            # Booyah, time to start doing stuff
            if self.connection:
                log.error(
                    "Host %s service started called before disconnect(%s, %s)",
                    self.hostname,
                    self.connection,
                    connection,
                )
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
            connect_defer.errback(fail)

        create_defer.addCallback(on_transport_create)
        create_defer.addErrback(on_transport_fail)

        return connect_defer

    def _open_channel(self, run):
        assert self.connection
        if not self._is_run_id_tracked(run):
            log.warning("Run %s no longer tracked (_open_channel)", run.id)
            return
        assert self.run_states[run.id].state < RUN_STATE_RUNNING

        self.run_states[run.id].state = RUN_STATE_STARTING

        chan = ssh.ExecChannel(conn=self.connection)

        chan.addOutputCallback(run.write_stdout)
        chan.addErrorCallback(run.write_stderr)
        chan.addEndCallback(run.done)

        chan.command = run.command
        chan.start_defer = defer.Deferred()
        chan.start_defer.addCallback(self._run_started, run)
        chan.start_defer.addErrback(self._run_start_error, run)

        chan.exit_defer = defer.Deferred()
        chan.exit_defer.addCallback(self._channel_complete, run)
        chan.exit_defer.addErrback(self._channel_complete_unknown, run)

        twistedutils.defer_timeout(chan.start_defer, RUN_START_TIMEOUT)

        self.run_states[run.id].channel = chan
        # TODO: I believe this needs to be checking the health of the connection
        # before trying to open a new channel.  If the connection is gone it
        # needs to re-establish, or if the connection is not responding
        # we shouldn't create this new channel
        self.connection.openChannel(chan)

    def _channel_complete(self, channel, run):
        """Callback once our channel has completed it's operation

        This is how we let our run know that we succeeded or failed.
        """
        log.info("Run %s has completed with %r", run.id, channel.exit_status)
        if not self._is_run_id_tracked(run):
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
        if not self._is_run_id_tracked(run):
            log.warning("Run %s no longer tracked (_run_started)", run.id)
            return
        assert self.run_states[run.id].state == RUN_STATE_STARTING
        self.run_states[run.id].state = RUN_STATE_RUNNING

        run.started()

    def _run_start_error(self, result, run):
        """We failed to even run the command due to communication difficulties

        Once all the runs have closed out we can try to reconnect.
        """
        log.error(
            "Error running %s, disconnecting from %s: %s",
            run.id,
            self.hostname,
            str(result),
        )

        # We clear out the deferred that likely called us because there are
        # actually more than one error paths because of user timeouts.
        if run.id in self.run_states and self.run_states[run.id].channel:
            self.run_states[run.id].channel.start_defer = None

        self._fail_run(
            run,
            failure.Failure(
                exc_value=ConnectError(
                    "Connection to %s@%s:%d failed" % (
                        self.username,
                        self.hostname,
                        self.port,
                    ),
                ),
            ),
        )

        # We want to hard hangup on this connection. It could theoretically
        # come back thanks to the magic of TCP, but something is up, best to
        # fail right now then limp along for and unknown amount of time.
        # self.connection.transport.connectionLost(failure.Failure())

    def __str__(self):
        return "Node:%s@%s:%s" % (
            self.username or "<default>",
            self.hostname,
            self.config.port,
        )

    def __repr__(self):
        return self.__str__()
