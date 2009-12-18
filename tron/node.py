import logging

from twisted.internet import protocol, defer, reactor

from tron import ssh


log = logging.getLogger('tron.node')

RUN_STATE_CONNECTING = 0    # Love to run this, but we need to finish connecting to our node first
RUN_STATE_STARTING = 5      # We are connected and trying to open a channel to exec the process
RUN_STATE_RUNNING = 10      # Process has been exec'ed, just waiting for it to exit
RUN_STATE_COMPLETE = 100    # Process has exited

class Error(Exception): pass

class RunState(object):
    def __init__(self, run):
        self.run = run
        self.state = RUN_STATE_CONNECTING
        self.deferred = defer.Deferred()

class Node(object):
    """A node is tron's interface to communicating with an actual machine"""
    def __init__(self, hostname=None):
        # Host we are to connect to
        self.hostname = hostname
        
        # The SSH connection we use to open channels on
        self.connection = None          # If present, means we are connected
        self.connection_defer = None    # If present, means we are trying to connect
    
        self.run_states = {}       # Map of run id to instance of RunState

    def run(self, run):
        """Execute the specified run
        
        A run consists of a very specific set of interfaces which allow us to execute a command on this remote machine and
        return results.
        """
        
        # When this run completes, for good or bad, we'll inform the caller by calling 'succeed' or 'fail' on the run
        # Since the definined interface is on these specific callbacks, we won't bother returning the deferred here. This
        # allows the caller to not really care about twisted specific stuff at all, all it needs to know is that one of those
        # functions will eventually be called back

        if run.id in self.run_states:
            raise Error("Run %s already running !?!", run.id)

        self.run_states[run.id] = RunState(run)

        # Now let's see if we need to start this off by establishing a connection or if we are already connected
        if self.connection is None:
            self._connect_then_run(run)
        else:
            self._open_channel(run)
    
        # We return the deferred here, but really we're trying to keep the rest of the world from getting too
        # involved with twisted. We will call back to mark the job success/fail directly, so using this deferred
        # isn't strictly necessary.
        return self.run_states[run.id]

    def _connect_then_run(self, run):
        # Have we started the connection process ?
        if self.connection_defer is None:
            self.connection_defer = self._connect()
        
        def call_open_channel(arg):
            self._open_channel(run)
            return arg

        self.connection_defer.addCallback(call_open_channel)
    
    def _service_stopped(self, connection):
        """Called when the SSH service has disconnected fully.
        
        We should be in a state where we know there are no runs in progress because all the SSH channels should 
        have disconnected them.
        """
        assert self.connection is connection
        self.connection = None

        log.info("Service to %s stopped", self.hostname)

        for run_id, run in self.run_states.iteritems():
            if run.state != RUN_STATE_CONNECTING:
                # Service ended. The open channels should know how to handle this (and cleanup) themselves, so
                # if there should not be any runs except those waiting to connect
                raise Error("Run %s in state %s when service stopped", run_id, run.state)

            # Now we can trigger a reconnect and re-start any waiting runs.
            self._connect_then_run(run)
        
    def _connect(self):
        # This is complicated because we have to deal with a few different steps before our connection is really available for us:
        #  1. Transport is created (our client creator does this)
        #  2. Our transport is secure, and we can create our connection
        #  3. The connection service is started, so we can use it
        
        # TODO: We need a timeout and handle error conditions to err all the runs waiting on this
        
        client_creator = protocol.ClientCreator(reactor, ssh.ClientTransport)
        create_defer = client_creator.connectTCP(self.hostname, 22)

        # We're going to create a deferred, returned to the caller, that will be called back when we
        # have an established, secure connection ready for opening channels. The value will be this instance
        # of node.
        connect_defer = defer.Deferred()
        
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
         
        create_defer.addCallback(on_transport_create)
        return connect_defer
        
    def _open_channel(self, run):
        assert self.connection
        assert self.run_states[run.id].state < RUN_STATE_RUNNING
        
        self.run_states[run.id].state = RUN_STATE_STARTING

        chan = ssh.ExecChannel(conn=self.connection)

        chan.command = run.command
        chan.start_defer = defer.Deferred()
        chan.start_defer.addCallback(self._run_started, run)
        chan.start_defer.addErrback(self._run_start_error, run)

        chan.exit_defer = defer.Deferred()
        chan.exit_defer.addCallback(self._channel_complete, run)
        chan.exit_defer.addErrback(self._channel_complete_unknown, run)
        
        # TODO: We'll maybe need to setup timers here with run.timeout_secs
        
        self.connection.openChannel(chan)
    
    def _channel_complete(self, channel, run):
        """Callback once our channel has completed it's operation
        
        This is how we let our run know that we succeeded or failed.
        """
        assert self.run_states[run.id].state < RUN_STATE_COMPLETE
        self.run_states[run.id].state = RUN_STATE_COMPLETE

        # TODO: Should probably do something with the output
        if channel.exit_status != 0:
            run.fail(channel.exit_status)
        else:
            run.succeed()
        
        self.run_states[run.id].deferred.callback(run)

        # Cleanup, we care nothing about this run anymore
        del self.run_states[run.id]

    def _channel_complete_unknown(self, channel, run):
        """Channel has closed on a running process without a proper exit
        
        We don't actually know if the run succeeded
        """
        run.fail_unknown()
        self.run_states[run.id].deferred.errback(run)

        # Cleanup, we care nothing this run anymore
        del self.run_states[run.id]

    def _run_started(self, channel, run):
        """Our run is actually a running process now, update the state"""
        log.info("Run %s started for %s", run.id, self.hostname)
        assert self.run_states[run.id].state == RUN_STATE_STARTING
        self.run_states[run.id].state = RUN_STATE_RUNNING
        
    def _run_start_error(self, channel, run):
        """We failed to even run the command due to communication difficulties
        
        We're going to mark this run as back to connecting.
        Once all the runs have closed out we can try to reconnect.
        """
        raise Exception("STOOOP!!")
        assert self.run_states[run.id].state < RUN_STATE_RUNNING
        log.error("Error running %s, disconnecting from %s", run.id, self.hostname)
        self.run_states[run.id].state = RUN_STATE_CONNECTING
        