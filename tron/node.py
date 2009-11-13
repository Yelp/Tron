from tron import ssh

from twisted.internet import protocol, defer, reactor

RUN_STATE_CONNECTING = 0
RUN_STATE_RUNNING = 10
RUN_STATE_COMPLETE = 100

class Node(object):
    """A node is tron's interface to communicating with an actual machine"""
    def __init__(self, hostname=None):
        # Host we are to connect to
        self.hostname = hostname
        
        # The SSH connection we use to open channels on
        self.connection = None
        self.connection_defer = None
    
        self.run_defer = {}
        self.run_state = {}

    def run(self, run):
        """Execute the specified run
        
        A run consists of a very specific set of interfaces which allow us to execute a command on this remote machine and
        return results.
        """
        if self.connection is None:
            self.run_state[run.id] = RUN_STATE_CONNECTING
            if self.connection_defer is None:
                self.connection_defer = self._connect()
            
            def call_open_channel(arg):
                self._open_channel(run)
                return arg
            self.connection_defer.addCallback(call_open_channel)
        else:
            self._open_channel(run)

        # When this run completes, for good or bad, we'll inform the caller by calling 'succeed' or 'fail' on the run
        # Since the definined interface is on these specific callbacks, we won't bother returning the deferred here. This
        # allows the caller to not really care about twisted specific stuff at all, all it needs to know is that one of those
        # functions will eventually be called back

        df = defer.Deferred()
        self.run_defer[run.id] = df
    
    def _connect(self):
        # This is complicated because we have to deal with a few different steps before our connection is really available for us:
        #  1. Transport is created (our client creator does this)
        #  2. Our transport is secure, and we can create our connection
        #  3. The connection service is started, so we can use it
        
        client_creator = protocol.ClientCreator(reactor, ssh.ClientTransport)
        create_defer = client_creator.connectTCP(self.hostname, 22)

        # We're going to create a deferred, returned to the caller, that will be called back when we
        # have an established, secure connection ready for opening channels. The value will be this instance
        # of node.
        connect_defer = defer.Deferred()
        
        def on_service_started(connection):
            connect_defer.callback(self)
            return connection

        def on_connection_secure(connection):
            self.connection = connection
            connection.service_defer.addCallback(on_service_started)
            return connection
            
        def on_transport_create(transport):
            transport.connection_defer = defer.Deferred()
            transport.connection_defer.addCallback(on_connection_secure)
            return transport
         
        create_defer.addCallback(on_transport_create)
        return connect_defer
        
    def _open_channel(self, run):
        assert self.connection
        assert self.run_state[run.id] < RUN_STATE_RUNNING
        
        self.run_state[run.id] = RUN_STATE_RUNNING

        chan = ssh.ExecChannel(conn=self.connection)

        chan.command = run.command
        chan.exit_defer = defer.Deferred()
        chan.exit_defer.addCallback(self._channel_complete, run)
        
        self.connection.openChannel(chan)
        # TODO: We'll maybe need to setup timers here with run.timeout_secs
    
    def _channel_complete(self, channel, run):
        """Callback once our channel has completed it's operation
        
        This is how we let our run know that we succeeded or failed.
        """
        assert self.run_state[run.id] < RUN_STATE_COMPLETE
        self.run_state[run.id] = RUN_STATE_COMPLETE

        # TODO: Should probably do something with the output
        if channel.exit_status != 0:
            run.fail(channel.exit_status)
        else:
            run.succeed()
        
        self.run_defer[run.id].callback(run_result)

        # Cleanup, we care nothing this run anymore
        del self.run_state[run.id]
        del self.run_defer[run.id]