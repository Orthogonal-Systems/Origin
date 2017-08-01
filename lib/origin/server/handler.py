import json
import zmq


class Handler(object):
    """A class for handling common stream related functions."""

    def __init__(self, config, destination, logger=None):
        """Initialize the class

        @param config configuration object
        @param destination a origin destination object
        @param logger python logger object
        """

        self.config = config
        self.dest = destination
        # set up the logger
        if logger is None:
            self.setup_logger()
        else:
            self.logger = logger
        # set up the sockets
        self.setup_sockets()
        # start the loop
        self.loop()

    def loop(self):
        """A poller loop for the registration sockets"""
        should_continue = True
        while should_continue:
            socks = dict(self.poller.poll())
            for sock in self.polled_sockets:
                # check registered socket for new command
                if (self.sockets[sock] in socks and
                        socks[self.sockets[sock] == zmq.POLLIN):
                    # read in the command
                    msg = self.sockets[sock].recv()
                    # pass message to the registered callback
                    self.callbacks[sock](msg)

    def manager_command(self, msg):
        """Parse and act on commands from a manager application."""
        raise NotImplementedError

    def setup_logger(self):
        pass

    def setup_socket(self, name, callback=None, settings={}, poll=True):
        """Setup a single socket and add it to the list of sockets.

        @param name A string containing a unique socket identifier
        @param callback A pointer to a callback function expecting a raw
            message from a client
        @param settings A settings dictionary that overwrites the defaults. The
            expected values with defaults are:
            `transport`: `tcp`
            `address`: `*`
            `port`: defaults to manager port from the configuration file
            `type`: zmq.REP (must be a zmq object)
        @param poll Boolean for adding the socket to the poller. Default: True
        """
        default_settings = {
            'transport' : 'tcp',
            'address'   : '*',
            'port'      : self.config.getint("Server", "manager_port_in")
            'type'      : zmq.REP
        }
        for s in settings:
            default_settings[s] = settings[s]

        addr = "{}://{}:{}".format(
            default_settings['transport'],
            default_settings['address'],
            default_settings['port']
        )
        self.sockets[name] = self.context.socket(default_settings['type'])
        self.logger.info("Binding {} socket to `{}`".format(name, addr))
        self.sockets[name].bind(addr)
        # poller loop expects a callback
        if poll:
            self.poller.register(self.sockets[name], zmq.POLLIN)
            self.callbacks[name] = callback
            self.polled_sockets.append(name)

    def setup_sockets(self):
        """Basic socket setup.

        Children classes should super this then add the necessary sockets. The
        manager socket is common to all handlers.
        """
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.sockets = {}
        self.callbacks = {}
        self.polled_sockets = []
        # instantiate a new incomming manager connection
        self.setup_socket('manager_in', self.manager_command)
        # instantiate a new outgoing manager connection
        self.setup_socket(
            'manager_out',
            poll=False,
            settings={
                'address'   : self.config.get("Server", "ip"),
                'port'      : self.config.getint("Server", "manager_port_out")
                'type'      : zmq.PUSH
            }
        )
