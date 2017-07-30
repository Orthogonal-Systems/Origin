import json
import zmq


def json_key_order(msg, record_dict):
    """Extract key_order from raw json message

    @param msg raw msg from client with repeatable key_order
    @return key_order A list containing a list of keys in the received order
    """
    key_order = []
    raw_key_list = msg.split('{')[1]
    raw_key_list = raw_key_list.split('}')[0]
    raw_key_list = raw_key_list.split(',')
    for idx, rawKey in enumerate(raw_key_list):
        for key in record_dict:
            if rawKey.find('"{}"'.format(key)) != -1:
                key_order.append(key.strip())
                break
    return key_order


class Registration(object):
    """A class for handling stream registration."""

    def __init__(self, config, destination, logger=None):
        """Initialize the class

        @param logger python logger object
        """
        self.logger = logger
        self.dest = destination

        self.context = zmq.Context()
        # instantiate native registration socket
        reg_addr = "tcp://*:{}"
        reg_port = self.config.getint("Server", "register_port")
        self.reg_socket = self.context.socket(zmq.REP)
        self.reg_socket.bind(reg_addr.format(reg_port))

        # instantiate json registration socket
        json_reg_addr = "tcp://*:{}"
        json_reg_port = self.config.getint("Server", "json_register_port")
        self.json_reg_socket = self.context.socket(zmq.REP)
        self.json_reg_socket.bind(json_reg_addr.format(json_reg_port))

        # instantiate the poller
        self.poller = zmq.Poller()
        self.poller.register(self.reg_socket, zmq.POLLIN)
        self.poller.register(self.json_reg_socket, zmq.POLLIN)

        # start the loop
        self.loop()

    def decode_json_msg(self, msg):
        msg_decoded = json.loads(msg)
        reg_obj = {}
        reg_obj['stream'] = msg_decoded[0]
        reg_obj['template'] = msg_decoded[1]
        reg_obj['key_order'] = json_key_order(msg, reg_obj['template'])
        return reg_obj

    def decode_msg(self, msg, format):
        result = None
        result_text = None
        try:
            if format == "native":
                reg_obj = self.decode_native_msg(msg)
            elif format == "json":
                reg_obj = self.decode_json_msg(msg)
            else:
                result = 1
                log = "Unrecognized registration format `{}` specified"
                result_text = log.format(format)
        except:
            result = 1
            result_text = "Failed to decode message from client"
        else:
            log = "Received registration of stream {}"
            self.logger.info(log.format(reg_obj['stream']))
            result, result_text = self.dest.register_stream(
                reg_obj['stream'],
                reg_obj['template'],
                reg_obj['key_order']
            )

        # notify if error
        if result != 0:
            log = (
                "Unable to register stream. Got msg that is badly "
                "formatted: {}"
            ).format(msg)
            self.logger.warn(log)

        return (result, result_text)

    def decode_native_msg(self, msg):
        reg_obj = {}
        msg_decoded = msg.split(',')
        record_dict = {}
        key_order = []
        for entry in msg_decoded[1:]:
            key, dtype = entry.split(':')
            key = key.strip()
            record_dict[key] = dtype.strip()
            key_order.append(key)
        reg_obj['stream'] = msg_decoded[0]
        reg_obj['template'] = record_dict[1]
        reg_obj['key_order'] = json_key_order(msg, record_dict)
        return reg_obj

    def loop(self):
        should_continue = True
        while should_continue:
            socks = dict(self.poller.poll())
            # check native registration socket
            if (self.reg_socket in socks and
                    socks[self.reg_socket] == zmq.POLLIN):
                msg = self.reg_socket.recv()
                self.register_stream(msg)

            # check json registration socket
            if (self.json_reg_socket in socks and
                    socks[self.json_reg_socket] == zmq.POLLIN):
                msg = self.json_reg_socket.recv()
                self.register_json_stream(msg)

    def register_json_stream(self, msg):
        for m in msg:
            result, result_text = self.decode_msg(m, 'json')
            response = (str(result), result_text)
            self.reg_stream.send(','.join(response))

    def register_stream(self, msg):
        for m in msg:
            result, result_text = self.decode_msg(m, 'native')
            response = (str(result), result_text)
            self.reg_stream.send(','.join(response))
