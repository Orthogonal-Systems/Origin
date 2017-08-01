import json
import zmq
import Handler


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


class Registration(Handler.Handler):
    """A class for handling stream registration."""

    def decode_json_msg(self, msg):
        """Decodes a JSON formated registration message.

        @param msg A JSON formated registration string from a client
        @return reg_obj A dictionary containing the registration information
        """
        msg_decoded = json.loads(msg)
        reg_obj = {}
        reg_obj['stream'] = msg_decoded[0]
        reg_obj['template'] = msg_decoded[1]
        reg_obj['key_order'] = json_key_order(msg, reg_obj['template'])
        return reg_obj

    def decode_msg(self, msg, format):
        """Decodes a data stream registration string from a client.

        @param msg A data stream registration string from a client
        @param format A string specifying the format type 'native' or 'json'
        @return (result, result_text) result is a error code with 0 being no
            error.  result_text is the response to be sent back to the sender.
        """
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
        """Decodes a natively formated registration message.

        @param msg A natively formated registration string from a client
        @return reg_obj A dictionary containing the registration information
        """
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

    def register_json_stream(self, msg):
        """Register a new json formatted data stream.

        @param msg A JSON formatted registration stream
        """
        for m in msg:
            result, result_text = self.decode_msg(m, 'json')
            response = (str(result), result_text)
            self.reg_stream.send(','.join(response))

    def register_stream(self, msg):
        """Register a new natively formatted data stream.

        @param msg A natively formatted registration stream
        """
        for m in msg:
            result, result_text = self.decode_msg(m, 'native')
            response = (str(result), result_text)
            self.reg_stream.send(','.join(response))

    def setup_sockets(self):
        """Setup the registration sockets for JSON and native formats, plus the
        default sockets inherited from the parent class.
        """
        # setup the default sockets from the parent class
        super(Registration, self).setup_sockets()
        # setup the native registration socket
        self.setup_socket(
            'reg_native',
            callback=register_stream
            settings={
                'port': self.config.getint("Server", "register_port")
            }
        )
        # setup the json registration socket
        self.setup_socket(
            'reg_json',
            callback=register_json_stream
            settings={
                'port': self.config.getint("Server", "json_register_port")
            }
        )
