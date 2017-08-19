import zmq
import struct
import json
import Handler
from collections import deque
from origin import current_time, TIMESTAMP


class DataHandler(Handler.Handler):
    """A class for handling a single data stream."""

    def __init__(self, config, destination, stream_versions, logger=None):
        # stream info object
        self.stream_versions = stream_versions
        super(DataHandler, self).__init__(config, destination, logger=logger)

    def initialize_handler(self):
        # make a queue list
        self.queue = deque([])

    def decode_native_msg(self, msg):
        self.logger.debug('data msg recieved, for stream ' + self.stream)
        data = msg[4:]  # remove first 4 bytes
        fmtstr = self.stream_versions["format_str"]
        try:
            dtuple = struct.unpack_from(fmtstr, data)
        except:
            resp = (
                "Error unpacking stream data. Stream: `{}` format_str: `{}`"
                " measurement bytes: `{}`"
            ).format(self.stream_versions['stream'], fmtstr, len(data))
            return (1, resp, {})
        measurements = list(dtuple[1:])
        meas_dict = {}
        for key in self.stream_versions['definition']:
            idx = self.stream_versions['definition'][key]['key_index']
            meas_dict[key] = measurements[idx]

        # queue up the dictionary
        self.queue_measurement(meas_dict)

    def decode_json_msg(self, msg):
        self.decode_json_msg(msg)
        messageDecoded = json.loads(msg)
        stream = messageDecoded[0]
        stream_id = self.dest.known_streams[stream]["id"]
        if len(messageDecoded) != 3:
            result = 1
            result_text = (
                "Measurement message didn't have all the required "
                "fields."
            )
        elif ((type(messageDecoded[1]) != int) and
                (type(messageDecoded[1]) != long)):
            result = 1
            esult_text = "Non-integer time sent"
            return (result, result_text, {})
        else:
            recordTime = messageDecoded[1]
            measurements = messageDecoded[2]
            return self.dest.measurement(
                stream,
                measurements
            )

    def decode_msg(self, msg, decoder):
        """Decodes a data stream message string from a client.

        @param msg A data stream registration string from a client
        @param decoder A pointer to a function for handling data data format 
            'native' or 'json'
        @return (result, result_text) result is a error code with 0 being no
            error.  result_text is the response to be logged.
        """
        try:
            result, result_text, meas = decoder(msg)
        except:
            result_text = "Failed to decode message from client"
            self.logger.exception("Exception in server code.")
            return (result, result_text.format(format))
        else:
            self.publish(streamID, meas)

    def process_data_stream(self, msg):
        """Register a new natively formatted data message.

        @param msg A natively formatted data message
        """
        for m in msg:
            result, result_text = self.decode_msg(m, self.decode_native_msg)

    def process_json_data_stream(self, msg):
        """Register a new json formatted data message.

        @param msg A JSON formatted data message
        """
        for m in msg:
            result, result_text = self.decode_msg(m, self.decode_json_msg)

    def setup_sockets(self):
        """Setup the data receiver sockets for JSON and native formats, plus
        the default sockets inherited from the parent class.
        """
        # setup the default sockets from the parent class
        super(DataHandler, self).setup_sockets()
        # setup the native registration socket
        self.setup_socket(
            'data_native',
            callback=self.process_data_stream,
            settings={
                'port': self.config.getint("Server", "measure_port"),
                'type': zmq.PULL
            }
        )
        # setup the json registration socket
        self.setup_socket(
            'data_json',
            callback=self.process_json_data_stream,
            settings={
                'port': self.config.getint("Server", "json_measure_port"),
                'type': zmq.PULL
            }
        )

    def queue_measurement(self, meas):
        if meas[TIMESTAMP] == 0:
            meas_dict[TIMESTAMP] = current_time(self.config)
        self.queue.append(meas)
