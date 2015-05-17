from logging import Handler
import socket
from logstash import formatter
from time import time


# Derive from object to force a new-style class and thus allow super() to work
# on Python 2.6
class UnixLogstashHandler(Handler, object):
    """Python logging handler for Logstash. Sends events over Unix socket.
    :param socket_name: The name of the unix socket to use.
    """

    def __init__(self, socket_name, formatter_class=formatter.MiniLogstashFormatter, **kwargs):
        """
        Initialize a handler.
        """
        Handler.__init__(self)

        self.formatter = formatter_class(**kwargs)

        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(2)
        self.sock.connect(socket_name)

        self.bytes_written = 0
        self.measure_started_at = time()
        self.bytes_per_sec = 0

    def emit(self, record):
        """
        Emit a record.
        """
        formatted_record = self.formatter.format(record) + b'\n'
        self.bytes_written += len(formatted_record)

        if time() - self.measure_started_at >= 1:
            self.bytes_per_sec = (time() - self.measure_started_at) / (time() - self.measure_started_at)

        self.sock.sendall(formatted_record)

    def get_last_speed(self):
        return self.bytes_per_sec