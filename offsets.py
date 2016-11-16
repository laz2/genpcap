
import socket

from io import BytesIO

from kafka.protocol.api import RequestHeader
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Int16, Int32, Int64, Schema, String


API_KEY = 2
correlation_id = 0


class OffsetRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('time', Int64),
                ('max_offsets', Int32))))
        )
    )
    DEFAULTS = {
        'replica_id': -1
    }


class OffsetRequest_v1(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('topics', Array(
             ('topic', String('utf-8')),
             ('partitions', Array(
                  ('partition', Int32),
                  ('time', Int64))))
        )
    )
    DEFAULTS = {
        'replica_id': -1
    }


def send(s, request):
    global correlation_id

    header = RequestHeader(
        request=request,
        correlation_id=correlation_id,
        client_id='test'
    )
    correlation_id += 1

    message = b''.join([header.encode(), request.encode()])
    size = Int32.encode(len(message))
    s.send(size)
    s.send(message)

    size = s.recv(4, socket.MSG_WAITALL)
    size = Int32.decode(BytesIO(size))
    s.recv(size, socket.MSG_WAITALL)


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 9092))

send(s, OffsetRequest_v0(
    replica_id=-1,
    topics=[('topic1', [(0, 0, 100)])]
))

s.close()
