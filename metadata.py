
import socket

from io import BytesIO

from kafka.protocol.api import RequestHeader
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Int16, Int32, Schema, String


METADATA_API_KEY = 3
correlation_id = 0


class MetadataRequest_v0(Struct):
    API_KEY = METADATA_API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('topics', Array(String('utf-8')))
    )


class MetadataRequest_v1(Struct):
    API_KEY = METADATA_API_KEY
    API_VERSION = 1
    SCHEMA = Schema(
        ('topics', Array(String('utf-8')))
    )

class MetadataRequest_v2(Struct):
    API_KEY = METADATA_API_KEY
    API_VERSION = 2
    SCHEMA = Schema(
        ('topics', Array(String('utf-8')))
    )


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

send(s, MetadataRequest_v0(topics=[]))
send(s, MetadataRequest_v0(topics=['topic1', 'topic2']))
send(s, MetadataRequest_v0(topics=['topic1', 'unknown_topic']))

send(s, MetadataRequest_v1(topics=[]))
send(s, MetadataRequest_v1(topics=['topic1', 'topic2']))

send(s, MetadataRequest_v2(topics=[]))
send(s, MetadataRequest_v2(topics=['topic1', 'topic2']))

s.close()
