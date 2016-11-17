
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Schema, String
from util import send


API_KEY = 3


class MetadataRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('topics', Array(String('utf-8')))
    )


class MetadataRequest_v1(Struct):
    API_KEY = API_KEY
    API_VERSION = 1
    SCHEMA = MetadataRequest_v0.SCHEMA


class MetadataRequest_v2(Struct):
    API_KEY = API_KEY
    API_VERSION = 2
    SCHEMA = MetadataRequest_v1.SCHEMA


send(MetadataRequest_v0(topics=[]))
send(MetadataRequest_v0(topics=['topic1', 'topic2']))
send(MetadataRequest_v0(topics=['topic1', 'unknown_topic']))

send(MetadataRequest_v1(topics=[]))
send(MetadataRequest_v1(topics=['topic1', 'topic2']))

send(MetadataRequest_v2(topics=[]))
send(MetadataRequest_v2(topics=['topic1', 'topic2']))
