
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Int32, Schema, String

from util import send

API_KEY = 9


class OffsetFetchRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(Int32))))
    )


class OffsetFetchRequest_v1(Struct):
    API_KEY = API_KEY
    API_VERSION = 1
    SCHEMA = OffsetFetchRequest_v0.SCHEMA


send(
    OffsetFetchRequest_v0(
        group_id='group1',
        topics=[
            ('topic1', [0])
        ]
    )
)

send(
    OffsetFetchRequest_v0(
        group_id='group1',
        topics=[
            ('topic1', [0])
        ]
    )
)
