
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Int32, Int64, Schema, String

from util import send

API_KEY = 1


class FetchRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('max_wait_time', Int32),
        ('min_bytes', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('max_bytes', Int32)))))
    )


class FetchRequest_v1(Struct):
    API_KEY = API_KEY
    API_VERSION = 1
    SCHEMA = FetchRequest_v0.SCHEMA


class FetchRequest_v2(Struct):
    API_KEY = API_KEY
    API_VERSION = 2
    SCHEMA = FetchRequest_v1.SCHEMA


class FetchRequest_v3(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('max_wait_time', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('max_bytes', Int32)))))
    )


send(
    FetchRequest_v0(
        replica_id=-1,
        max_wait_time=1000,
        min_bytes=0,
        topics=[('topic1', [(0, 0, 10000)])]
    )
)

send(
    FetchRequest_v1(
        replica_id=-1,
        max_wait_time=1000,
        min_bytes=0,
        topics=[
            ('topic1', [(0, 0, 10000)])
        ]
    )
)

send(
    FetchRequest_v2(
        replica_id=-1,
        max_wait_time=1000,
        min_bytes=0,
        topics=[
            ('topic1', [(0, 0, 10000)])
        ]
    )
)

send(
    FetchRequest_v3(
        replica_id=-1,
        max_wait_time=1000,
        min_bytes=0,
        max_bytes=10000,
        topics=[
            ('topic1', [(0, 0, 10000)])
        ]
    )
)
