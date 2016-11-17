
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Int32, Int64, Schema, String

from util import send

API_KEY = 2


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
                ('max_offsets', Int32)))))
    )


class OffsetRequest_v1(Struct):
    API_KEY = API_KEY
    API_VERSION = 1
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('topics', Array(
             ('topic', String('utf-8')),
             ('partitions', Array(
                  ('partition', Int32),
                  ('time', Int64)))))
    )


send(
    OffsetRequest_v0(
        replica_id=-1,
        topics=[
            ('topic1', [
                (0, 0, 100),
            ]),
            ('topic2', [
                (0, 0, 100),
                (1, 0, 100),
            ])
        ]
    )
)

send(
    OffsetRequest_v0(
        replica_id=-1,
        topics=[
            ('topic1', [
                (1, 0, 100),
            ]),
            ('unknown_topic', [
                (0, 0, 100),
            ])
        ]
    )
)

send(
    OffsetRequest_v1(
        replica_id=-1,
        topics=[
            ('topic1', [
                (0, 0),
            ]),
            ('topic2', [
                (0, 0),
                (1, 0),
            ])
        ]
    )
)

send(
    OffsetRequest_v1(
        replica_id=-1,
        topics=[
            ('topic1', [
                (1, 0),
            ]),
            ('unknown_topic', [
                (0, 0),
            ])
        ]
    )
)
