
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Int32, Int64, Schema, String

from util import send

API_KEY = 8


class OffsetCommitRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('metadata', String('utf-8')),
            ))))
    )


class OffsetCommitRequest_v1(Struct):
    API_KEY = API_KEY
    API_VERSION = 1
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('group_generation_id', Int32),
        ('member_id', String('utf-8')),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('timestamp', Int64),
                ('metadata', String('utf-8')),
            ))))
    )


class OffsetCommitRequest_v2(Struct):
    API_KEY = API_KEY
    API_VERSION = 2
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('group_generation_id', Int32),
        ('member_id', String('utf-8')),
        ('retention_time', Int64),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('metadata', String('utf-8')),
            ))))
    )


send(
    OffsetCommitRequest_v0(
        group_id='group1',
        topics=[
            ('unknown_topic1', [
                (0, 10, 'metadata'),
                (1, 10, 'metadata'),
            ]),
            ('unknown_topic1', [
                (0, 10, 'metadata'),
                (1, 10, 'metadata')
            ]),
        ]
    )
)


send(
    OffsetCommitRequest_v1(
        group_id='group1',
        group_generation_id=10,
        member_id='member',
        topics=[
            ('unknown_topic1', [
                (0, 10, 0, 'metadata'),
                (1, 10, 1000, 'metadata'),
            ]),
            ('unknown_topic1', [
                (0, 10, 0, 'metadata'),
                (1, 10, 1000, 'metadata')
            ]),
        ]
    )
)


send(
    OffsetCommitRequest_v2(
        group_id='group1',
        group_generation_id=10,
        member_id='member',
        retention_time=1000,
        topics=[
            ('unknown_topic1', [
                (0, 10, 'metadata'),
                (1, 10, 'metadata'),
            ]),
            ('unknown_topic1', [
                (0, 10, 'metadata'),
                (1, 10, 'metadata')
            ]),
        ]
    )
)
