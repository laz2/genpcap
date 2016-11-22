
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Schema, String, Int32, Bytes
from util import send


API_KEY = 14


class SyncGroupRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('generation_id', Int32),
        ('member_id', String('utf-8')),
        ('group_assignments', Array(
            ('member_id', String('utf-8')),
            ('member_assignment', Bytes)))
    )


send(
    SyncGroupRequest_v0(
        group_id='group1',
        generation_id=10,
        member_id='member1',
        group_assignments=[
            ('member1', b'metadata1'),
            ('member2', b'metadata2'),
        ]
    )
)
