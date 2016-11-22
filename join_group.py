
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Schema, String, Int32, Bytes
from util import send


API_KEY = 11


class JoinGroupRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('session_timeout', Int32),
        ('member_id', String('utf-8')),
        ('protocol_type', String('utf-8')),
        ('group_protocols', Array(
            ('protocol_name', String('utf-8')),
            ('protocol_metadata', Bytes)))
    )


class JoinGroupRequest_v1(Struct):
    API_KEY = API_KEY
    API_VERSION = 1
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('session_timeout', Int32),
        ('rebalance_timeout', Int32),
        ('member_id', String('utf-8')),
        ('protocol_type', String('utf-8')),
        ('group_protocols', Array(
            ('protocol_name', String('utf-8')),
            ('protocol_metadata', Bytes)))
    )


send(
    JoinGroupRequest_v0(
        group_id='group1',
        session_timeout=6000,
        member_id='',
        protocol_type='protocol',
        group_protocols=[
            ('protocol', b'metadata')
        ]
    )
)
send(
    JoinGroupRequest_v1(
        group_id='group1',
        session_timeout=6000,
        rebalance_timeout=1000,
        member_id='',
        protocol_type='protocol',
        group_protocols=[
            ('protocol', b'metadata')
        ]
    )
)
