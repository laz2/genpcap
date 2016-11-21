
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Schema, String, Int32
from util import send


API_KEY = 13


class LeaveGroupRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('member_id', String('utf-8')),
    )

send(
    LeaveGroupRequest_v0(
        group_id='group1',
        member_id='member_id',
    )
)
