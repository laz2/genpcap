
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Schema, String, Int32
from util import send


API_KEY = 12


class HeartbeatRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('group_generation_id', Int32),
        ('member_id', String('utf-8')),
    )

send(
    HeartbeatRequest_v0(
        group_id='group1',
        group_generation_id=1,
        member_id='member_id',
    )
)
