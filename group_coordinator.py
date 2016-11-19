
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Schema, String
from util import send


API_KEY = 10


class GroupCoordinatorRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('group_id', String('utf-8'))
    )

send(GroupCoordinatorRequest_v0(group_id='group1'))
send(GroupCoordinatorRequest_v0(group_id='unknown_group'))
