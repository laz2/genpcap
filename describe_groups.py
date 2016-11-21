
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Schema, String, Int32
from util import send


API_KEY = 15


class DescribeGroupsRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('group_ids', Array(String('utf-8')))
    )


send(
    DescribeGroupsRequest_v0(
        group_ids=['group1']
    )
)
