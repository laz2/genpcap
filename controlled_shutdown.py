
from kafka.protocol.struct import Struct
from kafka.protocol.types import Int32, Schema, String, Int16, Array

from util import send, send_with_response

API_KEY = 7


class ControlledShutdownRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('broker_id', Int32)
    )


class ControlledShutdownResponse_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('partitions_remaining', Array(
            ('topic', String('utf-8')),
            ('partition', Int32),
        ))
    )


send_with_response(
    ControlledShutdownRequest_v0(
        broker_id=1000,
    ),
    ControlledShutdownResponse_v0(
        error_code=0,
        partitions_remaining=[
            ('topic1', 0),
            ('topic2', 1)
        ]
    )
)
