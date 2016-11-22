
from kafka.protocol.struct import Struct
from kafka.protocol.types import Int32, Schema

from util import send

API_KEY = 7


class ControlledShutdownRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('broker_id', Int32)
    )

send(
    ControlledShutdownRequest_v0(
        broker_id=1000,
    )
)
