
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Int32, Int64, Schema, String, Boolean

from util import send

API_KEY = 5


class StopReplicaRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('controller_id', Int32),
        ('controller_epoch', Int32),
        ('delete_partitions', Boolean),
        ('partitions', Array(
            ('topic', String('utf-8')),
            ('partition', Int32)))
    )


send(
    StopReplicaRequest_v0(
        controller_id=1000,
        controller_epoch=1000,
        delete_partitions=True,
        partitions=[
            ('unknonw_topic1', 0),
            ('unknonw_topic2', 0),
        ]
    )
)
