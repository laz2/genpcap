
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Int16, Int32, Schema, String
from kafka.protocol.message import MessageSet

from util import send

API_KEY = 0


class ProduceRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('required_acks', Int16),
        ('timeout', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('messages', MessageSet)))))
    )


class ProduceRequest_v1(Struct):
    API_KEY = API_KEY
    API_VERSION = 1
    SCHEMA = ProduceRequest_v0.SCHEMA


class ProduceRequest_v2(Struct):
    API_KEY = API_KEY
    API_VERSION = 2
    SCHEMA = ProduceRequest_v1.SCHEMA


send(
    ProduceRequest_v0(
        required_acks=0,
        timeout=1000,
        topics=[
            (
                'topic1',
                [
                    (
                        0, MessageSet()
                    )
                ]
            )
        ]
    )
)


send(
    ProduceRequest_v1(
        required_acks=0,
        timeout=1000,
        topics=[
            (
                'topic1',
                [
                    (
                        0, MessageSet()
                    )
                ]
            )
        ]
    )
)


send(
    ProduceRequest_v2(
        required_acks=0,
        timeout=1000,
        topics=[
            (
                'topic1',
                [
                    (
                        0, MessageSet()
                    )
                ]
            )
        ]
    )
)
