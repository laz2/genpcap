
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Int32, Schema, String, Int16

from util import send

API_KEY = 19


class CreateTopicsRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('create_topic_requests', Array(
            ('topic', String('utf-8')),
            ('num_partitions', Int32),
            ('replication_factor', Int16),
            ('replica_assigment', Array(
                ('partition_id', Int32),
                ('replicas', Array(Int32))
            )),
            ('configs', Array(
                ('config_key', String('utf-8')),
                ('config_value', String('utf-8'))
            )))),
        ('timeout', Int32)
    )

send(
    CreateTopicsRequest_v0(
        create_topic_requests=[
            (
                'new_topic', 10, 3,
                [
                    (0, [0, 1, 2]),
                    (1, [0, 1, 2]),
                ],
                [
                    ('key1', 'value1'),
                    ('key2', 'value2'),
                ]
            )
        ],
        timeout=1000
    )
)
