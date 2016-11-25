
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Int32, Schema, String, Int16

from util import send

API_KEY = 4


class LeaderAndIsrRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('controller_id', Int32),
        ('controller_epoch', Int32),
        ('partition_states', Array(
            ('topic', String('utf-8')),
            ('partition', Int32),
            ('controller_epoch', Int32),
            ('leader', Int32),
            ('leader_epoch', Int32),
            ('isrs', Array(Int32)),
            ('zk_version', Int32),
            ('replicas', Array(Int32)),
        )),
        ('live_leaders', Array(
            ('id', Int32),
            ('host', String('utf-8')),
            ('port', Int32),
        ))
    )

send(
    LeaderAndIsrRequest_v0(
        controller_id=0,
        controller_epoch=53,
        partition_states=[
            (
                # topic
                'topic1',
                # partition
                i,
                # controller_epoch
                27,
                # leader
                0,
                # leader_epoch
                16,
                # isrs
                [j for j in range(10)],
                # zk_version
                16,
                # replicas
                [j for j in range(10)],
            )
            for i in range(20)
        ],
        live_leaders=[
            (
                # id
                i,
                # host
                'leader' + str(i),
                # port
                9092 + i
            )
            for i in range(10)
        ]
    )
)
