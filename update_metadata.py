
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Int32, Schema, String, Int16

from util import send

API_KEY = 6


class UpdateMetadataRequest_v0(Struct):
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


class UpdateMetadataRequest_v1(Struct):
    API_KEY = API_KEY
    API_VERSION = 1
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
            ('end_points', Array(
                ('port', Int32),
                ('host', String('utf-8')),
                ('security_protocol_type', Int16),
            ))
        ))
    )


class UpdateMetadataRequest_v2(Struct):
    API_KEY = API_KEY
    API_VERSION = 2
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
            ('end_points', Array(
                ('port', Int32),
                ('host', String('utf-8')),
                ('security_protocol_type', Int16),
            )),
            ('rack', String('utf-8')),
        ))
    )


send(
    UpdateMetadataRequest_v0(
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


send(
    UpdateMetadataRequest_v1(
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
                # end_points
                [
                    (
                        # port
                        9092 + i * 10 + j,
                        # host
                        'leader' + str(i * 10 + j),
                        # security_protocol_type
                        j
                    )
                    for j in range(-1, 5)
                ]
            )
            for i in range(5)
        ]
    )
)


send(
    UpdateMetadataRequest_v2(
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
                # end_points
                [
                    (
                        # port
                        9092 + i * 10 + j,
                        # host
                        'leader' + str(i * 10 + j),
                        # security_protocol_type
                        j
                    )
                    for j in range(-1, 5)
                ],
                # rack
                'rack' + str(i)
            )
            for i in range(5)
        ]
    )
)
