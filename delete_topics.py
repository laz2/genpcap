
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Schema, String, Int32
from util import send


API_KEY = 20

class DeleteTopicsRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('topics', Array(String('utf-8'))),
        ('timeout', Int32)
    )


if __name__ == '__main__':
    send(
        DeleteTopicsRequest_v0(
            topics=['unknown_topic1', 'unknown_topic2'],
            timeout=100
        )
    )
