
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Schema, String, Int32, Bytes
from util import send


API_KEY = 17

class SaslHandshakeRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('mechanism', String('utf-8')),
    )


send(SaslHandshakeRequest_v0(mechanism='unknown_mechanism'))
