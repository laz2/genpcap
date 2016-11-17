
from kafka.protocol.struct import Struct
from kafka.protocol.types import Schema

from util import send

API_KEY = 18


class ApiVersionsRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema()


send(ApiVersionsRequest_v0())
