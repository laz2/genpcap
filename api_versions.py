
from kafka.protocol.struct import Struct
from kafka.protocol.types import Schema, Int16, Array

from util import send

API_KEY = 18


class ApiVersionsRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema()


class ApiVersionsResponse_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('api_versions', Array(
            ('api_key', Int16),
            ('min_version', Int16),
            ('max_version', Int16))))


if __name__ == '__main__':
    send(ApiVersionsRequest_v0())
