
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Schema, Int16
from controlled_shutdown import ControlledShutdownRequest_v1, ControlledShutdownResponse_v1
from api_versions import ApiVersionsRequest_v0, ApiVersionsResponse_v0
from metadata import MetadataRequest_v0
from util import send_with_response


# unknown api key
class UnknownRequest_v0(Struct):
    API_KEY = 10000
    API_VERSION = 0
    SCHEMA = Schema()


class UnknownResponse_v0(Struct):
    API_KEY = 10000
    API_VERSION = 0
    SCHEMA = Schema()


send_with_response(
    UnknownRequest_v0(),
    UnknownResponse_v0()
)


# lesser version
class ControlledShutdownRequest_v0(ControlledShutdownRequest_v1):
    API_VERSION = 0


class ControlledShutdownResponse_v0(ControlledShutdownResponse_v1):
    API_VERSION = 0


send_with_response(
    ControlledShutdownRequest_v0(broker_id=1000),
    ControlledShutdownResponse_v1(error_code=0, partitions_remaining=[])
)


# greater version
class ControlledShutdownRequest_v1000(ControlledShutdownRequest_v0):
    API_VERSION = 1000


class ControlledShutdownResponse_v1000(ControlledShutdownResponse_v0):
    API_VERSION = 1000


send_with_response(
    ControlledShutdownRequest_v1000(broker_id=1000),
    ControlledShutdownResponse_v1(error_code=0, partitions_remaining=[])
)


# api versions

send_with_response(
    ApiVersionsRequest_v0(),
    ApiVersionsResponse_v0(
        error_code=0,
        api_versions=[
            # unknown api key
            (1000, 0, 0),
            # lesser version
            (ControlledShutdownRequest_v1.API_KEY, 0, 1),
            # greater version
            (ApiVersionsRequest_v0.API_KEY, 0, 1000),
            # two versions
            (MetadataRequest_v0.API_KEY, 0, 1000),
        ]
    )
)
