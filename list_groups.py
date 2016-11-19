

from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Schema, String, Int32
from util import send


API_KEY = 16


class ListGroupsRequest_v0(Struct):
    API_KEY = API_KEY
    API_VERSION = 0
    SCHEMA = Schema()


send(ListGroupsRequest_v0())
