
from kafka.protocol.types import Array, Schema, Int16
from util import send_with_response
from delete_topics import DeleteTopicsRequest_v0


# many strings
send_with_response(
    DeleteTopicsRequest_v0(
        topics=['unknown_topic{}'.format(i) for i in range(100)],
        timeout=100
    )
)

# ascii, utf-8, null
send_with_response(
    DeleteTopicsRequest_v0(
        topics=['unknown_topic1', None, 'неизвестный_топик'],
        timeout=100
    )
)


class BadLengthRequest_v0(DeleteTopicsRequest_v0):
    SCHEMA = Schema(
        ('topics', Array(Int16)),
    )


# bad length
send_with_response(
    BadLengthRequest_v0(
        topics=[-2],
    )
)
