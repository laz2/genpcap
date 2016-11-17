
import socket

from io import BytesIO

from kafka.protocol.api import RequestHeader
from kafka.protocol.types import Int32


correlation_id = 0


def send(request):
    global correlation_id

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 9092))

    header = RequestHeader(
        request=request,
        correlation_id=correlation_id,
        client_id='test'
    )
    correlation_id += 1

    message = b''.join([header.encode(), request.encode()])
    size = Int32.encode(len(message))
    s.send(size)
    s.send(message)

    size = s.recv(4, socket.MSG_WAITALL)
    size = Int32.decode(BytesIO(size))
    s.recv(size, socket.MSG_WAITALL)

    s.close()
