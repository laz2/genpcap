
import socket
import threading

from io import BytesIO

from kafka.protocol.api import RequestHeader
from kafka.protocol.types import Int32


PORT = 9092
correlation_id = 0


def send(request):
    global correlation_id

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', PORT))

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


def send_with_response(request, response):
    event = threading.Event()

    def server_thread():
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.bind(('127.0.0.1', PORT))
        ss.listen(1)
        event.set()
        (cs, _) = ss.accept()

        size = Int32.decode(BytesIO(cs.recv(4, socket.MSG_WAITALL)))
        id = Int32.decode(BytesIO(cs.recv(4, socket.MSG_WAITALL)))
        cs.recv(size - 4, socket.MSG_WAITALL)

        message = b''.join([Int32.encode(id), response.encode()])
        cs.send(Int32.encode(len(message)) + message)

        cs.close()
        ss.close()

    threading.Thread(target=server_thread).start()
    event.wait()
    send(request)
