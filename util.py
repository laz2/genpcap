
import socket
import threading

from io import BytesIO

from kafka.protocol.api import RequestHeader
from kafka.protocol.types import Int32


PORT = 9092
correlation_id = 0


def send(request, wait_response=True):
    global correlation_id

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with s:
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

        if wait_response:
            size = s.recv(4, socket.MSG_WAITALL)
            size = Int32.decode(BytesIO(size))
            s.recv(size, socket.MSG_WAITALL)


def send_with_response(request, response=None):
    event = threading.Event()
    need_response = response is not None

    def server_thread():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
            ss.bind(('127.0.0.1', PORT))
            ss.listen(1)
            event.set()

            (cs, _) = ss.accept()
            with cs:
                size = Int32.decode(BytesIO(cs.recv(4, socket.MSG_WAITALL)))
                id = Int32.decode(BytesIO(cs.recv(4, socket.MSG_WAITALL)))
                cs.recv(size - 4, socket.MSG_WAITALL)

                if need_response:
                    message = b''.join([Int32.encode(id), response.encode()])
                    cs.send(Int32.encode(len(message)) + message)

    threading.Thread(target=server_thread).start()
    event.wait()
    send(request, wait_response=need_response)
