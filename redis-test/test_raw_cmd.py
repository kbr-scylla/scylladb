#
# Copyright (C) 2019 ScyllaDB
#
#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
#

import socket
import logging

logger = logging.getLogger('redis-test')


class RedisSocket:

    def __init__(self, host='localhost', port=6379):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(host, port)

    def connect(self, host, port):
        self.socket.connect((host, port))

    def send(self, content=''):
        self.socket.send(content)

    def recv(self, buf_len=1024):
        return self.socket.recv(buf_len)

    def shutdown(self):
        self.socket.shutdown(socket.SHUT_WR)

    def close(self):
        self.socket.close()

def verify_cmd_response(cmd, expect_ret, shutdown=False):
    rs = RedisSocket()
    rs.send(cmd.encode())
    if shutdown:
        rs.shutdown()
    ret = rs.recv().decode()
    logger.debug('Received content size: %s' % len(ret))
    logger.debug(ret)
    assert ret == expect_ret
    rs.close()

def test_ping():
    verify_cmd_response('*1\r\n$4\r\nping\r\n', '+PONG\r\n')

def test_eof():
    # shutdown socket, and read nothing
    verify_cmd_response("", "", shutdown=True)

    # a EOF char `\x04` should be triggered parse error
    verify_cmd_response("\x04", "-ERR unknown command ''\r\n", shutdown=True)

def test_ping_and_eof():
    # regular ping with shutdown
    verify_cmd_response('*1\r\n$4\r\nping\r\n', '+PONG\r\n', shutdown=True)

    # a EOF char `\x04` should be triggered parse error
    verify_cmd_response("*1\r\n$4\r\nping\r\n\x04", "+PONG\r\n-ERR unknown command ''\r\n", shutdown=True)
