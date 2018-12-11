# -*- coding: utf-8 -*-
# Copyright 2018 Ciena Corporation
"""
Endpoints for testing
"""
from __future__ import absolute_import, division

from fnmatch import fnmatchcase
from functools import partial
from pprint import pformat
from struct import Struct

import attr
from twisted.internet import defer
from twisted.internet.interfaces import IAddress, IStreamClientEndpoint
from twisted.internet.protocol import connectionDone
from twisted.logger import Logger
from twisted.protocols.basic import Int32StringReceiver
from twisted.test import iosim
from zope.interface import implementer


@implementer(IStreamClientEndpoint)
@attr.s(frozen=True)
class FailureEndpoint(object):
    """
    Immediately fail every connection attempt.
    """
    reactor = attr.ib()
    host = attr.ib()
    port = attr.ib()

    def connect(self, protocolFactory):
        e = IndentationError('failing to connect {}'.format(protocolFactory))
        return defer.fail(e)


@implementer(IStreamClientEndpoint)
@attr.s(frozen=True)
class BlackholeEndpoint(object):
    """
    Black-hole every connection attempt by returning a deferred which never
    fires.
    """
    reactor = attr.ib()
    host = attr.ib()
    port = attr.ib()

    def connect(self, protocolFactory):
        return defer.Deferred()


@implementer(IAddress)
@attr.s(frozen=True)
class _DebugAddress(object):
    host = attr.ib()
    port = attr.ib()


@attr.s(frozen=True, cmp=False)
class Connections(object):
    """Externally controllable endpoint factory

    Each time `Connections` is called to generate an endpoint it returns one
    which records each connection attempt and returns an unresolved deferred.
    """
    calls = attr.ib(init=False, default=attr.Factory(list))
    connects = attr.ib(init=False, default=attr.Factory(list))

    def __call__(self, reactor, host, port):
        """

        """
        self.calls.append((host, port))
        return _PuppetEndpoint(partial(self._connect, host, port))

    def _connect(self, host, port, protocolFactory):
        d = defer.Deferred()
        self.connects.append((host, port, protocolFactory, d))
        return d

    def _match(self, host_pattern):
        for index in range(len(self.connects)):
            host = self.connects[index][0]
            if fnmatchcase(host, host_pattern):
                break
        else:
            raise AssertionError('No match for host pattern {!r}. Searched:\n{}'.format(
                host_pattern, pformat(self.connects)))
        return self.connects.pop(index)

    def accept(self, host_pattern):
        """
        Accept a pending connection request.

        :param str host_pattern:
            :func:`fnmatch.fnmatchcase` pattern against which the connection
            request must match.

        :returns:
            Two-tuple of (protocol, transport), where the transport is
            a `twisted.test.iosim.FakeTransport` in client mode.
        """
        host, port, protocolFactory, d = self._match(host_pattern)
        peerAddress = _DebugAddress(host, port)
        client_protocol = protocolFactory.buildProtocol(peerAddress)
        assert client_protocol is not None
        server_protocol = KafkaBrokerProtocol()
        client_transport = iosim.FakeTransport(client_protocol, isServer=False, peerAddress=peerAddress)
        server_transport = iosim.FakeTransport(server_protocol, isServer=True)
        pump = iosim.connect(server_protocol, server_transport,
                             client_protocol, client_transport)
        d.callback(client_protocol)
        return KafkaConnection(
            server=server_protocol,
            client=client_protocol,
            pump=pump,
        )


@implementer(IStreamClientEndpoint)
@attr.s(frozen=True)
class _PuppetEndpoint(object):
    """
    Implementation detail of `Connections`.
    """
    connect = attr.ib()


@attr.s(frozen=True, cmp=False)
class KafkaConnection(object):
    """
    """
    client = attr.ib()
    server = attr.ib()
    pump = attr.ib()


@attr.s(frozen=True)
class Request(object):
    _protocol = attr.ib(repr=False)
    api_key = attr.ib()
    api_version = attr.ib()
    correlation_id = attr.ib()
    client_id = attr.ib()
    rest = attr.ib()

    _req_header = Struct('>hhih')
    _resp_header = Struct('>i')

    @classmethod
    def from_bytes(cls, protocol, request):
        """
        :param protocol: `KafkaBrokerProtocol` object
        :param bytes request: Kafka request, less length prefix
        """
        size = cls._req_header.size
        api_key, api_version, correlation_id, client_id_len = cls._req_header.unpack(request[:size])
        if client_id_len < 0:  # Nullable string.
            header_end = size
            client_id = None
        else:
            header_end = size + client_id_len
            client_id = request[size:header_end].decode('utf-8')
        return cls(
            protocol,
            api_key,
            api_version,
            correlation_id,
            client_id,
            request[header_end:],
        )

    def respond(self, response):
        r = self._resp_header.pack(self.correlation_id) + response
        self._protocol.sendString(r)


class KafkaBrokerProtocol(Int32StringReceiver):
    """The server side of the Kafka protocol

    Like a real Kafka broker it handles one request at a time.

    :ivar requests:
    """
    _log = Logger()
    MAX_LENGTH = 2 ** 16  # Small limit for testing.
    disconnected = None

    def connectionMade(self):
        self._log.debug("Connected")
        self.requests = defer.DeferredQueue(backlog=1)

    def stringReceived(self, request):
        """
        Handle a request from the broker.
        """
        r = Request.from_bytes(self, request)
        self._log.debug("Received {request}", request=r)
        self.requests.put(r)

    def connectionLost(self, reason=connectionDone):
        self.disconnected = reason
        self._log.debug("Server connection lost: {reason}", reason=reason)
