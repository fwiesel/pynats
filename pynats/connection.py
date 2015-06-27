# -*- coding: utf-8 -*-
import socket
import sys
import json
import time
import random
import string
import traceback
from logging import getLogger
from io import TextIOWrapper
from pynats.commands import commands, MSG, INFO, PING, PONG, OK
from pynats.subscription import Subscription
from pynats.message import Message
import asyncio
try:
    import urllib.parse as urlparse
except:
    import urlparse


DEFAULT_URI = 'nats://localhost:4222'

LOG = getLogger(__name__)

class Connection(asyncio.Protocol):
    """
    A Connection represents a bare connection to a nats-server.
    """
    def __init__(
        self,
        url=DEFAULT_URI,
        name=None,
        ssl_required=False,
        verbose=False,
        pedantic=False,
        loop=None
    ):
        self._connect_timeout = None
        self._reader = None
        self._writer = None
        self._ping_mutex = None
        self._subscriptions = {}
        self._next_sid = 1
        self._options = {
            'url': urlparse.urlsplit(url),
            'name': name,
            'ssl_required': ssl_required,
            'verbose': verbose,
            'pedantic': pedantic
        }
        self._dispatch_table = self._build_dispatch_table()
        self._loop = asyncio.get_event_loop() if loop is None else loop
        self._info = None

    @asyncio.coroutine
    def connect(self):
        """
        Connect will attempt to connect to the NATS server. The url can
        contain username/password semantics.
        """
        self._reader = asyncio.StreamReader(loop=self._loop)
        protocol = asyncio.StreamReaderProtocol(self._reader, client_connected_cb=self._send_connect_msg ,loop=self._loop)
        transport, _ = yield from self._loop.create_connection(lambda: protocol, self._options['url'].hostname, self._options['url'].port, ssl=self._options['ssl_required'])
        self._writer = asyncio.StreamWriter(transport, protocol, self._reader, self._loop)
        self._receive_loop = asyncio.async(self.wait(), loop=self._loop)

    @asyncio.coroutine
    def _send_connect_msg(self, reader, writer):
        socket = writer.get_extra_info('socket')
        try:
            socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except AttributeError:
            pass

        writer.write(b''.join([b'CONNECT ', self._connect_config().encode('utf-8'), b'\r\n']))
        yield from writer.drain()

    def _connect_config(self):
        config = {
            'verbose': self._options['verbose'],
            'pedantic': self._options['pedantic'],
            'ssl_required': self._options['ssl_required'],
            'name': self._options['name'],
        }

        if self._options['url'].username is not None:
            config['user'] = self._options['url'].username
            config['pass'] = self._options['url'].password

        return json.dumps(config)

    @asyncio.coroutine
    def ping(self):
        if self._ping_mutex: # Wait for an already queued command
            yield from self._ping_mutex
        self._ping_mutex = asyncio.Future()

        self._writer.write(b'PING\r\n')
        yield from self._writer.drain()

        yield from self._ping_mutex # Assumption: The commands are answered in order

    def subscribe(self, subject, callback, queue=''):
        """
        Subscribe will express interest in the given subject. The subject can
        have wildcards (partial:*, full:>). Messages will be delivered to the
        associated callback.

        Args:
            subject (string): a string with the subject
            callback (function): callback to be called
        """

        s = Subscription(
            sid=self._next_sid,
            subject=subject,
            queue=queue,
            callback=callback,
            connection=self
        )

        self._subscriptions[s.sid] = s
        cmd = [b'SUB', s.subject.encode('utf-8'), s.queue.encode('utf-8'), str(s.sid).encode('utf-8'), b'\r\n']
        self._writer.write(b' '.join(cmd))
        self._next_sid += 1

        return s

    def unsubscribe(self, subscription, max=None):
        """
        Unsubscribe will remove interest in the given subject. If max is
        provided an automatic Unsubscribe that is processed by the server
        when max messages have been received

        Args:
            subscription (pynats.Subscription): a Subscription object
            max (int=None): number of messages
        """
        cmd = [b'UNSUB', str(subscription.sid).encode('utf-8')]
        if not max is None:
            cmd.append(str(max).encode('utf-8'))
        cmd.append(b'\r\n')
        self._writer.write(b' '.join(cmd))

    def publish(self, subject, msg=None, reply=None):
        """
        Publish publishes the data argument to the given subject.

        Args:
            subject (string): a string with the subject
            msg (string): payload string
            reply (string): subject used in the reply
        """
        cmd = [b'PUB ', subject.encode('utf-8'), b' ']
        if not reply is None:
            cmd.append(reply.encode('utf-8'))
            cmd.append(b' ')

        if msg:
            if isinstance(msg, str):
                msg = msg.encode('utf-8')
            cmd.append(str(len(msg)).encode('utf-8'))
            cmd.append(b'\r\n')
            cmd.append(msg)
            cmd.append(b'\r\n')
        else:
            cmd.append(b' 0\r\n\r\n')
        self._writer.write(b''.join(cmd))

    def request(self, subject, callback, msg=None):
        """
        Publish a message with an implicit inbox listener as the reply.
        Message is optional.

        Args:
            subject (string): a string with the subject
            callback (function): callback to be called
            msg (string=None): payload string
        """
        inbox = self._build_inbox()
        s = self.subscribe(inbox, callback)
        self.unsubscribe(s, 1)
        self.publish(subject, msg, inbox)

        return s

    def _build_inbox(self):
        id = ''.join(random.choice(string.ascii_lowercase) for i in range(13))
        return "_INBOX.%s" % id

    @asyncio.coroutine
    def wait(self, duration=None, count=0):
        while not self._reader.at_eof():
            line = yield from self._reader.readline()
            if len(line) == 0:
                continue
            elif len(line) < 4:
                LOG.error("Unexpected response: %r", line)
            else:
                yield from self._dispatch(line)

        LOG.debug("Finished loop")

    def _build_dispatch_table(self):
        return {
            b'MSG': self._handle_msg,
            b'+OK ': self._handle_ok,
            b'-ER': self._handle_error,
            b'PIN': self._handle_ping,
            b'PON': self._handle_pong,
            b'INF': self._handle_info
        }

    @asyncio.coroutine
    def _dispatch(self, line):
        handler = self._dispatch_table.get(line[0:3], None)
        if handler:
            if asyncio.iscoroutinefunction(handler):
                yield from handler(line)
            else:
                handler(line)
        else:
            LOG.error("Unknown command '%s'", line.split(maxsplit=1)[0])

    @asyncio.coroutine
    def _handle_msg(self, line):
        _, subject, sid, *var = line.split()
        subject = subject.decode('utf-8')
        sid = int(sid)
        size = int(var[-1])
        reply = var[0].decode('utf-8') if len(var) > 1 else None
        data = yield from self._reader.readexactly(size+2)
        data = bytearray(data)
        del data[-2:]

        msg = Message(
            sid=sid,
            subject=subject,
            size=size,
            data=data,
            reply=reply
        )

        s = self._subscriptions.get(sid)
        s.received += 1

        # Check for auto-unsubscribe
        if s.max > 0 and s.received == s.max:
            self._subscriptions.pop(s.sid)

        return s.handle_msg(msg)

    @asyncio.coroutine
    def _handle_ping(self, _):
        self._writer.write(b'PONG\r\n')
        yield from self._writer.drain()

    def _handle_ok(self, _):
        LOG.debug('Received OK')

    def _handle_error(self, err):
        LOG.error('Received Error: %s', err.decode('utf-8'))

    def _handle_pong(self, message):
        self._ping_mutex.set_result(message)

    def _handle_info(self, info):
        self._info = json.loads(info[4:].decode('utf-8'))
        LOG.debug('Received Info: %s', self._info)

    @asyncio.coroutine
    def reconnect(self):
        """
        Close the connection to the NATS server and open a new one
        """
        yield from self.close()
        yield from self.connect()

    @asyncio.coroutine
    def close(self):
        """
        Close will close the connection to the server.
        """
        if self._writer is None:
            return

        if self._writer.can_write_eof():
            self._writer.write_eof()
        if self._writer is None:
            yield from self._writer.close()


class UnknownResponse(Exception):
    pass


class UnexpectedResponse(Exception):
    pass
