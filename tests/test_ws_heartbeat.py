import asyncio
import base64
import os
from contextlib import asynccontextmanager
from urllib.parse import urlencode

import pytest
from websockets.frames import Frame, Opcode

from granian import Granian
from granian.errors import ConfigurationError


@asynccontextmanager
async def control_client(port, path='/ws_heartbeat', **options):
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    try:
        key = base64.b64encode(os.urandom(16)).decode()
        target = path
        if options:
            target += '?' + urlencode(options)
        writer.write(
            (
                f'GET {target} HTTP/1.1\r\n'
                f'Host: 127.0.0.1:{port}\r\n'
                'Connection: Upgrade\r\n'
                'Upgrade: websocket\r\n'
                'Sec-WebSocket-Version: 13\r\n'
                f'Sec-WebSocket-Key: {key}\r\n\r\n'
            ).encode('ascii')
        )
        await writer.drain()
        response = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), 3)
        assert response.startswith(b'HTTP/1.1 101 ')
        yield reader, writer
    finally:
        writer.close()
        await writer.wait_closed()


async def read_short_frame(reader, timeout=3):
    header = await asyncio.wait_for(reader.readexactly(2), timeout)
    assert header[0] & 0x80 and header[1] <= 125
    payload = await asyncio.wait_for(reader.readexactly(header[1]), 3)
    return Opcode(header[0] & 0x0F), payload


async def read_control(reader, timeout=3):
    opcode, payload = await read_short_frame(reader, timeout)
    assert opcode in (Opcode.PING, Opcode.PONG, Opcode.CLOSE)
    return opcode, payload


async def send_frame(writer, opcode, payload):
    writer.write(Frame(opcode, payload).serialize(mask=True))
    await writer.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize('runtime_mode', ['mt', 'st'])
async def test_native_ping_pong_with_busy_asgi_consumer(asgi_server, runtime_mode):
    async with (
        asgi_server(
            runtime_mode,
            websocket_options={'websocket_ping_interval': 0.15, 'websocket_ping_timeout': 0.5},
        ) as port,
        control_client(port) as (reader, writer),
    ):
        opcode, ping = await read_control(reader)
        assert opcode == Opcode.PING and ping
        await send_frame(writer, Opcode.PONG, ping)

        await send_frame(writer, Opcode.PING, b'client ping')
        assert await read_control(reader) == (Opcode.PONG, b'client ping')

        opcode, next_ping = await read_control(reader)
        assert opcode == Opcode.PING and next_ping != ping
        await send_frame(writer, Opcode.PONG, next_ping)


@pytest.mark.asyncio
@pytest.mark.parametrize('wrong_pong', [False, True])
async def test_native_ping_timeout_releases_unresponsive_peer(asgi_server, wrong_pong):
    async with (
        asgi_server(
            'st',
            websocket_options={
                'websocket_ping_interval': 0.1,
                'websocket_ping_timeout': 0.2,
                'websocket_close_timeout': 0.1,
            },
        ) as port,
        control_client(port) as (reader, writer),
    ):
        opcode, ping = await read_control(reader)
        assert opcode == Opcode.PING and ping
        if wrong_pong:
            await send_frame(writer, Opcode.PONG, b'unrelated pong')
            await send_frame(writer, Opcode.TEXT, b'application traffic')
        opcode, payload = await read_control(reader)
        assert opcode == Opcode.CLOSE and int.from_bytes(payload[:2], 'big') == 1011

        # The peer never acknowledges Close. The server still drops the socket.
        assert await asyncio.wait_for(reader.read(), 2) == b''


@pytest.mark.asyncio
@pytest.mark.parametrize('runtime_mode', ['mt', 'st'])
async def test_heartbeat_path_is_exact_and_ignores_query(asgi_server, runtime_mode):
    async with (
        asgi_server(
            runtime_mode,
            websocket_options={
                'websocket_ping_interval': 0.1,
                'websocket_ping_timeout': 0.2,
                'websocket_close_timeout': 0.1,
                'websocket_heartbeat_paths': ['/api/v4/ws'],
            },
        ) as port,
        control_client(port, '/api/v4/ws/legacy') as (legacy_reader, legacy_writer),
        control_client(port, '/api/v4/ws', client='v4') as (v4_reader, _),
    ):
        opcode, ping = await read_control(v4_reader)
        assert opcode == Opcode.PING and ping
        opcode, payload = await read_control(v4_reader)
        assert opcode == Opcode.CLOSE and int.from_bytes(payload[:2], 'big') == 1011
        assert await asyncio.wait_for(v4_reader.read(), 2) == b''

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(legacy_reader.read(1), 0.4)
        await send_frame(legacy_writer, Opcode.TEXT, b'legacy still works')
        assert await read_short_frame(legacy_reader) == (Opcode.TEXT, b'legacy still works')


@pytest.mark.asyncio
async def test_v4_heartbeat_with_default_timeouts(asgi_server):
    async with (
        asgi_server(
            'st',
            websocket_options={
                'websocket_ping_interval': 20,
                'websocket_heartbeat_paths': ['/api/v4/ws'],
            },
        ) as port,
        control_client(port, '/api/v4/ws') as (reader, writer),
    ):
        clock = asyncio.get_running_loop().time
        started = clock()
        opcode, ping = await read_control(reader, timeout=30)
        assert opcode == Opcode.PING and ping
        assert clock() - started >= 19
        await send_frame(writer, Opcode.PONG, ping)

        # The matching Pong keeps the socket alive for the next heartbeat.
        opcode, next_ping = await read_control(reader, timeout=30)
        assert opcode == Opcode.PING and next_ping and next_ping != ping

        # With no Pong or Close acknowledgement, Granian uses its default timeouts.
        started = clock()
        opcode, payload = await read_control(reader, timeout=15)
        assert opcode == Opcode.CLOSE and int.from_bytes(payload[:2], 'big') == 1011
        assert clock() - started >= 9

        started = clock()
        assert await asyncio.wait_for(reader.read(), timeout=10) == b''
        assert clock() - started >= 4


@pytest.mark.asyncio
@pytest.mark.parametrize('interval', [None, 0.5])
async def test_manual_channels_ping(asgi_server, interval):
    async with (
        asgi_server('st', websocket_options={'websocket_ping_interval': interval}) as port,
        control_client(port, manual=1) as (reader, writer),
    ):
        assert await read_control(reader) == (Opcode.PING, b'')
        await send_frame(writer, Opcode.PONG, b'')
        if interval is not None:
            opcode, payload = await read_control(reader)
            assert opcode == Opcode.PING and payload
            await send_frame(writer, Opcode.PONG, payload)


@pytest.mark.asyncio
async def test_manual_ping_waits_for_pending_native_pong(asgi_server):
    async with (
        asgi_server(
            'st',
            websocket_options={'websocket_ping_interval': 0.5, 'websocket_ping_timeout': 1},
        ) as port,
        control_client(port, manual_after_message=1) as (reader, writer),
    ):
        opcode, payload = await read_control(reader)
        assert opcode == Opcode.PING and payload
        await send_frame(writer, Opcode.TEXT, b'request a manual ping')
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(read_control(reader), 0.1)

        await send_frame(writer, Opcode.PONG, payload)
        assert await read_control(reader) == (Opcode.PING, b'')
        await send_frame(writer, Opcode.PONG, b'')


@pytest.mark.asyncio
async def test_receive_overflow_closes_busy_asgi_consumer(asgi_server):
    async with (
        asgi_server('st', websocket_options={'websocket_ping_interval': 2, 'websocket_close_timeout': 0.1}) as port,
        control_client(port) as (reader, writer),
    ):
        writer.write(b''.join(Frame(Opcode.BINARY, b'x').serialize(mask=True) for _ in range(256)))
        await writer.drain()
        opcode, payload = await read_control(reader)
        assert opcode == Opcode.CLOSE and int.from_bytes(payload[:2], 'big') == 1013
        assert await asyncio.wait_for(reader.read(), 2) == b''


@pytest.mark.asyncio
async def test_blocked_writer_releases_asgi_send_and_receive(asgi_server, tmp_path):
    marker = tmp_path / 'disconnected'

    async def wait_for_disconnect():
        while not marker.exists():
            await asyncio.sleep(0.05)

    async with (
        asgi_server(
            'st',
            websocket_options={
                'websocket_ping_interval': 0.1,
                'websocket_ping_timeout': 0.2,
                'websocket_close_timeout': 0.1,
            },
        ) as port,
        control_client(port, blocked_write=1, marker=marker) as (_, writer),
    ):
        writer.transport.pause_reading()
        await asyncio.wait_for(wait_for_disconnect(), 3)


@pytest.mark.parametrize(
    'options',
    [
        {'websocket_ping_interval': 0},
        {'websocket_ping_timeout': float('nan')},
        {'websocket_close_timeout': float('inf')},
        {'interface': 'rsgi', 'websocket_ping_interval': 20},
        {'websockets': False, 'websocket_ping_interval': 20},
        {'http': '2', 'websocket_ping_interval': 20},
        {'websocket_heartbeat_paths': ['/api/v4/ws']},
        {'websocket_ping_interval': 20, 'websocket_heartbeat_paths': ['/api/v4/ws?client=v4']},
    ],
)
def test_invalid_heartbeat_configuration(options):
    with pytest.raises(ConfigurationError):
        Granian('tests.apps.asgi:app', **{'interface': 'asgi', **options})
