"""Tests de routing y aislamiento del Broker clawbus (WebSocket)."""

import asyncio
import socket
import tempfile

import pytest
import pytest_asyncio
import websockets

from clawbus import (
    Room,
    Message,
    HELLO,
    SUBSCRIBE,
    UNSUBSCRIBE,
    PUBLISH,
    DELIVER,
    HEARTBEAT,
    from_json,
    to_json,
    new_msg_id,
)
from clawbus.registry import Registry
from clawbus.loopguard import LoopGuard
from clawbus.broker import Broker


# ── Helpers ─────────────────────────────────────────────────────────────────


def _find_free_port() -> int:
    """Encuentra un puerto TCP efímero libre."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_msg(**overrides: object) -> Message:
    """Fabrica un Message con defaults de test y overrides."""
    kwargs: dict[str, object] = {
        "id": new_msg_id(),
        "room": "equipo",
        "from_": "bridge",
        "body": "test message",
        **overrides,
    }
    return Message(**kwargs)  # type: ignore[arg-type]


def _make_publish(room: str, msg: Message) -> dict[str, object]:
    """Crea un dict de evento publish."""
    return {"type": PUBLISH, "room": room, "msg": msg.to_dict()}


async def _client_hello_subscribe(
    ws: websockets.ClientConnection,
    alias: str,
    room: str,
    backend: str = "claude_code",
) -> str:
    """Helper: HELLO + SUBSCRIBE, retorna el participant_alias confirmado."""
    await ws.send(to_json({
        "type": HELLO,
        "participant": alias,
        "auth": "test-token",
        "backend": backend,
        "gateway": "ws",
    }))
    ack = from_json(await asyncio.wait_for(ws.recv(), timeout=5))
    assert ack["type"] == "ack"
    assert ack["status"] == "ok", f"Hello failed: {ack}"
    participant = ack["participant"]

    await ws.send(to_json({"type": SUBSCRIBE, "room": room}))
    ack = from_json(await asyncio.wait_for(ws.recv(), timeout=5))
    assert ack["type"] == "ack"
    assert ack["status"] == "subscribed", f"Subscribe failed: {ack}"

    return participant


async def _recv_event(ws: websockets.ClientConnection, timeout: float = 5) -> dict:
    """Recibe un evento JSONL con timeout."""
    line = await asyncio.wait_for(ws.recv(), timeout=timeout)
    return from_json(line)


# ── Broker fixture ──────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def broker_port() -> int:
    """Arranca un Broker en puerto efímero con rooms de test y devuelve el puerto."""
    port = _find_free_port()

    with tempfile.TemporaryDirectory() as tmpdir:
        registry = Registry(tmpdir)
        registry.add_or_update_room(Room(
            name="equipo",
            telegram_chat_id=0,
            bot_token_ref="test",
        ))
        registry.add_or_update_room(Room(
            name="bob",
            telegram_chat_id=0,
            bot_token_ref="test",
        ))

        loopguard = LoopGuard()
        broker = Broker(registry, loopguard, host="127.0.0.1", port=port)

        task = asyncio.create_task(broker.start())
        await asyncio.sleep(0.3)

        yield port

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(0.1)


# ── Fan-out test ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fan_out(broker_port: int) -> None:
    """Dos clientes suscritos al mismo room reciben el DELIVER de un PUBLISH."""
    ws_url = f"ws://127.0.0.1:{broker_port}"

    async with websockets.connect(ws_url) as ws1:
        await _client_hello_subscribe(ws1, "bridge", "equipo")

        async with websockets.connect(ws_url) as ws2:
            await _client_hello_subscribe(ws2, "carol", "equipo")

            # Publish desde bridge
            msg = _make_msg(
                from_="bridge", to=["carol"],
                body="@carol hola", mentions=["carol"],
            )
            await ws1.send(to_json(_make_publish("equipo", msg)))

            # ws1 recibe deliver (self-echo, fan-out va antes que ack) + ack
            deliver1 = await _recv_event(ws1)
            assert deliver1["type"] == DELIVER
            assert deliver1["room"] == "equipo"

            ack = await _recv_event(ws1)
            assert ack["type"] == "ack"
            assert ack["status"] == "delivered", f"got {ack['status']}"

            # ws2 recibe deliver
            deliver2 = await _recv_event(ws2)
            assert deliver2["type"] == DELIVER
            assert deliver2["room"] == "equipo"

            # Ambos delivers deben tener los mentions correctos
            for d in (deliver1, deliver2):
                assert "carol" in d["msg"]["mentions"], f"Missing mention in {d}"
                assert d["msg"]["from"] == "bridge"


@pytest.mark.asyncio
async def test_fan_out_with_mentions(broker_port: int) -> None:
    """PUBLISH con mentions y to los entrega a ambos suscriptores."""
    ws_url = f"ws://127.0.0.1:{broker_port}"

    async with websockets.connect(ws_url) as ws_bridge, \
               websockets.connect(ws_url) as ws_carol:

        await _client_hello_subscribe(ws_bridge, "bridge", "equipo")
        await _client_hello_subscribe(ws_carol, "carol", "equipo")

        msg = _make_msg(
            from_="bridge",
            to=["carol"],
            body="@carol tomá el encargo",
            mentions=["carol"],
            hop_budget=3,
            hops_used=0,
        )
        await ws_bridge.send(to_json(_make_publish("equipo", msg)))

        # Bridge recibe deliver (self-echo) + ack
        d1 = await _recv_event(ws_bridge)
        assert d1["type"] == DELIVER
        ack = await _recv_event(ws_bridge)
        assert ack["status"] == "delivered"

        # Carol recibe deliver
        d2 = await _recv_event(ws_carol)
        assert d2["type"] == DELIVER
        assert d1["msg"]["mentions"] == ["carol"]
        assert d2["msg"]["mentions"] == ["carol"]


# ── Isolation test ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_isolation_different_rooms(broker_port: int) -> None:
    """Un cliente suscrito a 'bob' NO recibe mensajes publicados en 'equipo'."""
    ws_url = f"ws://127.0.0.1:{broker_port}"

    async with websockets.connect(ws_url) as ws_team, \
               websockets.connect(ws_url) as ws_bob:

        await _client_hello_subscribe(ws_team, "carol", "equipo")
        await _client_hello_subscribe(ws_bob, "alice", "bob")

        # Publicar en equipo
        msg = _make_msg(from_="carol", room="equipo", body="solo para equipo")
        await ws_team.send(to_json(_make_publish("equipo", msg)))

        # ws_team recibe deliver (self-echo) + ack
        deliver = await _recv_event(ws_team)
        assert deliver["type"] == DELIVER
        assert deliver["room"] == "equipo"
        ack = await _recv_event(ws_team)
        assert ack["status"] == "delivered"

        # ws_bob NO debe recibir nada de "equipo"
        with pytest.raises(asyncio.TimeoutError):
            await _recv_event(ws_bob, timeout=1.0)


@pytest.mark.asyncio
async def test_isolation_publish_goes_only_to_subscribers(broker_port: int) -> None:
    """PUBLISH en un room solo llega a sus suscriptores, no a otros."""
    ws_url = f"ws://127.0.0.1:{broker_port}"

    async with websockets.connect(ws_url) as ws_a, \
               websockets.connect(ws_url) as ws_b, \
               websockets.connect(ws_url) as ws_c:

        await _client_hello_subscribe(ws_a, "alice", "equipo")
        await _client_hello_subscribe(ws_b, "bob", "equipo")
        await _client_hello_subscribe(ws_c, "carol", "bob")

        msg = _make_msg(from_="alice", room="equipo", body="team chat")
        await ws_a.send(to_json(_make_publish("equipo", msg)))

        # alice recibe deliver (self-echo) + ack
        assert (await _recv_event(ws_a))["type"] == DELIVER
        assert (await _recv_event(ws_a))["type"] == "ack"

        # bob recibe deliver
        assert (await _recv_event(ws_b))["type"] == DELIVER

        # carol NO recibe nada
        with pytest.raises(asyncio.TimeoutError):
            await _recv_event(ws_c, timeout=1.0)


# ── Rate-limit test (broker integration) ────────────────────────────────────


@pytest.mark.asyncio
async def test_rate_limit_broker(broker_port: int) -> None:
    """Publicar 10 mensajes rápidos desde un emisor: todos se entregan bajo el tope
    de 30/min. La serialización '1 turno simultáneo' se removió (rompía el fan-out
    multi-destinatario); el anti-loop es hop_budget + el circuit-breaker por par."""
    ws_url = f"ws://127.0.0.1:{broker_port}"

    async with websockets.connect(ws_url) as ws:
        await _client_hello_subscribe(ws, "publisher", "equipo")

        for msg in [_make_msg() for _ in range(10)]:
            await ws.send(to_json(_make_publish("equipo", msg)))

        # Cada publish responde un ack (y hace deliver de vuelta al suscriptor).
        acks: list[dict] = []
        while len(acks) < 10:
            e = await _recv_event(ws)
            if e["type"] == "ack":
                acks.append(e)

        assert all(a["status"] == "delivered" for a in acks), \
            f"todos deben entregarse bajo el tope 30/min: {[a['status'] for a in acks]}"


# ── Unsubscribe test ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unsubscribe_stops_delivery(broker_port: int) -> None:
    """Después de UNSUBSCRIBE, el cliente no recibe más mensajes del room."""
    ws_url = f"ws://127.0.0.1:{broker_port}"

    async with websockets.connect(ws_url) as ws:
        await _client_hello_subscribe(ws, "carol", "equipo")

        # Unsubscribe
        await ws.send(to_json({"type": UNSUBSCRIBE, "room": "equipo"}))
        ack = await _recv_event(ws)
        assert ack["status"] == "unsubscribed"

        # Otro cliente publica en equipo
        async with websockets.connect(ws_url) as ws2:
            await _client_hello_subscribe(ws2, "bridge", "equipo")
            msg = _make_msg(from_="bridge", body="no deberías ver esto")
            await ws2.send(to_json(_make_publish("equipo", msg)))

            # ws2 recibe deliver (self-echo) + ack
            assert (await _recv_event(ws2))["type"] == DELIVER
            assert (await _recv_event(ws2))["type"] == "ack"

        # ws (carol) NO debe recibir nada porque se desuscribió
        with pytest.raises(asyncio.TimeoutError):
            await _recv_event(ws, timeout=1.0)


# ── Heartbeat test ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_heartbeat_updates_participant_state(broker_port: int) -> None:
    """HEARTBEAT actualiza el estado del participante sin error."""
    ws_url = f"ws://127.0.0.1:{broker_port}"

    async with websockets.connect(ws_url) as ws:
        await _client_hello_subscribe(ws, "carol", "equipo")

        await ws.send(to_json({
            "type": HEARTBEAT,
            "state": "working",
            "quota": {"turns": 5},
        }))
        # El heartbeat no genera respuesta, solo verificamos que no crashee
        await asyncio.sleep(0.2)

        # Seguimos operativos: publicar debería funcionar
        msg = _make_msg(from_="carol", body="post-heartbeat")
        await ws.send(to_json(_make_publish("equipo", msg)))
        # deliver (self-echo) + ack
        assert (await _recv_event(ws))["type"] == DELIVER
        ack = await _recv_event(ws)
        assert ack["status"] == "delivered"
