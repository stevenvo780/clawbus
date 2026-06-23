#!/usr/bin/env python3
"""Smoke test end-to-end sin red: Broker + Bridge fake + Agente fake.

Flujo:
  1. Bridge publica mensaje @carol
  2. Agente recibe, valida mentions
  3. Agente responde con in_reply_to, hop_budget-1, hops_used+1
  4. Bridge recibe la respuesta y valida los metadatos
"""

import asyncio
import sys
import tempfile
import time
from pathlib import Path

import websockets

# Asegurar que el módulo clawbus sea importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from clawbus import (
    Message,
    HELLO,
    SUBSCRIBE,
    PUBLISH,
    DELIVER,
    from_json,
    to_json,
    new_msg_id,
)
from clawbus.registry import Registry
from clawbus.loopguard import LoopGuard
from clawbus.broker import Broker


def find_free_port() -> int:
    """Encuentra un puerto TCP libre."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def client_hello_subscribe(
    ws: websockets.ClientConnection,
    alias: str,
    room: str,
) -> str:
    """Conecta un cliente: HELLO + SUBSCRIBE. Retorna el alias normalizado."""
    await ws.send(to_json({
        "type": HELLO,
        "participant": alias,
        "auth": "smoke-test-token",
        "backend": "claude_code",
        "gateway": "ws",
    }))
    ack = from_json(await ws.recv())
    assert ack["status"] == "ok", f"Hello failed: {ack}"

    await ws.send(to_json({"type": SUBSCRIBE, "room": room}))
    ack = from_json(await ws.recv())
    assert ack["status"] == "subscribed", f"Subscribe failed: {ack}"

    return ack.get("participant", alias)


async def main() -> int:
    """Orquesta el smoke test completo."""
    print("=== clawbus smoke_inproc ===")

    port = find_free_port()
    print(f"Puerto efímero: {port}")

    # ── Setup: Registry, Rooms, LoopGuard, Broker ───────────────────────
    with tempfile.TemporaryDirectory() as tmpdir:
        from clawbus import Room

        registry = Registry(tmpdir)
        registry.add_or_update_room(Room(
            name="equipo",
            telegram_chat_id=0,
            bot_token_ref="smoke",
        ))

        loopguard = LoopGuard()
        broker = Broker(registry, loopguard, host="127.0.0.1", port=port)

        broker_task = asyncio.create_task(broker.start())
        await asyncio.sleep(0.3)
        print("Broker iniciado.")

        try:
            ws_url = f"ws://127.0.0.1:{port}"

            async with websockets.connect(ws_url) as ws_bridge, \
                       websockets.connect(ws_url) as ws_agent:

                # ── Conexión y suscripción ──────────────────────────
                print("Conectando bridge (alice)...")
                await client_hello_subscribe(ws_bridge, "alice", "equipo")

                print("Conectando agente (carol)...")
                await client_hello_subscribe(ws_agent, "carol", "equipo")

                # ── Paso 1: Bridge publica mensaje @carol ──────────
                msg_id = new_msg_id()
                msg1 = Message(
                    id=msg_id,
                    room="equipo",
                    from_="alice",
                    body="@carol tomá el encargo",
                    mentions=["carol"],
                    hop_budget=3,
                    hops_used=0,
                )
                print(f"\n[Bridge] Publicando msg1 id={msg_id}: \"{msg1.body}\"")
                await ws_bridge.send(to_json({
                    "type": PUBLISH,
                    "room": "equipo",
                    "msg": msg1.to_dict(),
                }))

                # Bridge recibe DELIVER (self-echo) + ACK (fan-out va antes que ack)
                self_deliver = from_json(await ws_bridge.recv())
                assert self_deliver["type"] == DELIVER
                print(f"[Bridge] self-echo: {self_deliver['msg']['id'][:12]}...")

                ack1 = from_json(await ws_bridge.recv())
                assert ack1["status"] == "delivered", f"Expected delivered, got {ack1}"
                print(f"[Bridge] ack: {ack1['status']}")

                # ── Paso 2: Agente recibe DELIVER, valida mentions ──
                del1 = from_json(await ws_agent.recv())
                assert del1["type"] == DELIVER, f"Expected DELIVER, got {del1['type']}"
                assert del1["msg"]["mentions"] == ["carol"], \
                    f"Expected mentions=['carol'], got {del1['msg']['mentions']}"
                assert del1["msg"]["from"] == "alice"
                print(f"[Agente] Recibido: \"{del1['msg']['body']}\" mentions={del1['msg']['mentions']}")

                # ── Paso 3: Agente publica respuesta ─────────────────
                reply_id = new_msg_id()
                msg2 = Message(
                    id=reply_id,
                    room="equipo",
                    from_="carol",
                    to=["alice"],
                    kind="text",
                    body="ok @alice recibido",
                    mentions=["alice"],
                    in_reply_to=msg_id,
                    hop_budget=2,
                    hops_used=1,
                )
                print(f"\n[Agente] Publicando respuesta id={reply_id}: \"{msg2.body}\"")
                print(f"         in_reply_to={msg_id[:12]}... hop_budget={msg2.hop_budget} hops_used={msg2.hops_used}")
                await ws_agent.send(to_json({
                    "type": PUBLISH,
                    "room": "equipo",
                    "msg": msg2.to_dict(),
                }))

                # Agente recibe DELIVER (self-echo) + ACK
                self_deliver2 = from_json(await ws_agent.recv())
                assert self_deliver2["type"] == DELIVER

                ack2 = from_json(await ws_agent.recv())
                assert ack2["status"] == "delivered", f"Expected delivered, got {ack2}"
                print(f"[Agente] ack: {ack2['status']}")

                # ── Paso 4: Bridge recibe DELIVER de vuelta ──────────
                del2 = from_json(await ws_bridge.recv())
                assert del2["type"] == DELIVER, f"Expected DELIVER, got {del2['type']}"
                print(f"\n[Bridge] Recibido respuesta: \"{del2['msg']['body']}\"")

                # ── Paso 5: Validaciones finales ─────────────────────
                m = del2["msg"]

                # hop_budget y hops_used
                assert m["hop_budget"] == 2, \
                    f"Expected hop_budget=2, got {m['hop_budget']} (debería bajar de 3→2)"
                assert m["hops_used"] == 1, \
                    f"Expected hops_used=1, got {m['hops_used']} (debería subir de 0→1)"

                # in_reply_to
                assert m["in_reply_to"] == msg_id, \
                    f"Expected in_reply_to={msg_id}, got {m['in_reply_to']}"

                # mentions
                assert m["mentions"] == ["alice"], \
                    f"Expected mentions=['alice'], got {m['mentions']}"

                # from (quién respondió)
                assert m["from"] == "carol", \
                    f"Expected from='carol', got {m['from']}"

                print("\n=== Todas las validaciones pasaron ===")

        finally:
            broker_task.cancel()
            try:
                await broker_task
            except asyncio.CancelledError:
                pass
            await asyncio.sleep(0.1)
            print("Broker detenido.")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
