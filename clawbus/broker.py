import asyncio
import json
import os
import time
from typing import Optional, Set
import logging

import websockets
from websockets.server import WebSocketServerProtocol

from clawbus import (
    Room, Participant, Subscription, Message,
    HELLO, SUBSCRIBE, UNSUBSCRIBE, PUBLISH, DELIVER, HEARTBEAT, STATUS, NUDGE,
    ALIVE, WORKING, IDLE, DEAD,
    BROKER_WS_PORT, HOP_BUDGET,
    from_json, to_json, new_msg_id, normalize_alias, parse_mentions
)
from clawbus.registry import Registry
from clawbus.loopguard import LoopGuard

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Broker:
    """
    Servidor WebSocket pub/sub central.

    - Acepta hello/subscribe/unsubscribe/publish/heartbeat
    - Fan-out deliver a suscriptores del room
    - Marca idle (>45s) / dead (>90s, >180s si working)
    - Nudge a idle>300s
    - Expone GET /health
    - Aislamiento por room
    """

    def __init__(self, registry: Registry, loopguard: LoopGuard,
                 host: str = "0.0.0.0", port: int = BROKER_WS_PORT):
        self.registry = registry
        self.loopguard = loopguard
        self.host = host
        self.port = port

        # Mapeo de conexiones abiertas: { participant_alias: WebSocketServerProtocol }
        self.connections: dict[str, WebSocketServerProtocol] = {}

        # Mapeo de rooms activos: { room_name: set(participant_aliases) }
        self.room_members: dict[str, Set[str]] = {}

        # Circuit-breaker anti-loop: { (room, from, to): [ts, ...] } últimos 60s.
        self._pair_log: dict = {}

    async def start(self):
        """Inicia el servidor WebSocket y el heartbeat checker."""
        async with websockets.serve(self.handle_client, self.host, self.port):
            logger.info(f"Broker listening on ws://{self.host}:{self.port}")

            # Inicia el task de heartbeat checking
            await asyncio.gather(
                self._heartbeat_checker(),
                return_exceptions=True
            )

    async def handle_client(self, websocket: WebSocketServerProtocol):
        """Maneja una conexión WebSocket de un cliente."""
        participant_alias: Optional[str] = None

        try:
            async for line in websocket:
                try:
                    event = from_json(line)
                    event_type = event.get("type")

                    if event_type == HELLO:
                        participant_alias = await self._handle_hello(websocket, event)

                    elif participant_alias is None:
                        await self._send_error(websocket, "Not authenticated (no hello)")

                    elif event_type == SUBSCRIBE:
                        await self._handle_subscribe(websocket, event, participant_alias)

                    elif event_type == UNSUBSCRIBE:
                        await self._handle_unsubscribe(websocket, event, participant_alias)

                    elif event_type == PUBLISH:
                        await self._handle_publish(websocket, event, participant_alias)

                    elif event_type == HEARTBEAT:
                        await self._handle_heartbeat(websocket, event, participant_alias)

                    elif event_type == STATUS:
                        await self._handle_status(websocket, event)

                    else:
                        await self._send_error(websocket, f"Unknown event type: {event_type}")

                except ValueError as e:
                    await self._send_error(websocket, f"Invalid JSON: {e}")
                except Exception as e:
                    logger.error(f"Error processing event: {e}")
                    await self._send_error(websocket, str(e))

        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            # Un socket anterior no debe desconectar al participante reconectado.
            if participant_alias and self.connections.get(participant_alias) is websocket:
                del self.connections[participant_alias]
                # Marcar como offline
                participant = self.registry.get_participant(participant_alias)
                if participant:
                    participant.state = DEAD
                    self.registry.add_or_update_participant(participant)

    async def _handle_hello(self, websocket: WebSocketServerProtocol, event: dict) -> Optional[str]:
        """Valida auth y registra al participante."""
        participant_alias = event.get("participant")
        auth = event.get("auth")
        backend = event.get("backend")
        gateway = event.get("gateway")
        rooms_list = event.get("rooms", [])

        if not participant_alias or not auth:
            await self._send_error(websocket, "Missing participant or auth")
            return None

        # TODO: Validar auth contra vault (por ahora, simplemente aceptar)
        participant_alias = normalize_alias(participant_alias)

        # Crear/actualizar Participant
        participant = Participant(
            alias=participant_alias,
            backend=backend or "claude_code",
            gateway=gateway or "",
            rooms=list(rooms_list),
            state=ALIVE,
            last_heartbeat_ts=time.time(),
            created_ts=time.time()
        )
        self.registry.add_or_update_participant(participant)

        # Guardar conexión
        self.connections[participant_alias] = websocket

        # Responder ack
        await self._send_event(websocket, {
            "type": "ack",
            "participant": participant_alias,
            "status": "ok"
        })

        logger.info(f"Participant {participant_alias} authenticated")
        return participant_alias

    async def _handle_subscribe(self, websocket: WebSocketServerProtocol, event: dict, participant_alias: str):
        """Agrega una suscripción."""
        room = event.get("room")

        if not room:
            await self._send_error(websocket, "Missing room")
            return

        if not self.registry.get_room(room):
            await self._send_error(websocket, f"Room {room} does not exist")
            return

        # Agregar suscripción
        subscription = Subscription(
            participant=participant_alias,
            room=room,
            since=time.time()
        )
        self.registry.add_subscription(subscription)

        # Actualizar room_members
        if room not in self.room_members:
            self.room_members[room] = set()
        self.room_members[room].add(participant_alias)

        # Actualizar participant.rooms
        participant = self.registry.get_participant(participant_alias)
        if participant and room not in participant.rooms:
            participant.rooms.append(room)
            self.registry.add_or_update_participant(participant)

        await self._send_event(websocket, {
            "type": "ack",
            "room": room,
            "status": "subscribed"
        })

        logger.info(f"{participant_alias} subscribed to {room}")

    async def _handle_unsubscribe(self, websocket: WebSocketServerProtocol, event: dict, participant_alias: str):
        """Remueve una suscripción."""
        room = event.get("room")

        if not room:
            await self._send_error(websocket, "Missing room")
            return

        self.registry.remove_subscription(participant_alias, room)

        if room in self.room_members:
            self.room_members[room].discard(participant_alias)

        # Actualizar participant.rooms
        participant = self.registry.get_participant(participant_alias)
        if participant and room in participant.rooms:
            participant.rooms.remove(room)
            self.registry.add_or_update_participant(participant)

        await self._send_event(websocket, {
            "type": "ack",
            "room": room,
            "status": "unsubscribed"
        })

    async def _handle_publish(self, websocket: WebSocketServerProtocol, event: dict, participant_alias: str):
        """Valida y hace fan-out de un mensaje."""
        room = event.get("room")
        msg_dict = event.get("msg")

        if not room or not msg_dict:
            await self._send_error(websocket, "Missing room or msg")
            return

        # Parsear mensaje
        try:
            msg = Message.from_dict(msg_dict)
        except Exception as e:
            await self._send_error(websocket, f"Invalid message: {e}")
            return

        # Validar con loopguard
        ok, reason = self.loopguard.validate(room, msg.id, participant_alias, msg.hop_budget)

        if not ok:
            if reason == "dedup_hit":
                await self._send_event(websocket, {"type": "ack", "status": "dedup_hit"})
            elif reason == "hop_budget_zero":
                # Entregar sin turno
                await self._fan_out_deliver(room, msg, no_turn=True)
                await self._send_event(websocket, {"type": "ack", "status": "delivered_no_turn"})
            elif reason == "rate_limit_exceeded":
                await self._send_event(websocket, {"type": "ack", "status": "queued"})
            elif reason == "dropped":
                await self._send_event(websocket, {"type": "ack", "status": "dropped"})
            return

        # Circuit-breaker anti-loop por par dirigido (from→to): corta el ping-pong
        # estructuralmente, sin depender de hop_budget (que el canal Claude resetea a 3
        # en cada send_message). Broadcast (to vacío) no cuenta. Tope 12 msgs/par/60s.
        now = time.time()
        for tgt in (msg.to or []):
            key = (room, msg.from_, tgt)
            hist = [t for t in self._pair_log.get(key, []) if now - t < 60]
            if len(hist) >= 12:
                await self._send_event(websocket, {"type": "ack", "status": "loop_suspected"})
                logger.warning(f"loop_suspected {msg.from_}->{tgt} en {room}; mensaje descartado")
                return
            hist.append(now)
            self._pair_log[key] = hist

        # Fan-out deliver
        await self._fan_out_deliver(room, msg)

        await self._send_event(websocket, {"type": "ack", "status": "delivered"})

    async def _handle_heartbeat(self, websocket: WebSocketServerProtocol, event: dict, participant_alias: str):
        """Actualiza heartbeat y estado del participant."""
        state = event.get("state", ALIVE)
        quota = event.get("quota", {})

        participant = self.registry.get_participant(participant_alias)
        if not participant:
            await self._send_error(websocket, "Participant not found")
            return

        participant.state = state
        participant.last_heartbeat_ts = time.time()
        participant.quota = quota
        self.registry.add_or_update_participant(participant)

        # Fin de turno: si el participante reporta que NO está trabajando, liberá
        # su lock de turno en el loopguard para que pueda volver a publicar.
        # mark_done() re-bloquea mientras quede algo en cola (devuelve el sgte id),
        # así que drenamos hasta None -> deja working=False y cola vacía. Durante un
        # burst sincrónico no llegan heartbeats, así que el burst se encola como debe.
        # NOTA(v1): los publishes encolados de un burst se DESCARTAN al cerrar el
        # turno (la cola guarda ids, no cuerpos; no se reentregan). Documentado.
        if state != WORKING:
            for room in list(participant.rooms):
                while self.loopguard.mark_done(room, participant_alias):
                    pass

    async def _handle_status(self, websocket: WebSocketServerProtocol, event: dict):
        """Retorna estado del broker."""
        now = time.time()
        participants_info = []

        for alias, p in self.registry.participants.items():
            last_hb_s = int(now - p.last_heartbeat_ts) if p.last_heartbeat_ts else -1
            participants_info.append({
                "alias": alias,
                "state": p.state,
                "rooms": p.rooms,
                "last_hb_s": last_hb_s,
                "quota": p.quota
            })

        response = {
            "type": STATUS,
            "ok": True,
            "rooms": list(self.registry.rooms.keys()),
            "participants": participants_info
        }

        await self._send_event(websocket, response)

    async def _fan_out_deliver(self, room: str, msg: Message, no_turn: bool = False):
        """Hace fan-out de un mensaje a los suscriptores del room."""
        deliver_event = {
            "type": DELIVER,
            "room": room,
            "msg": msg.to_dict()
        }

        if no_turn:
            deliver_event["no_turn"] = True

        line = to_json(deliver_event)

        # Obtener suscriptores del room
        subscriptions = self.registry.get_subscriptions_by_room(room)

        for sub in subscriptions:
            if sub.participant in self.connections:
                try:
                    await self.connections[sub.participant].send(line)
                except Exception as e:
                    logger.error(f"Failed to deliver to {sub.participant}: {e}")

    async def _send_event(self, websocket: WebSocketServerProtocol, event: dict):
        """Envía un evento al cliente."""
        line = to_json(event)
        try:
            await websocket.send(line)
        except Exception as e:
            logger.error(f"Failed to send event: {e}")

    async def _send_error(self, websocket: WebSocketServerProtocol, message: str):
        """Envía un error al cliente."""
        await self._send_event(websocket, {
            "type": "error",
            "message": message
        })

    async def _heartbeat_checker(self):
        """Task periódico que marca idle/dead y emite nudges."""
        while True:
            await asyncio.sleep(10)  # Checkear cada 10s

            now = time.time()

            for alias, participant in list(self.registry.participants.items()):
                elapsed = now - participant.last_heartbeat_ts

                # Determinar nuevo estado
                if participant.state == WORKING:
                    # Extendido a 180s si está working
                    if elapsed > 180:
                        participant.state = DEAD
                else:
                    # 45s → idle, 90s → dead
                    if elapsed > 90:
                        participant.state = DEAD
                    elif elapsed > 45:
                        participant.state = IDLE

                # Emitir nudge si idle > 300s
                if participant.state == IDLE and elapsed > 300:
                    for room in participant.rooms:
                        if participant.alias in self.connections:
                            await self._send_event(
                                self.connections[participant.alias],
                                {
                                    "type": NUDGE,
                                    "room": room,
                                    "to": participant.alias,
                                    "reason": f"idle>{int(elapsed)}s"
                                }
                            )

                # Podar muertos viejos (>1h sin heartbeat y sin conexión viva): limpia
                # el cruft de tests y evita que _fan_out_deliver/_save crezcan sin límite.
                if (participant.state == DEAD and elapsed > 3600
                        and alias not in self.connections):
                    for room in list(participant.rooms):
                        self.registry.remove_subscription(alias, room)
                    self.registry.remove_participant(alias)
                    continue

                self.registry.add_or_update_participant(participant)


async def main():
    """Inicializa y corre el broker."""
    state_dir = os.getenv("CLAWBUS_STATE_DIR", "/workspace/clawbus/.state")
    registry = Registry(state_dir)

    # Sembrar rooms por defecto si faltan (host nuevo / clawbus_data fresco o corrupto):
    # sin rooms el broker RECHAZA toda suscripción → bus muerto con el contenedor "Up".
    for rn in ("agentes", "equipo"):
        if not registry.get_room(rn):
            registry.add_or_update_room(Room(rn, 0, "", [], time.time()))
            logger.info(f"room por defecto sembrado: {rn}")

    loopguard = LoopGuard()
    broker = Broker(registry, loopguard)

    await broker.start()


if __name__ == "__main__":
    asyncio.run(main())
