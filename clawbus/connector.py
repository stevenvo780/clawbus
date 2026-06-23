"""clawbus.connector — Participant connector: WS client + turn orchestration.

The Connector manages the lifecycle of one clawbus agent participant:
- WebSocket connection to the broker with exponential-backoff reconnect.
- Periodic heartbeat (state + quota).
- Inbound message delivery → activation check → backend turn → publish reply.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from clawbus import (
    ALIVE,
    BROKER_WS_PORT,
    DELIVER,
    DEAD,
    HEARTBEAT,
    HELLO,
    HOP_BUDGET,
    IDLE,
    Message,
    PUBLISH,
    WORKING,
    new_msg_id,
    parse_mentions,
    to_json,
    from_json,
)
from clawbus.backends import Backend, create_backend

_log = logging.getLogger("clawbus.connector")

_WS_BACKOFF_INITIAL_S = 1.0
_WS_BACKOFF_MAX_S = 60.0
_HEARTBEAT_INTERVAL_S = 15.0
_DEFAULT_TURN_TIMEOUT_S = 120.0
_BROADCAST_ALIASES = frozenset({"all", "here"})


class Connector:
    """WebSocket client and turn orchestrator for one clawbus agent."""

    def __init__(
        self,
        participant_alias: str,
        backend: Backend,
        broker_url: str = "ws://127.0.0.1:18791",
        auth_token: str = "",
    ) -> None:
        self.alias: str = participant_alias
        self._backend: Backend = backend
        self._broker_url: str = broker_url
        self._auth_token: str = auth_token
        self._turn_timeout_s: float = float(
            os.getenv("CLAWBUS_TURN_TIMEOUT_S", str(_DEFAULT_TURN_TIMEOUT_S))
        )

        # Session id per (alias, room) — persistent across turns in same subscription.
        self._sessions: dict[str, str] = {}

        # Internal state
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._ws_ready: asyncio.Event = asyncio.Event()
        self._stopped: asyncio.Event = asyncio.Event()
        self._down_since: float | None = None
        self._published_ids: set[str] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Reconnect loop with exponential backoff. Sends HELLO on connect."""
        backoff = _WS_BACKOFF_INITIAL_S
        self._down_since = time.monotonic()

        while not self._stopped.is_set():
            try:
                async with websockets.connect(
                    self._broker_url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=2**20,
                    open_timeout=10,
                ) as ws:
                    self._ws = ws
                    self._down_since = None
                    _log.info(
                        "connected to broker at %s as %s",
                        self._broker_url,
                        self.alias,
                    )
                    await self._send_hello(ws)
                    self._ws_ready.set()
                    try:
                        await self._ws_listen(ws)
                    finally:
                        self._ws = None
                        self._ws_ready.clear()
                    backoff = _WS_BACKOFF_INITIAL_S
            except asyncio.CancelledError:
                raise
            except (OSError, ConnectionClosed, WebSocketException) as exc:
                now = time.monotonic()
                if self._down_since is None:
                    self._down_since = now
                _log.warning(
                    "ws connect failed (%s: %s); retrying in %.1fs",
                    type(exc).__name__,
                    exc,
                    backoff,
                )
                try:
                    await asyncio.wait_for(
                        self._stopped.wait(), timeout=backoff
                    )
                except asyncio.TimeoutError:
                    pass
                backoff = min(_WS_BACKOFF_MAX_S, backoff * 2)
            except Exception as exc:  # noqa: BLE001
                _log.exception("unexpected ws error: %s", exc)
                try:
                    await asyncio.wait_for(
                        self._stopped.wait(), timeout=backoff
                    )
                except asyncio.TimeoutError:
                    pass
                backoff = min(_WS_BACKOFF_MAX_S, backoff * 2)

    async def run(self) -> None:
        """Starts the WS connect loop and heartbeat; runs until stopped."""
        tasks = [
            asyncio.create_task(self.connect(), name=f"ws-{self.alias}"),
            asyncio.create_task(
                self._heartbeat_loop(), name=f"hb-{self.alias}"
            ),
        ]
        try:
            await self._stopped.wait()
        finally:
            self._stopped.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def publish(self, msg: Message) -> None:
        """Send a PUBLISH event with the given message to the broker."""
        event = {
            "type": PUBLISH,
            "msg": msg.to_dict(),
        }
        await self._send_event(event)

    async def heartbeat(self, state: str, quota: dict[str, Any] | None = None) -> None:
        """Send a HEARTBEAT with the current state and optional quota dict."""
        event: dict[str, object] = {
            "type": HEARTBEAT,
            "participant": self.alias,
            "state": state,
        }
        if quota is not None:
            event["quota"] = quota
        await self._send_event(event)

    async def subscribe(self, room: str) -> None:
        """Send a SUBSCRIBE event for a room."""
        event = {
            "type": "subscribe",
            "participant": self.alias,
            "room": room,
        }
        await self._send_event(event)
        _log.info("subscribed %s to room=%s", self.alias, room)

    async def unsubscribe(self, room: str) -> None:
        """Send an UNSUBSCRIBE event for a room."""
        event = {
            "type": "unsubscribe",
            "participant": self.alias,
            "room": room,
        }
        await self._send_event(event)
        _log.info("unsubscribed %s from room=%s", self.alias, room)

    # ------------------------------------------------------------------
    # Deliver / turn orchestration
    # ------------------------------------------------------------------

    def _should_activate(self, msg: Message) -> bool:
        """Determine whether this participant should run a turn.

        Activates if:
        1. The participant alias is in ``msg.mentions``, OR
        2. ``msg.in_reply_to`` was originated by this participant, OR
        3. The mention list includes a broadcast alias (@all / @here)
           AND the message comes from a human.
        """
        if self.alias in msg.mentions:
            return True
        if msg.in_reply_to is not None and msg.in_reply_to in self._published_ids:
            return True
        if msg.from_ != self.alias:
            broadcast = any(
                m in _BROADCAST_ALIASES for m in msg.mentions
            )
            if broadcast:
                return True
        return False

    async def on_deliver(self, msg: Message) -> None:
        """Main delivery callback.

        Validates activation, runs the backend turn with timeout,
        builds and publishes a reply message.
        """
        if not self._should_activate(msg):
            _log.debug("skipping deliver %s (not activated)", msg.id)
            return

        _log.info("deliver %s -> running turn for %s", msg.id, self.alias)

        session_id = self._get_session_id(msg.room)

        await self.heartbeat(WORKING)

        try:
            response = await asyncio.wait_for(
                self._backend.run_turn(msg.body, session_id),
                timeout=self._turn_timeout_s,
            )
        except asyncio.TimeoutError:
            response = (
                f"\u274c {self.alias}: turno abortado por timeout"
            )
            _log.warning("turn timeout for %s (session=%s)", self.alias, session_id)
        except Exception as exc:  # noqa: BLE001
            response = f"\u274c {self.alias}: {exc}"
            _log.error("turn error for %s: %s", self.alias, exc)

        mentions = parse_mentions(response)
        reply_id = new_msg_id()

        reply_msg = Message(
            id=reply_id,
            room=msg.room,
            from_=self.alias,
            to=list(mentions) if mentions else [],
            kind="text",
            body=response,
            mentions=list(mentions),
            in_reply_to=msg.id,
            hop_budget=msg.hop_budget - 1,
            hops_used=msg.hops_used + 1,
            created_ts=time.time(),
        )

        self._published_ids.add(reply_id)
        await self.publish(reply_msg)
        await self.heartbeat(IDLE)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_session_id(self, room: str) -> str:
        """Return a persistent pseudorandom session id for (alias, room)."""
        key = f"{self.alias}:{room}"
        if key not in self._sessions:
            self._sessions[key] = new_msg_id()
        return self._sessions[key]

    async def _send_hello(
        self, ws: websockets.WebSocketClientProtocol
    ) -> None:
        hello: dict[str, object] = {
            "type": HELLO,
            "participant": self.alias,
            "backend": self._backend.__class__.__name__,
        }
        if self._auth_token:
            hello["auth"] = self._auth_token
        await ws.send(to_json(hello))
        _log.debug("hello sent for %s", self.alias)

    async def _send_event(self, event: dict[str, Any]) -> None:
        """Send a JSONL line over the WS, or log a warning if disconnected."""
        ws = self._ws
        if ws is None or not self._ws_ready.is_set():
            _log.debug("ws not ready; dropped event type=%s", event.get("type"))
            return
        line = to_json(event)
        try:
            await ws.send(line)
        except (ConnectionClosed, WebSocketException) as exc:
            _log.warning("ws send failed: %s", exc)

    async def _ws_listen(
        self, ws: websockets.WebSocketClientProtocol
    ) -> None:
        """Read events from the broker and dispatch."""
        try:
            async for raw in ws:
                line = (
                    raw if isinstance(raw, str)
                    else raw.decode("utf-8", "replace")
                )
                try:
                    event = from_json(line)
                except (ValueError, json.JSONDecodeError) as exc:
                    _log.warning(
                        "invalid json from broker: %s: %r", exc, line[:200]
                    )
                    continue

                etype = event.get("type")
                if etype == DELIVER:
                    msg_dict = event.get("msg") or {}
                    if not isinstance(msg_dict, dict):
                        _log.warning("deliver without msg dict: %r", event)
                        continue
                    try:
                        msg = Message.from_dict(msg_dict)
                    except (KeyError, TypeError, ValueError) as exc:
                        _log.warning(
                            "invalid deliver msg: %s: %r", exc, msg_dict
                        )
                        continue
                    try:
                        await self.on_deliver(msg)
                    except Exception as exc:  # noqa: BLE001
                        _log.exception("on_deliver error: %s", exc)
                elif etype == "nudge":
                    _log.info("nudge received: %s", event.get("reason") or event)
                elif etype == "status":
                    _log.debug("status from broker: %s", event)
                else:
                    _log.debug("unhandled broker event type=%s", etype)
        except (ConnectionClosed, WebSocketException) as exc:
            _log.warning("ws listener closed: %s", exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            _log.exception("ws listener crashed: %s", exc)

    async def _heartbeat_loop(self) -> None:
        """Send periodic HEARTBEAT events every ``_HEARTBEAT_INTERVAL_S``."""
        try:
            await asyncio.wait_for(self._ws_ready.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            return

        while not self._stopped.is_set():
            try:
                await self.heartbeat(ALIVE)
            except Exception as exc:  # noqa: BLE001
                _log.warning("heartbeat send error: %s", exc)
            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=_HEARTBEAT_INTERVAL_S
                )
            except asyncio.TimeoutError:
                pass
