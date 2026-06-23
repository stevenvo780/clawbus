"""clawbus.bridge — Telegram <-> clawbus broker bridge (one process per room).

The bridge connects to the clawbus broker over WebSocket as a special
participant named ``bridge:<room>`` and mirrors a Telegram group to a
clawbus room:

* Telegram messages from allow-listed humans  -> ``publish`` event to the broker.
* Broker ``deliver`` events from agents       -> posted to the Telegram group
  with the prefix ``"<icon> <alias>:\\n<body>"``.

Configuration (env vars; ``~/.env`` is loaded as a fallback):

* ``CLAWBUS_BRIDGE_ROOM``        — room name, e.g. ``equipo`` or ``bob``.
* ``CLAWBUS_BRIDGE_BOT_TOKEN``   — Telegram bot token for the room's bot.
* ``CLAWBUS_BRIDGE_CHAT_ID``     — numeric Telegram chat id of the group.
* ``CLAWBUS_BRIDGE_HUMAN_MAP``   — comma-separated ``alias:user_id`` pairs
  defining the allow-list (e.g. ``alice:123,bob:456``).
* ``CLAWBUS_BROKER_WS``          — broker ws URL (default
  ``ws://127.0.0.1:18791/``). ``CLAWBUS_BROKER_WS_HOST`` /
  ``CLAWBUS_BROKER_WS_PORT`` override host/port independently.
* ``CLAWBUS_BRIDGE_SECURITY_LOG``— optional path for a JSONL security log
  of dropped messages from non-allow-listed users.
* ``CLAWBUS_BRIDGE_LOG_LEVEL``   — log level (default ``INFO``).

Run with ``python -m clawbus.bridge``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import httpx
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from clawbus import (
    ALIVE,
    BROKER_WS_PORT,
    DELIVER,
    HEARTBEAT,
    HELLO,
    HOP_BUDGET,
    Message,
    PUBLISH,
    SUBSCRIBE,
    new_msg_id,
    parse_mentions,
    normalize_alias,
    to_json,
    from_json,
)


TELEGRAM_API_BASE = "https://api.telegram.org"
MAX_TG_TEXT_LEN = 4096  # Telegram Bot API hard limit

# Visual icons per participant (round-robin assigned). Section 7 of the spec
# says the icon "puede variar por participante (🔷🔶🟢🟡🟣) configurable".
PARTICIPANT_ICONS: tuple[str, ...] = ("🔷", "🔶", "🟢", "🟡", "🟣")
SYSTEM_ICON = "⚙️"


log = logging.getLogger("clawbus.bridge")


# ---------------------------------------------------------------------------
# Allow-list: telegram user_id <-> normalized human alias
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HumanMap:
    """Maps Telegram ``user_id`` -> normalized human alias.

    Also keeps the set of normalized aliases for fuzzy fallback matching
    against the Telegram ``username`` / ``first_name`` fields.
    """

    by_user_id: dict[int, str] = field(default_factory=dict)
    aliases: frozenset[str] = field(default_factory=frozenset)

    @classmethod
    def parse(cls, raw: str) -> "HumanMap":
        """Parses ``"alias:user_id,alias:user_id,..."`` (whitespace tolerant)."""
        by_user_id: dict[int, str] = {}
        aliases: set[str] = set()
        for piece in raw.split(","):
            piece = piece.strip()
            if not piece:
                continue
            if ":" not in piece:
                raise ValueError(
                    f"invalid human_map entry {piece!r}: expected 'alias:user_id'"
                )
            alias_raw, uid_raw = piece.split(":", 1)
            alias = normalize_alias(alias_raw)
            if not alias:
                raise ValueError(f"invalid alias in entry {piece!r}")
            try:
                uid = int(uid_raw.strip())
            except ValueError as exc:
                raise ValueError(
                    f"invalid user_id {uid_raw!r} in entry {piece!r}: {exc}"
                ) from exc
            by_user_id[uid] = alias
            aliases.add(alias)
        if not by_user_id:
            raise ValueError("human_map is empty")
        return cls(by_user_id=by_user_id, aliases=frozenset(aliases))

    def resolve(
        self,
        user_id: int,
        *,
        username: str | None = None,
        first_name: str | None = None,
    ) -> str | None:
        """Returns the normalized alias for a Telegram update, or None."""
        direct = self.by_user_id.get(user_id)
        if direct is not None:
            return direct
        # Fuzzy fallback: match normalized username / first_name against
        # the configured aliases. Useful when the operator hasn't pinned
        # the numeric user_id yet.
        for candidate in (username, first_name):
            if not candidate:
                continue
            n = normalize_alias(candidate)
            if n in self.aliases:
                return n
        return None


# ---------------------------------------------------------------------------
# Token bucket (Telegram outbound rate-limit)
# ---------------------------------------------------------------------------


class TokenBucket:
    """Async-safe token bucket.

    ``rate`` tokens added per second up to ``burst``. ``acquire`` blocks
    until at least ``n`` tokens are available.
    """

    def __init__(self, rate: float, burst: int) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if burst <= 0:
            raise ValueError("burst must be > 0")
        self.rate = rate
        self.burst = float(burst)
        self._tokens = float(burst)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, n: float = 1.0) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                self._last = now
                self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
                if self._tokens >= n:
                    self._tokens -= n
                    return
                deficit = n - self._tokens
            await asyncio.sleep(deficit / self.rate)


# ---------------------------------------------------------------------------
# Telegram API helpers (httpx, no python-telegram-bot dep needed)
# ---------------------------------------------------------------------------


class TelegramAPI:
    """Thin async wrapper around the Telegram Bot API used by the bridge."""

    def __init__(
        self,
        bot_token: str,
        http: httpx.AsyncClient,
        *,
        log: logging.Logger,
    ) -> None:
        self.bot_token = bot_token
        self._http = http
        self._log = log

    def _url(self, method: str) -> str:
        return f"{TELEGRAM_API_BASE}/bot{self.bot_token}/{method}"

    async def get_updates(
        self,
        *,
        offset: int | None,
        timeout: int = 30,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Long-poll ``getUpdates``; ``timeout`` is the long-poll window (s)."""
        params: dict[str, Any] = {
            "timeout": timeout,
            "limit": limit,
            "allowed_updates": json.dumps(["message"]),
        }
        if offset is not None:
            params["offset"] = offset
        # HTTP timeout must exceed the long-poll window so the request
        # does not get cut short by the client.
        http_timeout = timeout + 10.0
        try:
            resp = await self._http.get(
                self._url("getUpdates"), params=params, timeout=http_timeout
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            self._log.warning("getUpdates transport error: %s", exc)
            raise
        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"getUpdates not ok: {data}")
        return data.get("result", []) or []

    async def send_message(
        self,
        *,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
    ) -> dict[str, Any]:
        """Posts ``text`` to ``chat_id``; truncates to the Bot API limit."""
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text[:MAX_TG_TEXT_LEN],
        }
        if reply_to_message_id is not None:
            payload["reply_to_message_id"] = reply_to_message_id
        if parse_mode:
            payload["parse_mode"] = parse_mode
        resp = await self._http.post(
            self._url("sendMessage"), json=payload, timeout=10.0
        )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"sendMessage server error {resp.status_code}: {resp.text[:200]}"
            )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"sendMessage not ok: {data}")
        return data.get("result", {}) or {}

    async def get_me(self) -> dict[str, Any]:
        """Lightweight liveness check at startup."""
        resp = await self._http.get(self._url("getMe"), timeout=10.0)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"getMe not ok: {data}")
        return data.get("result", {}) or {}


# ---------------------------------------------------------------------------
# Main bridge class
# ---------------------------------------------------------------------------


class TelegramBridge:
    """One Telegram bridge per room."""

    HEARTBEAT_INTERVAL_S = 30.0
    WS_BACKOFF_INITIAL_S = 1.0
    WS_BACKOFF_MAX_S = 60.0
    POLL_TIMEOUT_S = 30
    POLL_LIMIT = 100
    DOWN_LOG_THRESHOLD_S = 300.0  # >5min down -> log "bridge-<room>-down"
    DEDUP_MAX_ENTRIES = 4096
    DEDUP_TTL_S = 600.0

    def __init__(
        self,
        *,
        room: str,
        bot_token: str,
        chat_id: int,
        broker_ws_url: str,
        human_map: HumanMap,
        bridge_alias: str | None = None,
        bot_username: str | None = None,
        security_log_path: str | None = None,
        log: logging.Logger | None = None,
    ) -> None:
        self.room = room
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.broker_ws_url = broker_ws_url
        self.human_map = human_map
        self.bridge_alias = bridge_alias or f"bridge:{room}"
        self.bot_username = bot_username
        self.security_log_path = security_log_path
        self._log = log or logging.getLogger(f"clawbus.bridge.{room}")

        self._http: httpx.AsyncClient | None = None
        self._tg: TelegramAPI | None = None

        self._ws: Any = None
        self._ws_ready = asyncio.Event()  # set after HELLO+SUBSCRIBE succeed
        self._ws_connected = asyncio.Event()
        self._stopped = asyncio.Event()

        self._outbox: deque[dict[str, Any]] = deque(maxlen=1000)
        self._tg_bucket = TokenBucket(rate=10.0, burst=20)
        self._published_ids: set[str] = set()  # dedup for own broadcasts
        self._seen_msg_ids: dict[str, float] = {}  # inbound dedup
        self._last_dedup_cleanup = time.monotonic()

        self._icon_for: dict[str, str] = {}
        self._next_icon_idx = 0

        self._down_since: float | None = None
        self._last_down_log = 0.0

    # ----- visual helpers --------------------------------------------------

    def _icon_for_alias(self, alias: str) -> str:
        if alias not in self._icon_for:
            self._icon_for[alias] = PARTICIPANT_ICONS[
                self._next_icon_idx % len(PARTICIPANT_ICONS)
            ]
            self._next_icon_idx += 1
        return self._icon_for[alias]

    def _render_for_telegram(self, msg: Message) -> str:
        """Renders a broker ``deliver`` as Telegram text (section 7 format)."""
        icon = self._icon_for_alias(msg.from_)
        head = f"{icon} {msg.from_}:"
        body = msg.body or ""
        return f"{head}\n{body}"

    # ----- dedup -----------------------------------------------------------

    def _is_inbound_duplicate(self, msg_id: str) -> bool:
        now = time.monotonic()
        # Periodic GC of stale entries (cheap, bounded).
        if now - self._last_dedup_cleanup > 60.0:
            cutoff = now - self.DEDUP_TTL_S
            self._seen_msg_ids = {
                k: v for k, v in self._seen_msg_ids.items() if v > cutoff
            }
            self._last_dedup_cleanup = now
        if msg_id in self._seen_msg_ids:
            return True
        self._seen_msg_ids[msg_id] = now
        if len(self._seen_msg_ids) > self.DEDUP_MAX_ENTRIES:
            # Drop the oldest entries.
            keep = self.DEDUP_MAX_ENTRIES // 2
            for k in sorted(self._seen_msg_ids, key=self._seen_msg_ids.get)[: keep]:
                self._seen_msg_ids.pop(k, None)
        return False

    # ----- security log ----------------------------------------------------

    def _log_security(self, *, user_id: int, username: str | None, reason: str) -> None:
        if not self.security_log_path:
            return
        try:
            line = json.dumps(
                {
                    "ts": time.time(),
                    "event": "human_not_allowlisted",
                    "room": self.room,
                    "user_id": user_id,
                    "username": username,
                    "reason": reason,
                },
                ensure_ascii=False,
            )
            with open(self.security_log_path, "a", encoding="utf-8") as fp:
                fp.write(line + "\n")
        except OSError as exc:
            self._log.warning("could not write security log: %s", exc)

    # ----- Telegram -> bus -------------------------------------------------

    async def _handle_telegram_update(self, update: dict[str, Any]) -> None:
        msg = update.get("message") or update.get("edited_message")
        if not msg:
            return
        # Only mirror updates from the configured group.
        chat = msg.get("chat") or {}
        if chat.get("id") != self.chat_id:
            return
        user = msg.get("from") or {}
        user_id = user.get("id")
        if user_id is None:
            return
        text = msg.get("text") or msg.get("caption") or ""
        if not text.strip():
            # Ignore stickers, photos, voice, etc. (out of scope for v1).
            return

        alias = self.human_map.resolve(
            user_id,
            username=user.get("username"),
            first_name=user.get("first_name"),
        )
        if alias is None:
            self._log.info(
                "drop update room=%s user_id=%s username=%s (not in allow-list)",
                self.room,
                user_id,
                user.get("username"),
            )
            self._log_security(
                user_id=user_id,
                username=user.get("username"),
                reason="not_in_allowlist",
            )
            return

        # Build a publish message: ``from = <human alias>``, broadcast to room.
        msg_obj = Message(
            id=new_msg_id(),
            room=self.room,
            from_=alias,
            to=[],  # broadcast
            kind="text",
            body=text,
            mentions=parse_mentions(text),
            hop_budget=HOP_BUDGET,
            hops_used=0,
            created_ts=time.time(),
        )
        self._published_ids.add(msg_obj.id)

        event = {
            "type": PUBLISH,
            "room": self.room,
            "msg": msg_obj.to_dict(),
        }
        await self._send_event(event)
        self._log.debug(
            "telegram -> bus: from=%s mentions=%s len=%d",
            alias,
            msg_obj.mentions,
            len(text),
        )

    # ----- bus -> Telegram -------------------------------------------------

    async def _handle_deliver(self, event: dict[str, Any]) -> None:
        msg_dict = event.get("msg") or {}
        if not isinstance(msg_dict, dict):
            return
        try:
            msg = Message.from_dict(msg_dict)
        except (KeyError, TypeError, ValueError) as exc:
            self._log.warning("invalid deliver msg: %s: %r", exc, msg_dict)
            return
        # Cross-room safety: the broker should not do this, but be defensive.
        if msg.room != self.room:
            return
        # Dedup our own publishes (broker fans out the publish back to us).
        if msg.id in self._published_ids:
            self._published_ids.discard(msg.id)
            return
        if self._is_inbound_duplicate(msg.id):
            return
        # Don't repost messages originating from a human (visual loop).
        if msg.from_ in self.human_map.aliases:
            return
        # Drop loop-exhausted messages with a system notice instead.
        text = self._render_for_telegram(msg)
        try:
            assert self._tg is not None
            await self._tg.send_message(chat_id=self.chat_id, text=text)
        except (httpx.HTTPError, RuntimeError) as exc:
            self._log.error(
                "sendMessage failed (msg id=%s from=%s): %s",
                msg.id,
                msg.from_,
                exc,
            )

    # ----- WS send / queue -------------------------------------------------

    async def _send_event(self, event: dict[str, Any]) -> None:
        """Sends immediately if the WS is up; otherwise enqueues (FIFO drop)."""
        ws = self._ws
        if ws is not None and self._ws_ready.is_set():
            line = to_json(event)
            try:
                await ws.send(line)
                return
            except (ConnectionClosed, WebSocketException) as exc:
                self._log.warning("ws send failed (%s); queued", exc)
        # Queue for later.
        if len(self._outbox) == self._outbox.maxlen:
            dropped = self._outbox.popleft()
            self._log.warning(
                "outbox full; dropped oldest event type=%s",
                dropped.get("type"),
            )
        self._outbox.append(event)

    async def _flush_outbox(self) -> None:
        ws = self._ws
        if ws is None or not self._ws_ready.is_set():
            return
        flushed = 0
        failed = False
        while self._outbox and not failed:
            event = self._outbox[0]
            try:
                await ws.send(to_json(event))
            except (ConnectionClosed, WebSocketException) as exc:
                self._log.warning("ws flush send failed: %s", exc)
                failed = True
                break
            self._outbox.popleft()
            flushed += 1
        if flushed:
            self._log.info("flushed %d queued events (room=%s)", flushed, self.room)

    # ----- HELLO / SUBSCRIBE / HEARTBEAT -----------------------------------

    async def _register_with_broker(self) -> None:
        ws = self._ws
        if ws is None:
            return
        # ``auth`` and ``backend`` are intentionally omitted: the bridge is
        # an infrastructure-level relay, not a registered participant with
        # a backend. The broker must accept ``bridge:<room>`` as a special
        # pseudo-participant (section 7).
        hello = {
            "type": HELLO,
            "participant": self.bridge_alias,
            "rooms": [self.room],
            "kind": "bridge",
        }
        await ws.send(to_json(hello))
        sub = {
            "type": SUBSCRIBE,
            "participant": self.bridge_alias,
            "room": self.room,
        }
        await ws.send(to_json(sub))
        self._ws_ready.set()
        self._log.info(
            "registered with broker as %s (room=%s)",
            self.bridge_alias,
            self.room,
        )

    async def _heartbeat_loop(self) -> None:
        # Send one heartbeat shortly after start so the broker sees us alive
        # even if the first 30s tick would otherwise be silent.
        try:
            await asyncio.wait_for(self._ws_ready.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            return
        while not self._stopped.is_set():
            event = {
                "type": HEARTBEAT,
                "participant": self.bridge_alias,
                "state": ALIVE,
            }
            try:
                await self._send_event(event)
            except Exception as exc:  # noqa: BLE001
                self._log.warning("heartbeat send error: %s", exc)
            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=self.HEARTBEAT_INTERVAL_S
                )
            except asyncio.TimeoutError:
                pass

    # ----- WS lifecycle ----------------------------------------------------

    async def _ws_listener(self) -> None:
        ws = self._ws
        if ws is None:
            return
        try:
            async for raw in ws:
                line = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
                try:
                    event = from_json(line)
                except (ValueError, json.JSONDecodeError) as exc:
                    self._log.warning(
                        "invalid json from broker: %s: %r", exc, line[:200]
                    )
                    continue
                etype = event.get("type")
                if etype == DELIVER:
                    try:
                        await self._handle_deliver(event)
                    except Exception as exc:  # noqa: BLE001
                        self._log.exception("handle_deliver error: %s", exc)
                elif etype == "nudge":
                    # Bridge is not a turn-running agent; just log.
                    self._log.info("nudge received: %s", event.get("reason") or event)
                elif etype == "status":
                    self._log.debug("status from broker: %s", event)
                else:
                    self._log.debug("unhandled broker event type=%s", etype)
        except (ConnectionClosed, WebSocketException) as exc:
            self._log.warning("ws listener closed: %s", exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._log.exception("ws listener crashed: %s", exc)

    async def _connect_ws_loop(self) -> None:
        backoff = self.WS_BACKOFF_INITIAL_S
        self._down_since = time.monotonic()
        while not self._stopped.is_set():
            try:
                async with websockets.connect(
                    self.broker_ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=2**20,
                    open_timeout=10,
                ) as ws:
                    self._ws = ws
                    self._ws_connected.set()
                    self._down_since = None
                    self._log.info("connected to broker at %s", self.broker_ws_url)
                    try:
                        await self._register_with_broker()
                        await self._flush_outbox()
                        await self._ws_listener()
                    finally:
                        self._ws = None
                        self._ws_ready.clear()
                        self._ws_connected.clear()
                    # Reset backoff after a clean session.
                    backoff = self.WS_BACKOFF_INITIAL_S
            except asyncio.CancelledError:
                raise
            except (OSError, ConnectionClosed, WebSocketException) as exc:
                now = time.monotonic()
                if self._down_since is None:
                    self._down_since = now
                self._maybe_log_down(now)
                self._log.warning(
                    "ws connect failed (%s: %s); retrying in %.1fs",
                    type(exc).__name__,
                    exc,
                    backoff,
                )
                try:
                    await asyncio.wait_for(self._stopped.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(self.WS_BACKOFF_MAX_S, backoff * 2)
            except Exception as exc:  # noqa: BLE001
                self._log.exception("unexpected ws error: %s", exc)
                try:
                    await asyncio.wait_for(self._stopped.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(self.WS_BACKOFF_MAX_S, backoff * 2)

    def _maybe_log_down(self, now: float) -> None:
        if self._down_since is None:
            return
        elapsed = now - self._down_since
        if (
            elapsed >= self.DOWN_LOG_THRESHOLD_S
            and (now - self._last_down_log) >= self.DOWN_LOG_THRESHOLD_S
        ):
            self._log.warning(
                "bridge-%s-down: ws disconnected for %.0fs", self.room, elapsed
            )
            self._last_down_log = now

    # ----- Telegram poller -------------------------------------------------

    async def _telegram_poller(self) -> None:
        assert self._tg is not None
        offset: int | None = None
        backoff = 1.0
        while not self._stopped.is_set():
            try:
                updates = await self._tg.get_updates(
                    offset=offset,
                    timeout=self.POLL_TIMEOUT_S,
                    limit=self.POLL_LIMIT,
                )
                backoff = 1.0
            except (httpx.HTTPError, RuntimeError) as exc:
                self._log.warning(
                    "getUpdates error: %s; retrying in %.1fs", exc, backoff
                )
                try:
                    await asyncio.wait_for(self._stopped.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(60.0, backoff * 2)
                continue
            for upd in updates:
                upd_id = upd.get("update_id")
                if isinstance(upd_id, int):
                    offset = max(offset or 0, upd_id + 1)
                try:
                    await self._handle_telegram_update(upd)
                except Exception as exc:  # noqa: BLE001
                    self._log.exception("handle_telegram_update error: %s", exc)

    # ----- main entrypoint -------------------------------------------------

    async def run(self) -> None:
        """Starts the WS loop, heartbeat, and Telegram poller; runs forever."""
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(60.0))
        self._tg = TelegramAPI(self.bot_token, self._http, log=self._log)
        # ``getMe`` is a soft liveness check: a transient Telegram API
        # outage must not prevent the bridge from coming up.
        try:
            me = await self._tg.get_me()
            bot_username = me.get("username")
            if bot_username and not self.bot_username:
                self.bot_username = bot_username
        except (httpx.HTTPError, RuntimeError) as exc:
            self._log.warning("getMe failed at startup: %s", exc)
            bot_username = None
        self._log.info(
            "starting bridge room=%s alias=%s chat_id=%s broker=%s bot=@%s",
            self.room,
            self.bridge_alias,
            self.chat_id,
            self.broker_ws_url,
            self.bot_username or bot_username or "?",
        )
        tasks = [
            asyncio.create_task(self._connect_ws_loop(), name=f"ws-{self.room}"),
            asyncio.create_task(self._heartbeat_loop(), name=f"hb-{self.room}"),
            asyncio.create_task(self._telegram_poller(), name=f"tg-{self.room}"),
        ]
        try:
            await self._stopped.wait()
        finally:
            self._stopped.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._http.aclose()
            self._http = None
            self._tg = None

    def stop(self) -> None:
        """Signals the bridge to shut down."""
        self._stopped.set()


# ---------------------------------------------------------------------------
# .env loader (fallback for the bot token per spec)
# ---------------------------------------------------------------------------


def _load_env_file(path: Path) -> dict[str, str]:
    """Parses a simple ``KEY=VALUE`` file; ignores blanks and ``#`` comments."""
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key:
            continue
        value = value.strip().strip('"').strip("'")
        out[key] = value
    return out


def _load_env() -> dict[str, str]:
    """Loads ``~/.env`` then overlays ``os.environ`` (env wins)."""
    merged: dict[str, str] = {}
    home_env = Path(os.path.expanduser("~/.env"))
    merged.update(_load_env_file(home_env))
    for key, value in os.environ.items():
        merged[key] = value
    return merged


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def _build_bridge_from_env(env: dict[str, str]) -> TelegramBridge:
    room = env.get("CLAWBUS_BRIDGE_ROOM")
    if not room:
        raise SystemExit("CLAWBUS_BRIDGE_ROOM is required")
    bot_token = env.get("CLAWBUS_BRIDGE_BOT_TOKEN")
    if not bot_token:
        raise SystemExit("CLAWBUS_BRIDGE_BOT_TOKEN is required")
    chat_id_raw = env.get("CLAWBUS_BRIDGE_CHAT_ID")
    if not chat_id_raw:
        raise SystemExit("CLAWBUS_BRIDGE_CHAT_ID is required")
    try:
        chat_id = int(chat_id_raw)
    except ValueError as exc:
        raise SystemExit(
            f"CLAWBUS_BRIDGE_CHAT_ID must be an integer, got {chat_id_raw!r}"
        ) from exc
    human_map_raw = env.get("CLAWBUS_BRIDGE_HUMAN_MAP", "")
    if not human_map_raw:
        raise SystemExit(
            "CLAWBUS_BRIDGE_HUMAN_MAP is required (e.g. 'alice:123,bob:456')"
        )
    try:
        human_map = HumanMap.parse(human_map_raw)
    except ValueError as exc:
        raise SystemExit(f"invalid CLAWBUS_BRIDGE_HUMAN_MAP: {exc}") from exc
    broker_ws_url = env.get("CLAWBUS_BROKER_WS")
    if not broker_ws_url:
        host = env.get("CLAWBUS_BROKER_WS_HOST", "127.0.0.1")
        port = int(env.get("CLAWBUS_BROKER_WS_PORT", str(BROKER_WS_PORT)))
        broker_ws_url = f"ws://{host}:{port}/"
    bridge_alias = env.get("CLAWBUS_BRIDGE_ALIAS") or f"bridge:{room}"
    return TelegramBridge(
        room=room,
        bot_token=bot_token,
        chat_id=chat_id,
        broker_ws_url=broker_ws_url,
        human_map=human_map,
        bridge_alias=bridge_alias,
        security_log_path=env.get("CLAWBUS_BRIDGE_SECURITY_LOG"),
    )


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("CLAWBUS_BRIDGE_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    env = _load_env()
    bridge = _build_bridge_from_env(env)
    try:
        asyncio.run(bridge.run())
    except KeyboardInterrupt:
        bridge.stop()
        print("bridge stopped", file=sys.stderr)


if __name__ == "__main__":
    main()