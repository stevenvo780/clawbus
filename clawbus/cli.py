from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from clawbus import (
    ALIVE,
    BACKEND_CLAUDE_CODE,
    BACKEND_OPENCLAW,
    BACKEND_OPENCODE,
    BROKER_WS_PORT,
    HELLO,
    NUDGE,
    Participant,
    STATUS,
    SUBSCRIBE,
    UNSUBSCRIBE,
    from_json,
    normalize_alias,
    to_json,
)

DEFAULT_BACKEND: str = BACKEND_CLAUDE_CODE
DEFAULT_GATEWAY: str = "http://127.0.0.1:18810"
DEFAULT_BROKER_HOST: str = "127.0.0.1"
WS_TIMEOUT_S: float = 5.0
REGISTRY_PATH: Path = Path("/var/lib/clawbus/registry.json")

OK_MARK: str = "\u2713"
ERR_MARK: str = "\u2717"


class BrokerUnavailable(RuntimeError):
    pass


def conf_dir() -> Path:
    env = os.environ.get("CLAWBUS_CONF_DIR")
    if env:
        return Path(env)
    try:
        if os.geteuid() == 0:
            return Path("/etc/clawbus")
    except (AttributeError, OSError):
        pass
    return Path.home() / ".config" / "clawbus"


def participants_dir() -> Path:
    d = conf_dir() / "participants"
    d.mkdir(parents=True, exist_ok=True)
    return d


def participant_file(alias: str) -> Path:
    return participants_dir() / f"{alias}.json"


def gateway_default() -> str:
    path = conf_dir() / "gateway.json"
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                url = data.get("default")
                if isinstance(url, str) and url.strip():
                    return url
        except (OSError, json.JSONDecodeError):
            pass
    return DEFAULT_GATEWAY


def load_participant(alias: str) -> Optional[Participant]:
    path = participant_file(alias)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return Participant.from_dict(data)
    except (OSError, json.JSONDecodeError, KeyError, ValueError, TypeError):
        return None


def save_participant(p: Participant) -> None:
    path = participant_file(p.alias)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(p.to_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp, path)


def delete_participant(alias: str) -> bool:
    try:
        participant_file(alias).unlink()
        return True
    except FileNotFoundError:
        return False


def broker_ws_url() -> str:
    host = os.environ.get("CLAWBUS_BROKER_WS_HOST", DEFAULT_BROKER_HOST)
    port_raw = os.environ.get("CLAWBUS_BROKER_WS_PORT", str(BROKER_WS_PORT))
    try:
        port = int(port_raw)
    except ValueError:
        port = BROKER_WS_PORT
    return f"ws://{host}:{port}/"


async def broker_request(
    events: list[dict[str, Any]],
    *,
    expect_type: Optional[str] = None,
    timeout: float = WS_TIMEOUT_S,
) -> list[dict[str, Any]]:
    url = broker_ws_url()
    responses: list[dict[str, Any]] = []
    try:
        async with websockets.connect(
            url, open_timeout=timeout, close_timeout=timeout
        ) as ws:
            for ev in events:
                await ws.send(to_json(ev))
            deadline = time.monotonic() + timeout
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                line = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
                try:
                    parsed = from_json(line)
                except (ValueError, json.JSONDecodeError):
                    continue
                if not isinstance(parsed, dict):
                    continue
                responses.append(parsed)
                if expect_type and parsed.get("type") == expect_type:
                    break
    except (OSError, ConnectionClosed, WebSocketException) as exc:
        raise BrokerUnavailable(str(exc)) from exc
    return responses


def print_ok(msg: str) -> None:
    print(f"{OK_MARK} {msg}")


def print_err(msg: str) -> None:
    print(f"{ERR_MARK} Error: {msg}", file=sys.stderr)


def render_table(headers: tuple[str, ...], rows: list[tuple[Any, ...]]) -> None:
    str_rows: list[tuple[str, ...]] = [tuple(str(c) for c in r) for r in rows]
    widths: list[int] = [len(h) for h in headers]
    for r in str_rows:
        for i, cell in enumerate(r):
            if i < len(widths) and len(cell) > widths[i]:
                widths[i] = len(cell)

    def fmt(cells: tuple[str, ...]) -> str:
        parts: list[str] = []
        for i, c in enumerate(cells):
            w = widths[i] if i < len(widths) else len(c)
            parts.append(c.ljust(w))
        return " | ".join(parts)

    sep = "-+-".join("-" * w for w in widths)
    print(fmt(headers))
    print(sep)
    for r in str_rows:
        print(fmt(r))


_QUOTA_KEY_RE = re.compile(r"^([a-zA-Z][a-zA-Z0-9]*)_(\d+)([smhd])$")


def _parse_quota_key(key: str) -> tuple[str, str]:
    m = _QUOTA_KEY_RE.match(key)
    if m:
        return m.group(1), f"{m.group(2)}{m.group(3)}"
    return key, ""


def _format_reset_in_s(reset_in_s: Any) -> str:
    try:
        rs = int(reset_in_s)
    except (TypeError, ValueError):
        return ""
    if rs < 0:
        return ""
    if rs >= 3600:
        return f" (reset {rs // 3600}h{(rs % 3600) // 60}m)"
    if rs >= 60:
        return f" (reset {rs // 60}m)"
    return f" (reset {rs}s)"


def _format_quota(quota: dict[str, Any]) -> str:
    if not quota:
        return "-"
    parts: list[str] = []
    for key, info in quota.items():
        if not isinstance(info, dict):
            continue
        used_raw = info.get("used")
        used_str = "?"
        if used_raw is not None:
            try:
                used_str = f"{float(used_raw):.2f}"
            except (TypeError, ValueError):
                pass
        provider, window = _parse_quota_key(str(key))
        reset_str = _format_reset_in_s(info.get("reset_in_s"))
        suffix = f"/{window}" if window else ""
        parts.append(f"{provider}: {used_str}{suffix}{reset_str}")
    return "; ".join(parts) if parts else "-"


def _format_last_hb(value: Any) -> str:
    if value is None:
        return "-"
    try:
        return str(int(round(float(value))))
    except (TypeError, ValueError):
        return "-"


def render_status(participants: list[dict[str, Any]]) -> None:
    headers: tuple[str, ...] = (
        "PARTICIPANTE",
        "ROOM",
        "STATE",
        "LAST HB (s)",
        "CUOTA",
    )
    rows: list[tuple[str, ...]] = []
    for entry in participants:
        if not isinstance(entry, dict):
            continue
        alias = str(entry.get("alias", "?"))
        rooms_raw = entry.get("rooms", [])
        if isinstance(rooms_raw, list) and rooms_raw:
            room_s = ",".join(str(r) for r in rooms_raw)
        else:
            room_s = "-"
        state = str(entry.get("state", "?"))
        last_hb_raw = entry.get("last_hb_s", entry.get("last_heartbeat_ts", 0))
        last_hb_s = _format_last_hb(last_hb_raw)
        quota_raw = entry.get("quota", {})
        quota_s = _format_quota(quota_raw) if isinstance(quota_raw, dict) else "-"
        rows.append((alias, room_s, state, last_hb_s, quota_s))
    render_table(headers, rows)


def read_registry_rooms() -> Optional[list[dict[str, Any]]]:
    env = os.environ.get("CLAWBUS_REGISTRY")
    path = Path(env) if env else REGISTRY_PATH
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    rooms_raw = data.get("rooms")
    if not isinstance(rooms_raw, list):
        return None
    out: list[dict[str, Any]] = []
    for r in rooms_raw:
        if isinstance(r, dict):
            out.append(r)
    return out


def _extract_error(responses: list[dict[str, Any]]) -> Optional[str]:
    for r in responses:
        et = r.get("type")
        if et in ("error", "nack"):
            msg = r.get("message") or r.get("error")
            if isinstance(msg, str) and msg:
                return msg
            return "broker rejected the request"
        if "error" in r and isinstance(r["error"], str) and r["error"]:
            return r["error"]
    return None


async def cmd_join(args: argparse.Namespace) -> int:
    alias = normalize_alias(args.alias)
    if not alias:
        print_err("alias cannot be empty")
        return 2
    room_raw = args.room
    if not isinstance(room_raw, str) or not room_raw.strip():
        print_err("room cannot be empty")
        return 2
    room = room_raw.strip()
    backend: str = args.backend or DEFAULT_BACKEND
    gateway: str = args.gateway or gateway_default()

    existing = load_participant(alias)
    now = time.time()
    if existing is None:
        p = Participant(
            alias=alias,
            backend=backend,
            gateway=gateway,
            rooms=[],
            state=ALIVE,
            last_heartbeat_ts=0.0,
            quota={},
            created_ts=now,
        )
    else:
        p = existing
        p.backend = backend
        p.gateway = gateway

    if room not in p.rooms:
        p.rooms.append(room)

    try:
        save_participant(p)
    except OSError as exc:
        print_err(f"cannot persist participant file: {exc}")
        return 1

    events: list[dict[str, Any]] = [
        {
            "type": HELLO,
            "participant": alias,
            "rooms": [room],
            "backend": backend,
            "gateway": gateway,
            "kind": "cli",
        },
        {"type": SUBSCRIBE, "participant": alias, "room": room},
    ]
    try:
        responses = await broker_request(
            events, expect_type=SUBSCRIBE, timeout=WS_TIMEOUT_S
        )
    except BrokerUnavailable as exc:
        print_err(f"Broker unavailable (WS error): {exc}")
        return 1

    err_msg = _extract_error(responses)
    if err_msg is not None:
        print_err(err_msg)
        return 1

    print_ok(f"{alias} joined room {room} (backend: {backend})")
    return 0


async def cmd_leave(args: argparse.Namespace) -> int:
    alias = normalize_alias(args.alias)
    if not alias:
        print_err("alias cannot be empty")
        return 2

    p = load_participant(alias)
    has_room_arg = bool(args.room)

    if has_room_arg:
        rooms = [args.room]
    elif p is not None:
        rooms = list(p.rooms)
    else:
        rooms = []

    if not rooms:
        if has_room_arg:
            print_err(f"no subscription to room {args.room!r} for {alias}")
            return 1
        if p is None:
            print_err(f"unknown alias: {alias}")
            return 1
        delete_participant(alias)
        print_ok(f"{alias} deleted")
        return 0

    events = [
        {"type": UNSUBSCRIBE, "participant": alias, "room": r} for r in rooms
    ]
    try:
        responses = await broker_request(
            events, expect_type=UNSUBSCRIBE, timeout=WS_TIMEOUT_S
        )
    except BrokerUnavailable as exc:
        print_err(f"Broker unavailable (WS error): {exc}")
        return 1

    err_msg = _extract_error(responses)
    if err_msg is not None:
        print_err(err_msg)
        return 1

    if p is not None:
        for r in rooms:
            if r in p.rooms:
                p.rooms.remove(r)

    if not has_room_arg:
        delete_participant(alias)
        print_ok(f"{alias} deleted")
        return 0

    if p is not None and not p.rooms:
        delete_participant(alias)
        print_ok(f"{alias} deleted")
        return 0

    if p is not None:
        try:
            save_participant(p)
        except OSError as exc:
            print_err(f"cannot persist participant file: {exc}")
            return 1

    for r in rooms:
        print_ok(f"{alias} left room {r}")
    return 0


async def cmd_status(args: argparse.Namespace) -> int:
    event = {"type": STATUS}
    try:
        responses = await broker_request(
            [event], expect_type=STATUS, timeout=WS_TIMEOUT_S
        )
    except BrokerUnavailable as exc:
        print_err(f"Broker unavailable (WS error): {exc}")
        return 1

    resp = next((r for r in responses if r.get("type") == STATUS), None)
    if resp is None:
        print_err("no status response from broker")
        return 1

    participants_raw = resp.get("participants", [])
    participants: list[dict[str, Any]] = []
    if isinstance(participants_raw, list):
        for entry in participants_raw:
            if isinstance(entry, dict):
                participants.append(entry)
    render_status(participants)
    return 0


async def cmd_rooms(args: argparse.Namespace) -> int:
    rooms_data = read_registry_rooms()
    if rooms_data is None:
        try:
            responses = await broker_request(
                [{"type": STATUS}], expect_type=STATUS, timeout=WS_TIMEOUT_S
            )
        except BrokerUnavailable as exc:
            print_err(f"Broker unavailable (WS error): {exc}")
            return 1
        resp = next((r for r in responses if r.get("type") == STATUS), None)
        if resp is None:
            print_err("no rooms available (broker offline and no registry)")
            return 1
        names_raw = resp.get("rooms", [])
        names: list[str] = []
        if isinstance(names_raw, list):
            for n in names_raw:
                if isinstance(n, str):
                    names.append(n)
        rooms_data = [
            {"name": n, "bot_token_ref": "-", "allowlist_human": []} for n in names
        ]

    headers: tuple[str, ...] = ("ROOM", "BOT_TOKEN_REF", "ALLOWLIST")
    rows: list[tuple[str, ...]] = []
    for r in rooms_data:
        name = str(r.get("name", "?"))
        bot = str(r.get("bot_token_ref", "-"))
        allow_raw = r.get("allowlist_human", [])
        if isinstance(allow_raw, list) and allow_raw:
            allow_s = ", ".join(str(x) for x in allow_raw)
        else:
            allow_s = "-"
        rows.append((name, bot, allow_s))
    render_table(headers, rows)
    return 0


async def cmd_nudge(args: argparse.Namespace) -> int:
    alias = normalize_alias(args.alias)
    if not alias:
        print_err("alias cannot be empty")
        return 2

    if args.room:
        rooms = [args.room]
    else:
        p = load_participant(alias)
        if p is None:
            print_err(f"unknown alias: {alias}")
            return 1
        rooms = list(p.rooms)

    if not rooms:
        print_err(f"{alias} is not subscribed to any room")
        return 1

    events = [
        {"type": NUDGE, "room": r, "to": alias, "reason": "manual"}
        for r in rooms
    ]
    try:
        await broker_request(events, timeout=WS_TIMEOUT_S)
    except BrokerUnavailable as exc:
        print_err(f"Broker unavailable (WS error): {exc}")
        return 1

    print_ok(f"nudge sent to {alias}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="clawbus",
        description="clawbus CLI enrollment tool",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    join_p = sub.add_parser("join", help="Create/edit a participant and subscribe to a room")
    join_p.add_argument("alias", help="Participant alias")
    join_p.add_argument("--room", required=True, help="Room name to join")
    join_p.add_argument(
        "--backend",
        default=DEFAULT_BACKEND,
        choices=[BACKEND_CLAUDE_CODE, BACKEND_OPENCLAW, BACKEND_OPENCODE],
        help=f"Backend type (default: {DEFAULT_BACKEND})",
    )
    join_p.add_argument(
        "--gateway",
        help="Backend gateway URL (default: from <conf>/gateway.json)",
    )

    leave_p = sub.add_parser(
        "leave", help="Unsubscribe from a room (or all rooms if --room is omitted)"
    )
    leave_p.add_argument("alias", help="Participant alias")
    leave_p.add_argument("--room", help="Specific room to leave")

    sub.add_parser("status", help="Show participant status table")
    sub.add_parser("rooms", help="List all rooms")

    nudge_p = sub.add_parser("nudge", help="Force a manual nudge")
    nudge_p.add_argument("alias", help="Participant alias")
    nudge_p.add_argument("--room", help="Specific room (default: all subscribed rooms)")

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "join":
            return asyncio.run(cmd_join(args))
        if args.command == "leave":
            return asyncio.run(cmd_leave(args))
        if args.command == "status":
            return asyncio.run(cmd_status(args))
        if args.command == "rooms":
            return asyncio.run(cmd_rooms(args))
        if args.command == "nudge":
            return asyncio.run(cmd_nudge(args))
    except KeyboardInterrupt:
        print_err("interrupted")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())