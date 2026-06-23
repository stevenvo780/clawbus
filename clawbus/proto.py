from dataclasses import dataclass, field
import json
import re
import unicodedata
import uuid
from typing import Any, Optional

# Protocol Event Types
HELLO = "hello"
SUBSCRIBE = "subscribe"
UNSUBSCRIBE = "unsubscribe"
PUBLISH = "publish"
DELIVER = "deliver"
HEARTBEAT = "heartbeat"
STATUS = "status"
NUDGE = "nudge"

# Participant States
ALIVE = "alive"
WORKING = "working"
IDLE = "idle"
DEAD = "dead"

# Participant Backends
BACKEND_CLAUDE_CODE = "claude_code"
BACKEND_OPENCLAW = "openclaw"
BACKEND_OPENCODE = "opencode"

# Defaults & Constants
HOP_BUDGET = 3
DEFAULT_HOP_BUDGET = 3
BROKER_WS_PORT = 18791


@dataclass
class Room:
    name: str
    telegram_chat_id: int
    bot_token_ref: str
    allowlist_human: list[str] = field(default_factory=list)
    created_ts: float = field(default_factory=float)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "telegram_chat_id": self.telegram_chat_id,
            "bot_token_ref": self.bot_token_ref,
            "allowlist_human": list(self.allowlist_human),
            "created_ts": self.created_ts,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Room":
        return cls(
            name=data["name"],
            telegram_chat_id=data["telegram_chat_id"],
            bot_token_ref=data["bot_token_ref"],
            allowlist_human=list(data.get("allowlist_human", [])),
            created_ts=float(data.get("created_ts", 0.0)),
        )


@dataclass
class Participant:
    alias: str
    backend: str
    gateway: str
    rooms: list[str] = field(default_factory=list)
    state: str = ALIVE
    last_heartbeat_ts: float = field(default_factory=float)
    quota: dict[str, Any] = field(default_factory=dict)
    created_ts: float = field(default_factory=float)

    def to_dict(self) -> dict[str, Any]:
        return {
            "alias": self.alias,
            "backend": self.backend,
            "gateway": self.gateway,
            "rooms": list(self.rooms),
            "state": self.state,
            "last_heartbeat_ts": self.last_heartbeat_ts,
            "quota": dict(self.quota),
            "created_ts": self.created_ts,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Participant":
        return cls(
            alias=data["alias"],
            backend=data["backend"],
            gateway=data["gateway"],
            rooms=list(data.get("rooms", [])),
            state=data.get("state", ALIVE),
            last_heartbeat_ts=float(data.get("last_heartbeat_ts", 0.0)),
            quota=dict(data.get("quota", {})),
            created_ts=float(data.get("created_ts", 0.0)),
        )


@dataclass
class Subscription:
    participant: str
    room: str
    since: float = field(default_factory=float)

    def to_dict(self) -> dict[str, Any]:
        return {
            "participant": self.participant,
            "room": self.room,
            "since": self.since,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Subscription":
        return cls(
            participant=data["participant"],
            room=data["room"],
            since=float(data.get("since", 0.0)),
        )


@dataclass
class Message:
    id: str
    room: str
    from_: str  # Maps to 'from' in JSON
    to: list[str] = field(default_factory=list)
    kind: str = "text"
    body: str = ""
    mentions: list[str] = field(default_factory=list)
    in_reply_to: Optional[str] = None
    hop_budget: int = HOP_BUDGET
    hops_used: int = 0
    created_ts: float = field(default_factory=float)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "room": self.room,
            "from": self.from_,
            "to": list(self.to),
            "kind": self.kind,
            "body": self.body,
            "mentions": list(self.mentions),
            "in_reply_to": self.in_reply_to,
            "hop_budget": self.hop_budget,
            "hops_used": self.hops_used,
            "created_ts": self.created_ts,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Message":
        # Supports reading either 'from' or 'from_' key
        from_val = data.get("from") or data.get("from_")
        if from_val is None:
            raise KeyError("Message dict missing 'from' or 'from_' key")
        
        return cls(
            id=data["id"],
            room=data["room"],
            from_=from_val,
            to=list(data.get("to", [])),
            kind=data.get("kind", "text"),
            body=data.get("body", ""),
            mentions=list(data.get("mentions", [])),
            in_reply_to=data.get("in_reply_to"),
            hop_budget=int(data.get("hop_budget", HOP_BUDGET)),
            hops_used=int(data.get("hops_used", 0)),
            created_ts=float(data.get("created_ts", 0.0)),
        )


def new_msg_id() -> str:
    """Generates a UUIDv4-based simple 24-character hex message ID."""
    return uuid.uuid4().hex[:24]


def normalize_alias(text: str) -> str:
    """
    Applies alias normalization: NFKC + casefold + strips diacritics.
    Also strips a leading '@' if present, and trims whitespace.
    """
    if not text:
        return ""
    text = text.strip()
    if text.startswith('@'):
        text = text[1:]
        text = text.strip()
    
    # Apply NFKC normalization
    text = unicodedata.normalize('NFKC', text)
    # Apply casefold for case insensitivity
    text = text.casefold()
    # Strip diacritics (decompose character combinations and filter out combining marks)
    decomposed = unicodedata.normalize('NFD', text)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    return stripped.strip()


def parse_mentions(text: str) -> list[str]:
    """
    Parses mentions matching regex @([a-z0-9_-]{1,32}) case-insensitively,
    run over the normalized body (NFKC + case-fold + strip diacritics).
    Returns a list of normalized alias strings (deduplicated).
    """
    if not text:
        return []
    
    # Normalize the entire body first
    norm_text = unicodedata.normalize('NFKC', text)
    norm_text = norm_text.casefold()
    decomposed = unicodedata.normalize('NFD', norm_text)
    norm_text = "".join(c for c in decomposed if not unicodedata.combining(c))
    
    # Run regex on the normalized body
    matches = re.findall(r'@([a-z0-9_-]{1,32})(?![a-z0-9_-])', norm_text, re.IGNORECASE)
    
    seen = set()
    res = []
    for m in matches:
        normalized = normalize_alias(m)
        if normalized and normalized not in seen:
            seen.add(normalized)
            res.append(normalized)
    return res


def to_json(event: dict) -> str:
    """
    Serializes a dictionary event to a single line JSON string with a trailing newline.
    Ensures version 'v': 1 is present in the output.
    """
    if "v" not in event:
        event = {"v": 1, **event}
    return json.dumps(event, ensure_ascii=False) + "\n"


def from_json(line: str) -> dict:
    """
    Deserializes a single line JSON string and validates that version 'v' is 1.
    """
    line = line.strip()
    if not line:
        raise ValueError("Cannot parse empty line")
    data = json.loads(line)
    if not isinstance(data, dict):
        raise ValueError("Event must be a JSON object")
    if data.get("v") != 1:
        raise ValueError("Invalid event protocol version 'v': expected 1")
    return data
