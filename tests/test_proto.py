"""Tests de serialización y utilidades del protocolo clawbus."""

import json
import time
import pytest

from clawbus import (
    Room,
    Participant,
    Subscription,
    Message,
    HELLO,
    SUBSCRIBE,
    UNSUBSCRIBE,
    PUBLISH,
    DELIVER,
    HEARTBEAT,
    STATUS,
    ALIVE,
    WORKING,
    BACKEND_CLAUDE_CODE,
    HOP_BUDGET,
    DEFAULT_HOP_BUDGET,
    new_msg_id,
    parse_mentions,
    normalize_alias,
    to_json,
    from_json,
)


# ── Dataclass round-trip tests ──────────────────────────────────────────────


def test_room_roundtrip() -> None:
    """Room.to_dict() → Room.from_dict() debe preservar todos los campos."""
    room = Room(
        name="equipo",
        telegram_chat_id=-1001234567890,
        bot_token_ref="secretref:tg/equipo",
        allowlist_human=["alice", "bob"],
        created_ts=1700000000.0,
    )
    data = room.to_dict()
    restored = Room.from_dict(data)
    assert restored.name == room.name
    assert restored.telegram_chat_id == room.telegram_chat_id
    assert restored.bot_token_ref == room.bot_token_ref
    assert restored.allowlist_human == room.allowlist_human
    assert restored.created_ts == room.created_ts


def test_room_from_dict_defaults() -> None:
    """Room.from_dict() con campos opcionales ausentes debe usar defaults."""
    room = Room.from_dict({
        "name": "test",
        "telegram_chat_id": 0,
        "bot_token_ref": "v",
    })
    assert room.allowlist_human == []
    assert room.created_ts == 0.0


def test_participant_roundtrip() -> None:
    """Participant.to_dict() → Participant.from_dict() debe preservar campos."""
    p = Participant(
        alias="carol",
        backend=BACKEND_CLAUDE_CODE,
        gateway="ws",
        rooms=["equipo"],
        state=WORKING,
        last_heartbeat_ts=1700000001.0,
        quota={"turns": 5, "tokens": 1000},
        created_ts=1700000000.0,
    )
    data = p.to_dict()
    restored = Participant.from_dict(data)
    assert restored.alias == p.alias
    assert restored.backend == p.backend
    assert restored.gateway == p.gateway
    assert restored.rooms == p.rooms
    assert restored.state == p.state
    assert restored.last_heartbeat_ts == p.last_heartbeat_ts
    assert restored.quota == p.quota
    assert restored.created_ts == p.created_ts


def test_participant_from_dict_defaults() -> None:
    """Participant.from_dict() con campos ausentes debe usar defaults."""
    p = Participant.from_dict({
        "alias": "erin",
        "backend": "openclaw",
        "gateway": "ws",
    })
    assert p.rooms == []
    assert p.state == ALIVE
    assert p.last_heartbeat_ts == 0.0
    assert p.quota == {}
    assert p.created_ts == 0.0


def test_subscription_roundtrip() -> None:
    """Subscription.to_dict() → Subscription.from_dict() debe preservar campos."""
    sub = Subscription(
        participant="carol",
        room="equipo",
        since=1700000000.0,
    )
    data = sub.to_dict()
    restored = Subscription.from_dict(data)
    assert restored.participant == sub.participant
    assert restored.room == sub.room
    assert restored.since == sub.since


def test_subscription_from_dict_defaults() -> None:
    """Subscription.from_dict() sin 'since' → 0.0."""
    sub = Subscription.from_dict({"participant": "carol", "room": "equipo"})
    assert sub.since == 0.0


def test_message_roundtrip() -> None:
    """Message.to_dict() → Message.from_dict() debe preservar todos los campos,
    incluyendo el mapeo from_ ↔ 'from'."""
    msg = Message(
        id="abc123def456",
        room="equipo",
        from_="carol",
        to=["alice", "bob"],
        kind="text",
        body="hola @alice",
        mentions=["alice"],
        in_reply_to="xyz789",
        hop_budget=3,
        hops_used=1,
        created_ts=1700000000.0,
    )
    data = msg.to_dict()

    # El campo serializado debe llamarse "from", no "from_"
    assert "from" in data
    assert "from_" not in data
    assert data["from"] == "carol"

    restored = Message.from_dict(data)
    assert restored.id == msg.id
    assert restored.room == msg.room
    assert restored.from_ == msg.from_
    assert restored.to == msg.to
    assert restored.kind == msg.kind
    assert restored.body == msg.body
    assert restored.mentions == msg.mentions
    assert restored.in_reply_to == msg.in_reply_to
    assert restored.hop_budget == msg.hop_budget
    assert restored.hops_used == msg.hops_used
    assert restored.created_ts == msg.created_ts


def test_message_from_dict_accepts_from_key() -> None:
    """Message.from_dict() debe aceptar la clave 'from' (formato wire)."""
    data = {
        "id": "abc",
        "room": "equipo",
        "from": "carol",
        "body": "test",
    }
    msg = Message.from_dict(data)
    assert msg.from_ == "carol"


def test_message_from_dict_accepts_from_underscore_key() -> None:
    """Message.from_dict() debe aceptar la clave 'from_' (compatibilidad)."""
    data = {
        "id": "abc",
        "room": "equipo",
        "from_": "carol",
        "body": "test",
    }
    msg = Message.from_dict(data)
    assert msg.from_ == "carol"


def test_message_from_dict_missing_from_raises() -> None:
    """Message.from_dict() sin 'from' ni 'from_' debe lanzar KeyError."""
    data = {"id": "abc", "room": "equipo"}
    with pytest.raises(KeyError):
        Message.from_dict(data)


def test_message_default_hop_budget() -> None:
    """Message sin hop_budget explícito debe usar DEFAULT_HOP_BUDGET=3."""
    msg = Message(id="abc", room="equipo", from_="carol")
    assert msg.hop_budget == DEFAULT_HOP_BUDGET
    assert msg.hop_budget == 3

    # from_dict() sin clave hop_budget también debe usar el default
    msg2 = Message.from_dict({
        "id": "abc",
        "room": "equipo",
        "from": "carol",
    })
    assert msg2.hop_budget == HOP_BUDGET


def test_message_default_hops_used() -> None:
    """Message sin hops_used debe ser 0."""
    msg = Message(id="abc", room="equipo", from_="carol")
    assert msg.hops_used == 0


# ── JSONL round-trip tests por tipo de evento ───────────────────────────────


def _make_msg() -> Message:
    return Message(
        id=new_msg_id(),
        room="equipo",
        from_="bridge",
        body="@carol hola equipo",
        mentions=["carol"],
    )


def test_roundtrip_hello_event() -> None:
    """Hello event: to_json → from_json round-trip."""
    event = {
        "type": HELLO,
        "participant": "carol",
        "auth": "secret123",
        "backend": "claude_code",
        "gateway": "ws",
    }
    line = to_json(event)
    parsed = from_json(line)
    assert parsed["type"] == HELLO
    assert parsed["participant"] == "carol"
    assert parsed["v"] == 1


def test_roundtrip_subscribe_event() -> None:
    """Subscribe event: to_json → from_json round-trip."""
    event = {"type": SUBSCRIBE, "room": "equipo"}
    line = to_json(event)
    parsed = from_json(line)
    assert parsed["type"] == SUBSCRIBE
    assert parsed["room"] == "equipo"


def test_roundtrip_unsubscribe_event() -> None:
    """Unsubscribe event: to_json → from_json round-trip."""
    event = {"type": UNSUBSCRIBE, "room": "equipo"}
    line = to_json(event)
    parsed = from_json(line)
    assert parsed["type"] == UNSUBSCRIBE


def test_roundtrip_publish_event() -> None:
    """Publish event con Message anidado: to_json → from_json round-trip."""
    msg = _make_msg()
    event = {"type": PUBLISH, "room": "equipo", "msg": msg.to_dict()}
    line = to_json(event)
    parsed = from_json(line)
    assert parsed["type"] == PUBLISH
    assert parsed["room"] == "equipo"
    assert parsed["msg"]["from"] == "bridge"
    restored = Message.from_dict(parsed["msg"])
    assert restored.body == msg.body
    assert restored.mentions == msg.mentions


def test_roundtrip_deliver_event() -> None:
    """Deliver event con Message anidado: to_json → from_json round-trip."""
    msg = _make_msg()
    event = {"type": DELIVER, "room": "equipo", "msg": msg.to_dict()}
    line = to_json(event)
    parsed = from_json(line)
    assert parsed["type"] == DELIVER
    assert parsed["msg"]["from"] == "bridge"


def test_roundtrip_deliver_event_with_no_turn() -> None:
    """Deliver con no_turn=True debe persistir en round-trip."""
    msg = _make_msg()
    event = {"type": DELIVER, "room": "equipo", "msg": msg.to_dict(), "no_turn": True}
    line = to_json(event)
    parsed = from_json(line)
    assert parsed["no_turn"] is True


def test_roundtrip_heartbeat_event() -> None:
    """Heartbeat event: to_json → from_json round-trip."""
    event = {
        "type": HEARTBEAT,
        "state": WORKING,
        "quota": {"turns": 5},
    }
    line = to_json(event)
    parsed = from_json(line)
    assert parsed["type"] == HEARTBEAT
    assert parsed["state"] == WORKING
    assert parsed["quota"] == {"turns": 5}


def test_roundtrip_status_event() -> None:
    """Status event: to_json → from_json round-trip."""
    ts = time.time()
    event = {
        "type": STATUS,
        "ok": True,
        "rooms": ["equipo"],
        "participants": [
            {"alias": "carol", "state": ALIVE, "rooms": ["equipo"], "last_hb_s": 5, "quota": {}}
        ],
    }
    line = to_json(event)
    parsed = from_json(line)
    assert parsed["type"] == STATUS
    assert parsed["ok"] is True
    assert parsed["rooms"] == ["equipo"]
    assert len(parsed["participants"]) == 1


# ── Utilidades: to_json / from_json ─────────────────────────────────────────


def test_to_json_adds_version_when_missing() -> None:
    """to_json() debe agregar 'v': 1 si no está presente."""
    line = to_json({"type": HELLO})
    data = json.loads(line.strip())
    assert data["v"] == 1
    assert data["type"] == HELLO


def test_to_json_preserves_existing_version() -> None:
    """to_json() no debe duplicar 'v' si ya está presente."""
    line = to_json({"v": 1, "type": HELLO})
    data = json.loads(line.strip())
    assert data["v"] == 1


def test_to_json_output_is_single_line_with_newline() -> None:
    """to_json() produce exactamente una línea terminada en \\n."""
    line = to_json({"type": HELLO})
    assert line.endswith("\n")
    assert line.count("\n") == 1


def test_from_json_rejects_empty_line() -> None:
    """from_json() debe lanzar ValueError con línea vacía."""
    with pytest.raises(ValueError, match="empty"):
        from_json("")
    with pytest.raises(ValueError, match="empty"):
        from_json("  \n  ")


def test_from_json_rejects_non_dict() -> None:
    """from_json() debe lanzar ValueError si el JSON no es un objeto."""
    with pytest.raises(ValueError, match="object"):
        from_json("[1,2,3]")


def test_from_json_validates_version() -> None:
    """from_json() debe lanzar ValueError si 'v' != 1."""
    with pytest.raises(ValueError, match="version"):
        from_json('{"v": 2, "type": "hello"}')


def test_from_json_parses_valid_line() -> None:
    """from_json() debe parsear correctamente una línea JSONL válida."""
    data = from_json(to_json({"type": HELLO, "x": 42}))
    assert data["type"] == HELLO
    assert data["x"] == 42
    assert data["v"] == 1


# ── Utilidades: new_msg_id ─────────────────────────────────────────────────


def test_new_msg_id_length() -> None:
    """new_msg_id() debe retornar string de 24 caracteres hex."""
    mid = new_msg_id()
    assert len(mid) == 24
    assert all(c in "0123456789abcdef" for c in mid)


def test_new_msg_id_uniqueness() -> None:
    """IDs consecutivos deben ser distintos."""
    ids = {new_msg_id() for _ in range(100)}
    assert len(ids) == 100


# ── Utilidades: parse_mentions ──────────────────────────────────────────────


def test_parse_mentions_basic() -> None:
    """Extracción de @alias básicos."""
    result = parse_mentions("@carol hola @erin")
    assert result == ["carol", "erin"]


def test_parse_mentions_diacritics() -> None:
    """@josé debe normalizarse a 'jose'."""
    result = parse_mentions("@josé")
    assert result == ["jose"]


def test_parse_mentions_diacritics_complex() -> None:
    """@José_AliCe debe normalizarse correctamente."""
    result = parse_mentions("@José_AliCe")
    assert "jose_alice" in result


def test_parse_mentions_case_insensitive() -> None:
    """Menciones deben ser case-insensitive."""
    result = parse_mentions("@CAROL y @carol")
    assert result == ["carol"]  # deduplicado


def test_parse_mentions_deduplication() -> None:
    """@alias duplicados deben aparecer solo una vez."""
    result = parse_mentions("@carol @carol hola @CAROL")
    assert result == ["carol"]


def test_parse_mentions_empty() -> None:
    """Texto vacío → lista vacía."""
    assert parse_mentions("") == []
    assert parse_mentions("sin menciones aquí") == []


def test_parse_mentions_alias_boundaries() -> None:
    """Alias de 32 caracteres máximo, los más largos se ignoran."""
    alias_32 = "@" + "a" * 32
    alias_33 = "@" + "b" * 33
    result = parse_mentions(f"{alias_32} {alias_33}")
    assert "a" * 32 in result
    assert "b" * 33 not in result


def test_parse_mentions_underscore_and_hyphen() -> None:
    """Alias con guiones bajos y medios deben capturarse."""
    result = parse_mentions("@user_name @other-name")
    assert "user_name" in result
    assert "other-name" in result


def test_parse_mentions_numbers() -> None:
    """Alias con dígitos deben capturarse."""
    result = parse_mentions("@user123")
    assert result == ["user123"]


# ── Utilidades: normalize_alias ─────────────────────────────────────────────


def test_normalize_alias_strips_at() -> None:
    """@carol → carol."""
    assert normalize_alias("@carol") == "carol"


def test_normalize_alias_casefold() -> None:
    """CAROL → carol (casefold)."""
    assert normalize_alias("CAROL") == "carol"


def test_normalize_alias_diacritics_full() -> None:
    """José_AliCe → jose_alice (NFKC + casefold + strip diacríticos)."""
    result = normalize_alias("@José_AliCe")
    assert result == "jose_alice"


def test_normalize_alias_n_with_tilde() -> None:
    """Muñoz → munoz."""
    result = normalize_alias("Muñoz")
    assert result == "munoz"


def test_normalize_alias_trim_whitespace() -> None:
    """Whitespace alrededor debe trimarse."""
    assert normalize_alias("  @carol  ") == "carol"


def test_normalize_alias_empty() -> None:
    """String vacío → ''."""
    assert normalize_alias("") == ""


def test_normalize_alias_only_at() -> None:
    """Solo '@' → ''."""
    assert normalize_alias("@") == ""


def test_normalize_alias_german_umlauts() -> None:
    """Müller → muller."""
    result = normalize_alias("Müller")
    assert result == "muller"


# ── Constantes ──────────────────────────────────────────────────────────────


def test_constants_are_strings() -> None:
    """Los tipos de evento son strings esperados."""
    assert HELLO == "hello"
    assert SUBSCRIBE == "subscribe"
    assert UNSUBSCRIBE == "unsubscribe"
    assert PUBLISH == "publish"
    assert DELIVER == "deliver"
    assert HEARTBEAT == "heartbeat"
    assert STATUS == "status"


def test_state_constants() -> None:
    """Los estados son strings esperados."""
    assert ALIVE == "alive"
    assert WORKING == "working"


def test_hop_constants() -> None:
    """HOP_BUDGET y DEFAULT_HOP_BUDGET son 3."""
    assert HOP_BUDGET == 3
    assert DEFAULT_HOP_BUDGET == 3
