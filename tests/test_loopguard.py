"""Tests del LoopGuard: dedup LRU, hop budget y rate-limit."""

import time
import pytest

from clawbus.loopguard import LoopGuard


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def lg() -> LoopGuard:
    """LoopGuard con defaults (10k dedup, 16 queue)."""
    return LoopGuard()


@pytest.fixture
def lg_small_dedup() -> LoopGuard:
    """LoopGuard con LRU pequeño para testear evicción."""
    return LoopGuard(max_dedup_per_room=3)


@pytest.fixture
def lg_small_queue() -> LoopGuard:
    """LoopGuard con cola pequeña para testear cola llena."""
    return LoopGuard(max_rate_limit_queue=2)


# ── Dedup LRU tests ────────────────────────────────────────────────────────


def test_dedup_first_pass(lg: LoopGuard) -> None:
    """El primer mensaje debe pasar la validación."""
    ok, reason = lg.validate("room_a", "msg_1", "user_x", hop_budget=1)
    assert ok is True
    assert reason == ""


def test_dedup_second_hit(lg: LoopGuard) -> None:
    """El mismo msg_id en el mismo room debe ser rechazado como duplicado."""
    lg.validate("room_a", "msg_1", "user_x", hop_budget=1)
    ok, reason = lg.validate("room_a", "msg_1", "user_x", hop_budget=1)
    assert ok is False
    assert reason == "dedup_hit"


def test_dedup_same_id_different_room(lg: LoopGuard) -> None:
    """Mismo msg_id en rooms distintos NO es duplicado."""
    lg.validate("room_a", "msg_1", "user_x", hop_budget=1)
    ok, reason = lg.validate("room_b", "msg_1", "user_x", hop_budget=1)
    assert ok is True
    assert reason == ""


def test_dedup_lru_eviction(lg_small_dedup: LoopGuard) -> None:
    """Cuando el LRU excede el máximo, el mensaje más antiguo se evicta (FIFO)."""
    lg = lg_small_dedup  # max_dedup_per_room=3

    lg.validate("room", "msg_0", "a", hop_budget=1)  # más antiguo
    lg.validate("room", "msg_1", "b", hop_budget=1)
    lg.validate("room", "msg_2", "c", hop_budget=1)

    assert len(lg.dedup_lru["room"]) == 3
    assert "msg_0" in lg.dedup_lru["room"]

    # 4º insert → evicta el más antiguo (msg_0)
    lg.validate("room", "msg_3", "d", hop_budget=1)

    assert len(lg.dedup_lru["room"]) == 3
    assert "msg_0" not in lg.dedup_lru["room"], "msg_0 debería haber sido evictado"
    assert "msg_1" in lg.dedup_lru["room"]
    assert "msg_2" in lg.dedup_lru["room"]
    assert "msg_3" in lg.dedup_lru["room"]


def test_dedup_lru_eviction_order(lg_small_dedup: LoopGuard) -> None:
    """La evicción respeta orden de inserción: el primero en entrar sale primero."""
    lg = lg_small_dedup

    lg.validate("room", "first", "a", hop_budget=1)
    lg.validate("room", "second", "b", hop_budget=1)
    lg.validate("room", "third", "c", hop_budget=1)

    # Insertar 4º → evicta "first"
    lg.validate("room", "fourth", "d", hop_budget=1)

    assert "first" not in lg.dedup_lru["room"]
    assert "second" in lg.dedup_lru["room"]
    assert "third" in lg.dedup_lru["room"]
    assert "fourth" in lg.dedup_lru["room"]

    # Insertar 5º → evicta "second" (ahora el más antiguo)
    lg.validate("room", "fifth", "e", hop_budget=1)

    assert "second" not in lg.dedup_lru["room"]
    assert "third" in lg.dedup_lru["room"]
    assert "fourth" in lg.dedup_lru["room"]
    assert "fifth" in lg.dedup_lru["room"]


# ── Hop budget tests ───────────────────────────────────────────────────────


def test_hop_budget_positive_passes(lg: LoopGuard) -> None:
    """hop_budget=1 debe pasar la validación."""
    ok, reason = lg.validate("room", "msg_1", "user", hop_budget=1)
    assert ok is True
    assert reason == ""


def test_hop_budget_zero_fails(lg: LoopGuard) -> None:
    """hop_budget=0 debe fallar con 'hop_budget_zero'."""
    ok, reason = lg.validate("room", "msg_1", "user", hop_budget=0)
    assert ok is False
    assert reason == "hop_budget_zero"


def test_hop_budget_negative_fails(lg: LoopGuard) -> None:
    """hop_budget=-1 debe fallar con 'hop_budget_zero'."""
    ok, reason = lg.validate("room", "msg_1", "user", hop_budget=-1)
    assert ok is False
    assert reason == "hop_budget_zero"


def test_hop_budget_three_passes(lg: LoopGuard) -> None:
    """hop_budget=3 (el default) debe pasar."""
    ok, reason = lg.validate("room", "msg_1", "user", hop_budget=3)
    assert ok is True


def test_hop_budget_zero_is_deduped_before_rejection(lg: LoopGuard) -> None:
    """hop_budget=0 se registra en dedup ANTES de ser rechazado por hop_budget_zero."""
    lg.validate("room", "msg_1", "user", hop_budget=0)  # rechazado por hop_budget=0
    # Se insertó en dedup antes del chequeo de hop_budget
    ok, reason = lg.validate("room", "msg_1", "user", hop_budget=1)
    assert ok is False
    assert reason == "dedup_hit"


# ── Rate-limit tests ───────────────────────────────────────────────────────


def test_rate_limit_first_turn_passes(lg: LoopGuard) -> None:
    """El primer turno de un participante en un room pasa sin problemas."""
    ok, reason = lg.validate("room", "msg_1", "user_x", hop_budget=1)
    assert ok is True
    assert reason == ""


def test_rate_limit_different_participants_independent(lg: LoopGuard) -> None:
    """Participantes distintos en el mismo room NO comparten working slot."""
    lg.validate("room", "msg_a", "user_a", hop_budget=1)  # user_a working
    ok, reason = lg.validate("room", "msg_b", "user_b", hop_budget=1)  # user_b intenta
    assert ok is True  # user_b puede tomar su propio turno
    assert reason == ""


def test_mark_done_no_queue_returns_none(lg: LoopGuard) -> None:
    """mark_done() sin cola retorna None."""
    lg.validate("room", "msg_1", "user", hop_budget=1)
    assert lg.mark_done("room", "user") is None


def test_mark_done_unknown_room_returns_none(lg: LoopGuard) -> None:
    """mark_done() en room/participant desconocido retorna None."""
    assert lg.mark_done("nonexistent", "user") is None


def test_rate_limit_30_per_min_drops_after_limit(lg: LoopGuard) -> None:
    """Después de 30 turnos en 60s, los nuevos se dropean."""
    # Iniciar y completar 30 turnos
    for i in range(30):
        ok, reason = lg.validate("room", f"msg_{i:02d}", "user", hop_budget=1)
        assert ok is True, f"Turno {i} debería pasar: {reason}"
        # Completar el turno inmediatamente
        result = lg.mark_done("room", "user")
        # El primer mark_done retorna None (sin cola), los siguientes también
        # porque cada validate empieza turno nuevo con working=False
        assert result is None

    # El turno 31 debe dropearse por cuota 30/min
    ok, reason = lg.validate("room", "msg_overflow", "user", hop_budget=1)
    assert ok is False
    assert reason == "dropped"


def test_rate_limit_quota_resets_after_window(lg: LoopGuard) -> None:
    """Simular turnos antiguos fuera de la ventana de 60s."""
    # Insertar 30 turnos con timestamp antiguo (fuera de ventana)
    old_time = time.time() - 120  # 120 segundos atrás
    if "room" not in lg.rate_limit:
        lg.rate_limit["room"] = {}
    if "user" not in lg.rate_limit["room"]:
        lg.rate_limit["room"]["user"] = {
            "working": False,
            "quota_min": [(1, old_time) for _ in range(30)],
            "queue": [],
        }

    # La limpieza de quota_min al validar debe remover los viejos
    ok, reason = lg.validate("room", "msg_new", "user", hop_budget=1)
    assert ok is True  # Debe pasar porque los turnos viejos se limpiaron
    assert reason == ""


def test_rate_limit_per_room_isolation(lg: LoopGuard) -> None:
    """Rate-limit de un room no afecta a otro."""
    # Tomar turno en room_a
    lg.validate("room_a", "msg_1", "user", hop_budget=1)

    # El mismo user en room_b debería poder tomar turno
    ok, reason = lg.validate("room_b", "msg_x", "user", hop_budget=1)
    assert ok is True


# ── Combined scenarios ──────────────────────────────────────────────────────


def test_dedup_before_rate_limit(lg_small_queue: LoopGuard) -> None:
    """Dedup se chequea antes que rate-limit: duplicado → dedup_hit, no se encola."""
    lg = lg_small_queue

    lg.validate("room", "msg_1", "user", hop_budget=1)
    lg.validate("room", "msg_2", "user", hop_budget=1)  # encolado

    # Reenviar msg_1 → debe ser dedup_hit, no rate_limit_exceeded
    ok, reason = lg.validate("room", "msg_1", "user", hop_budget=1)
    assert ok is False
    assert reason == "dedup_hit"


def test_hop_budget_before_rate_limit(lg: LoopGuard) -> None:
    """hop_budget_zero se chequea antes que rate-limit."""
    lg.validate("room", "msg_1", "user", hop_budget=1)  # working=True

    # hop_budget=0 debería dar hop_budget_zero aunque esté rate-limited
    ok, reason = lg.validate("room", "msg_x", "user", hop_budget=0)
    assert ok is False
    assert reason == "hop_budget_zero"
    # Además NO debe encolarse: la cola debería seguir vacía
    assert lg.mark_done("room", "user") is None
