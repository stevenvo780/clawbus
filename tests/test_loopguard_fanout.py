#!/usr/bin/env python3
"""Test de regresión: un emisor debe poder dirigir mensajes a VARIOS destinatarios
seguidos (fan-out) sin que el loopguard encole/descarte los posteriores.

Reproduce el bug: Frank escribe a carol, dave, erin en ráfaga → solo el 1ro pasa.
"""
import sys
sys.path.insert(0, ".")
from clawbus.loopguard import LoopGuard  # noqa: E402


def test_fanout_multiples_destinatarios():
    lg = LoopGuard()
    room = "agentes"
    sender = "frank"
    results = []
    for i, target in enumerate(["carol", "dave", "erin"]):
        ok, reason = lg.validate(room, f"msg-{i}", sender, hop_budget=3)
        results.append((target, ok, reason))
    print("Resultados fan-out:")
    for t, ok, reason in results:
        print(f"  -> {t}: ok={ok} reason={reason!r}")
    passed = [r for r in results if r[1]]
    assert len(passed) == 3, (
        f"FALLO: solo {len(passed)}/3 publishes pasaron. "
        f"Los demás se encolan/descartan → hermanos no reciben.")
    print("OK: los 3 destinatarios reciben el mensaje.")


def test_dedup_sigue_funcionando():
    lg = LoopGuard()
    ok1, _ = lg.validate("agentes", "dup-1", "frank", 3)
    ok2, reason2 = lg.validate("agentes", "dup-1", "frank", 3)
    assert ok1 and not ok2 and reason2 == "dedup_hit", "dedup roto"
    print("OK: dedup intacto.")


def test_hop_budget_cero_no_genera_turno():
    lg = LoopGuard()
    ok, reason = lg.validate("agentes", "hb-0", "frank", hop_budget=0)
    assert not ok and reason == "hop_budget_zero", "hop_budget roto"
    print("OK: hop_budget=0 sigue frenando turnos (anti-loop intacto).")


def test_spam_cap_30_min():
    lg = LoopGuard()
    drops = 0
    for i in range(40):
        ok, reason = lg.validate("agentes", f"spam-{i}", "flooder", 3)
        if not ok and reason == "dropped":
            drops += 1
    assert drops > 0, "el tope anti-spam de 30/min debería frenar un flood de 40"
    print(f"OK: tope 30/min frena el flood ({drops} dropped de 40).")


if __name__ == "__main__":
    test_fanout_multiples_destinatarios()
    test_dedup_sigue_funcionando()
    test_hop_budget_cero_no_genera_turno()
    test_spam_cap_30_min()
    print("\nTODOS LOS TESTS PASARON")
