#!/usr/bin/env python3
"""Tests del matching de direccionamiento del oc_connector (dave/erin).

Cubre las rutas por las que un agente puede ser contactado, incluida la que
fallaba: BROADCAST (to vacío = 'escribile a todos').
"""
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "integrations", "openclaw"))
from connector import is_for_me  # noqa: E402


def case(desc, alias, msg, expected):
    got = is_for_me(alias, msg)
    flag = "OK " if got == expected else "FALLO"
    print(f"  [{flag}] {desc}: is_for_me({alias!r}) = {got} (esperado {expected})")
    assert got == expected, f"{desc}: esperado {expected}, got {got}"


def run():
    print("Matching del oc_connector:")
    # EL CASO QUE FALLABA: broadcast (to vacío, sin mentions) → para todos
    case("broadcast a dave", "dave",
         {"from": "frank", "to": [], "mentions": []}, True)
    case("broadcast a erin", "erin",
         {"from": "frank", "to": [], "mentions": []}, True)
    # broadcast con 'to' ausente (no solo vacío)
    case("broadcast (sin campo to)", "dave",
         {"from": "frank"}, True)
    # dirigido a mí explícito
    case("dirigido a dave", "dave",
         {"from": "frank", "to": ["dave"], "mentions": ["dave"]}, True)
    # dirigido a varios (incluido yo) — el fan-out del fix anterior
    case("dirigido a carol+dave+erin", "erin",
         {"from": "frank", "to": ["carol", "dave", "erin"],
          "mentions": ["carol", "dave", "erin"]}, True)
    # dirigido SOLO a otro → NO me toca
    case("dirigido solo a carol", "dave",
         {"from": "frank", "to": ["carol"], "mentions": ["carol"]}, False)
    # mención sin estar en 'to'
    case("mencionado en body", "dave",
         {"from": "frank", "to": ["carol"], "mentions": ["carol", "dave"]}, True)
    # mi propio mensaje → nunca
    case("mi propio mensaje", "dave",
         {"from": "dave", "to": [], "mentions": []}, False)
    # mi propio broadcast → nunca (anti-loop)
    case("mi propio broadcast", "erin",
         {"from": "erin", "to": [], "mentions": []}, False)
    print("\nTODOS LOS TESTS PASARON")


if __name__ == "__main__":
    run()
