#!/usr/bin/env python3
"""Tests del enrutado de salida del oc_connector: convertir la respuesta libre del
agente OpenClaw en mensajes dirigidos del bus (convención '@alias: …')."""
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "integrations", "openclaw"))
from connector import parse_outbound  # noqa: E402

ROSTER = {"frank", "grace", "carol", "erin"}


def case(desc, reply, sender, expected):
    got = parse_outbound(reply, ROSTER, sender)
    flag = "OK " if got == expected else "FALLO"
    print(f"  [{flag}] {desc}")
    if got != expected:
        print(f"        got={got}\n        exp={expected}")
    assert got == expected, desc


def run():
    print("Enrutado de salida (parse_outbound):")
    # respuesta normal → va al que escribió
    case("solo respuesta al sender", "Sí, ya terminé.", "frank",
         [("frank", "Sí, ya terminé.")])
    # dirigirse a otro hermano → enruta a ese hermano
    case("dirige a erin", "@erin: pasame el reporte", "frank",
         [("erin", "pasame el reporte")])
    # combinado: texto al sender + directiva a otro
    case("sender + directiva", "Ok frank, lo veo.\n@carol: ¿lo desplegaste?", "frank",
         [("frank", "Ok frank, lo veo."), ("carol", "¿lo desplegaste?")])
    # varias directivas
    case("dos directivas", "@erin: hola\n@grace: ping", "frank",
         [("erin", "hola"), ("grace", "ping")])
    # alias desconocido → NO enruta, queda como texto al sender
    case("alias fuera del roster → texto", "@pepe: hola", "frank",
         [("frank", "@pepe: hola")])
    # un '@' en medio de texto no es directiva
    case("@ en medio no es directiva", "avisale a @erin que sí", "frank",
         [("frank", "avisale a @erin que sí")])
    # respuesta vacía → nada
    case("vacío → nada", "", "frank", [])
    print("\nTODOS LOS TESTS PASARON")


if __name__ == "__main__":
    run()
