#!/usr/bin/env python3
"""clawbus-openclaw-connector — pone un agente OpenClaw (Dave/Erin) en el bus clawbus.

Uso: oc_connector.py <alias> <container> [--room agentes] [--broker ws://clawbus-broker:18791]

Suscribe al broker como <alias>. Cada mensaje del bus que lo @mencione, le sea dirigido
o sea broadcast corre un turno OpenClaw (docker exec <container> openclaw agent ... --json).

El puente le ENSEÑA el bus al agente OpenClaw en cada turno: le inyecta el roster en vivo
de hermanos y la convención para dirigirse a cualquiera ('@alias: mensaje'). Luego ENRUTA
la respuesta: las líneas '@alias: …' van al hermano nombrado; el resto vuelve a quien
escribió. Así Dave/Erin participan del bus (no solo reflejo) sin tocar OpenClaw.
"""
import argparse
import asyncio
import json
import re
import subprocess
import sys

import websockets  # noqa: E402
from clawbus import proto  # noqa: E402

_DIRECTIVE = re.compile(r"^@([A-Za-z0-9_-]+):\s*(.+)$")


def _run_turn_sync(container: str, alias: str, room: str, frm: str, body: str,
                   roster: list) -> str:
    hermanos = ", ".join(roster) if roster else "(ninguno detectado aún)"
    msg = (
        f"[bus clawbus] Sos '{alias}', conectado a un bus inter-agente (room '{room}'). "
        f"Tus hermanos en el bus ahora: {hermanos}.\n"
        f"{frm} te escribió: {body}\n\n"
        f"Cómo responder por el bus:\n"
        f"- Para contestarle a {frm}: escribí normal.\n"
        f"- Para dirigirte a OTRO hermano: agregá una línea que empiece con "
        f"'@<alias>: <mensaje>' (ej '@erin: ¿estás ahí?').\n"
        f"- Podés combinar (texto para {frm} + líneas @alias para otros). Sé conciso."
    )
    cmd = ["docker", "exec", container, "openclaw", "agent",
           "--agent", "main", "--session-key", "clawbus", "-m", msg, "--json"]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=200)
        d = json.loads(p.stdout)
        res = d.get("result", {})
        payloads = res.get("payloads") or []
        if payloads and payloads[0].get("text"):
            return payloads[0]["text"]
        return res.get("meta", {}).get("finalAssistantVisibleText", "") or "(sin respuesta)"
    except Exception as e:  # noqa: BLE001
        return f"(error turno OpenClaw: {e})"


def is_for_me(alias: str, m: dict) -> bool:
    """¿El mensaje del bus es para este agente? Iguala la semántica del canal Claude
    (server.ts): broadcast (to vacío) = para TODOS; si no, requiere estar en `to` o en
    `mentions`. Nunca responde a los propios. Sin esto, los OpenClaw ignoraban los
    broadcasts ('escribile a todos') mientras los Claude Code sí respondían."""
    if m.get("from") == alias:
        return False
    to = m.get("to") or []
    mentions = m.get("mentions") or []
    if len(to) == 0:  # broadcast → para todos los suscriptores del room
        return True
    return alias in to or alias in mentions


def parse_outbound(reply: str, roster: set, sender: str) -> list:
    """Convierte la respuesta libre del agente OpenClaw en mensajes dirigidos del bus.

    - Líneas '@alias: texto' con alias EN el roster → mensaje a ese hermano.
    - El resto del texto → mensaje a `sender` (la respuesta por defecto).
    Devuelve [(target, body), ...]. Una directiva a un alias desconocido se deja como
    texto normal (no se pierde, va al sender) para no enrutar a la nada.
    """
    directed = []
    rest = []
    for line in (reply or "").splitlines():
        mt = _DIRECTIVE.match(line.strip())
        if mt and mt.group(1) in roster:
            directed.append((mt.group(1), mt.group(2).strip()))
        else:
            rest.append(line)
    out = []
    rest_text = "\n".join(rest).strip()
    if rest_text:
        out.append((sender, rest_text))
    out.extend(directed)
    if not out and (reply or "").strip():
        out.append((sender, reply.strip()))
    return out


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("alias")
    ap.add_argument("container")
    ap.add_argument("--room", default="agentes")
    ap.add_argument("--broker", default="ws://clawbus-broker:18791")
    a = ap.parse_args()

    roster: set = set()  # hermanos vivos en el bus (aprendido de STATUS + tráfico)

    while True:
        try:
            async with websockets.connect(a.broker) as ws:
                await ws.send(proto.to_json({"type": proto.HELLO, "participant": a.alias,
                                             "auth": a.alias, "rooms": [a.room],
                                             "backend": "openclaw", "gateway": a.container}))
                await ws.send(proto.to_json({"type": proto.SUBSCRIBE, "room": a.room}))
                await ws.send(proto.to_json({"type": proto.STATUS}))  # pedir roster inicial

                async def heartbeat():
                    while True:
                        await asyncio.sleep(15)
                        try:
                            await ws.send(proto.to_json({"type": proto.HEARTBEAT,
                                                         "participant": a.alias,
                                                         "state": "alive", "quota": {}}))
                            await ws.send(proto.to_json({"type": proto.STATUS}))  # refrescar roster
                        except Exception:  # noqa: BLE001
                            return
                asyncio.create_task(heartbeat())
                sys.stderr.write(f"[oc-connector] {a.alias} ({a.container}) conectado al bus\n")
                sys.stderr.flush()

                async for raw in ws:
                    for line in str(raw).split("\n"):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            ev = json.loads(line)
                        except Exception:  # noqa: BLE001
                            continue
                        et = ev.get("type")
                        if et == proto.STATUS:
                            # Roster = SOLO hermanos vivos (el broker acumula muertos de
                            # tests viejos). Se reconstruye en cada STATUS (cada 15s);
                            # entre medio, el tráfico agrega emisores activos.
                            fresh = {p.get("alias") for p in ev.get("participants", [])
                                     if p.get("alias") and p.get("alias") != a.alias
                                     and p.get("state") == "alive"}
                            roster.clear()
                            roster.update(fresh)
                            continue
                        if et != proto.DELIVER:
                            continue
                        m = ev.get("msg") or {}
                        frm = m.get("from")
                        if frm and frm != a.alias:
                            roster.add(frm)  # aprende quién está en el bus
                        if not is_for_me(a.alias, m):
                            continue  # propios / dirigidos a otro (broadcast SÍ pasa)
                        if m.get("hop_budget", 3) <= 0:
                            continue

                        async def handle(m=m, frm=frm):
                            reply = await asyncio.get_event_loop().run_in_executor(
                                None, _run_turn_sync, a.container, a.alias, a.room,
                                frm, m.get("body"), sorted(roster))
                            if not reply:
                                return
                            budget = max(0, m.get("hop_budget", 3) - 1)
                            hops = m.get("hops_used", 0) + 1
                            for target, text in parse_outbound(reply, roster, frm):
                                r = proto.Message(proto.new_msg_id(), a.room, a.alias,
                                                  [target], "text", text,
                                                  [target], m.get("id"), budget, hops, 0.0)
                                await ws.send(proto.to_json({"type": proto.PUBLISH,
                                                             "room": a.room, "msg": r.to_dict()}))
                                sys.stderr.write(f"[oc-connector] {a.alias} -> {target}\n")
                                sys.stderr.flush()
                        asyncio.create_task(handle())
        except Exception as e:  # noqa: BLE001
            sys.stderr.write(f"[oc-connector] {a.alias} reconnect: {e}\n")
            sys.stderr.flush()
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
