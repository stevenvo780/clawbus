"""Punto de entrada del paquete: `python -m clawbus <broker|bridge|...>`.

Despacha al subcomando correcto. Usado por el Dockerfile/compose y el deploy:
  python -m clawbus broker
  python -m clawbus bridge equipo
  python -m clawbus join <alias> --room <room>   (y demás del CLI)
"""
import asyncio
import os
import sys


def main() -> None:
    argv = sys.argv[1:]
    cmd = argv[0] if argv else None

    if cmd == "broker":
        from clawbus import broker
        asyncio.run(broker.main())
    elif cmd == "bridge":
        # El room puede venir como argumento posicional o por env CLAWBUS_BRIDGE_ROOM.
        if len(argv) > 1:
            os.environ.setdefault("CLAWBUS_BRIDGE_ROOM", argv[1])
        from clawbus import bridge
        bridge.main()
    else:
        # Resto -> CLI (join/leave/status/rooms/nudge).
        from clawbus import cli
        raise SystemExit(cli.main(argv))


if __name__ == "__main__":
    main()
