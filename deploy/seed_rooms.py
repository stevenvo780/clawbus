"""Siembra los rooms de clawbus en el registry. Idempotente.

Lee CLAWBUS_STATE_DIR (default /var/lib/clawbus) y los chat_ids por env:
  CLAWBUS_EQUIPO_CHAT_ID  (default 0 = grupo Acme)
  CLAWBUS_BOB_CHAT_ID  (0 = aún no creado -> se omite)
"""
import os
import time

from clawbus.proto import Room
from clawbus.registry import Registry


def main() -> None:
    reg = Registry(os.getenv("CLAWBUS_STATE_DIR", "/var/lib/clawbus"))
    equipo_chat = int(os.getenv("CLAWBUS_EQUIPO_CHAT_ID", "0"))
    bob_chat = int(os.getenv("CLAWBUS_BOB_CHAT_ID", "0"))

    # 'agentes' = room inter-agente (lo usan connectors, plugin-canal y clawbus_ask);
    # es el que NO debe faltar nunca. Sin espejo de Telegram (chat_id 0).
    if not reg.get_room("agentes"):
        reg.add_or_update_room(Room("agentes", 0, "", [], time.time()))
    reg.add_or_update_room(
        Room("equipo", equipo_chat, "secretref:clawbus/bot-equipo", ["alice", "bob"], time.time())
    )
    if bob_chat:
        reg.add_or_update_room(
            Room("bob", bob_chat, "secretref:clawbus/bot-bob", ["bob"], time.time())
        )

    print("rooms sembrados:", {n: r.telegram_chat_id for n, r in reg.rooms.items()})


if __name__ == "__main__":
    main()
