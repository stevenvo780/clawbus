import time
from collections import OrderedDict
from typing import Optional, Tuple


class LoopGuard:
    """
    Valida publish() eventos contra dedup (LRU 10k/room), rate-limit (tope 30/min por
    emisor; SIN serialización de 1 turno — rompía el fan-out multi-destinatario), y
    presupuesto de saltos (HOP_BUDGET=3, el anti-loop real).
    """

    def __init__(self, max_dedup_per_room: int = 10000, max_rate_limit_queue: int = 16):
        self.dedup_lru = {}  # { room: OrderedDict(msg_id -> ts) }
        self.max_dedup_per_room = max_dedup_per_room

        # Rate-limit: { room: { participant: { "working": bool, "quota_min": int, "queue": [msg_id, ...] } } }
        self.rate_limit = {}
        self.max_queue = max_rate_limit_queue

    def validate(self, room: str, msg_id: str, participant: str, hop_budget: int) -> Tuple[bool, str]:
        """
        Valida un mensaje publish.

        Retorna: (ok: bool, reason: str)
        - (True, "") → pasa todas las validaciones
        - (False, "dedup_hit") → msg_id ya visto en este room
        - (False, "hop_budget_zero") → hop_budget=0, no generar turno (pero sí entregar con flag no_turn=True)
        - (False, "dropped") → el emisor superó 30 publishes/min en el room
        """

        # 1. Dedup check
        if room not in self.dedup_lru:
            self.dedup_lru[room] = OrderedDict()

        if msg_id in self.dedup_lru[room]:
            return False, "dedup_hit"

        # Add to dedup (evict oldest if size exceeded)
        self.dedup_lru[room][msg_id] = time.time()
        if len(self.dedup_lru[room]) > self.max_dedup_per_room:
            self.dedup_lru[room].popitem(last=False)

        # 2. Hop budget check
        if hop_budget <= 0:
            return False, "hop_budget_zero"

        # 3. Rate-limit check
        if room not in self.rate_limit:
            self.rate_limit[room] = {}
        if participant not in self.rate_limit[room]:
            self.rate_limit[room][participant] = {
                "working": False,
                "quota_min": [],  # [ (turn_count, ts), ... ] para 30/min
                "queue": []
            }

        state = self.rate_limit[room][participant]

        # Limpiar quota_min (quitar turnos > 60s)
        now = time.time()
        state["quota_min"] = [(cnt, ts) for cnt, ts in state["quota_min"] if now - ts < 60]

        turn_count = sum(cnt for cnt, ts in state["quota_min"])

        # Rate-limit: tope duro 30/min por (room, emisor) como anti-spam/anti-loop.
        # Se QUITÓ la serialización "1 turno simultáneo": antes marcaba working=True
        # en el primer publish y encolaba+DESCARTABA todos los siguientes del mismo
        # emisor (rate_limit_exceeded → el broker respondía "queued" y nunca hacía
        # fan-out). Eso rompía el caso multi-destinatario: un agente que le escribe a
        # varios hermanos en ráfaga solo lograba entregar al PRIMERO. El anti-loop real
        # es hop_budget (se decrementa por salto, ver arriba) + este tope 30/min.
        if turn_count >= 30:
            return False, "dropped"

        state["quota_min"].append((1, now))

        return True, ""

    def mark_done(self, room: str, participant: str) -> Optional[str]:
        """
        Marca un turno como done. Retorna el siguiente msg_id de la cola, o None.
        """
        if room not in self.rate_limit or participant not in self.rate_limit[room]:
            return None

        state = self.rate_limit[room][participant]
        state["working"] = False

        if state["queue"]:
            next_msg_id = state["queue"].pop(0)
            state["working"] = True
            state["quota_min"].append((1, time.time()))
            return next_msg_id

        return None
