import json
import os
import time
from pathlib import Path
from typing import Optional
from clawbus import Room, Participant, Subscription


class Registry:
    """
    Carga/guardado atómico de registry.json.

    Path: env CLAWBUS_STATE_DIR / "registry.json" (default /workspace/clawbus/.state/registry.json)

    Write-then-rename: escribe a .tmp, luego rename (atómico en POSIX).
    """

    def __init__(self, state_dir: Optional[str] = None):
        if state_dir is None:
            state_dir = os.getenv("CLAWBUS_STATE_DIR", "/workspace/clawbus/.state")

        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.registry_path = self.state_dir / "registry.json"

        # In-memory state
        self.rooms: dict[str, Room] = {}
        self.participants: dict[str, Participant] = {}
        self.subscriptions: list[Subscription] = []

        self._load()

    def _load(self):
        """Carga el registry.json si existe, si no inicia vacío."""
        if self.registry_path.exists():
            with open(self.registry_path, "r") as f:
                data = json.load(f)

            # Load rooms
            for name, room_dict in data.get("rooms", {}).items():
                self.rooms[name] = Room.from_dict(room_dict)

            # Load participants
            for alias, part_dict in data.get("participants", {}).items():
                self.participants[alias] = Participant.from_dict(part_dict)

            # Load subscriptions
            for sub_dict in data.get("subscriptions", []):
                self.subscriptions.append(Subscription.from_dict(sub_dict))

    def _save(self):
        """Guarda el registry atómicamente (write-then-rename)."""
        data = {
            "rooms": {name: room.to_dict() for name, room in self.rooms.items()},
            "participants": {alias: p.to_dict() for alias, p in self.participants.items()},
            "subscriptions": [sub.to_dict() for sub in self.subscriptions],
        }

        # Write to temporary file
        tmp_path = self.registry_path.parent / f"{self.registry_path.name}.tmp"
        with open(tmp_path, "w") as f:
            json.dump(data, f, indent=2)

        # Atomic rename
        tmp_path.replace(self.registry_path)

    # Métodos CRUD

    def add_or_update_room(self, room: Room):
        """Agrega o actualiza una room."""
        self.rooms[room.name] = room
        self._save()

    def remove_room(self, name: str):
        """Remueve una room."""
        if name in self.rooms:
            del self.rooms[name]
            self._save()

    def get_room(self, name: str) -> Optional[Room]:
        """Obtiene una room por nombre."""
        return self.rooms.get(name)

    def add_or_update_participant(self, participant: Participant):
        """Agrega o actualiza un participant."""
        self.participants[participant.alias] = participant
        self._save()

    def remove_participant(self, alias: str):
        """Remueve un participant."""
        if alias in self.participants:
            del self.participants[alias]
            # También borrar sus suscripciones
            self.subscriptions = [s for s in self.subscriptions if s.participant != alias]
            self._save()

    def get_participant(self, alias: str) -> Optional[Participant]:
        """Obtiene un participant por alias."""
        return self.participants.get(alias)

    def add_subscription(self, subscription: Subscription):
        """Agrega una suscripción."""
        # Evitar duplicados
        if not any(s.participant == subscription.participant and s.room == subscription.room
                   for s in self.subscriptions):
            self.subscriptions.append(subscription)
            self._save()

    def remove_subscription(self, participant: str, room: str):
        """Remueve una suscripción."""
        original_len = len(self.subscriptions)
        self.subscriptions = [s for s in self.subscriptions
                             if not (s.participant == participant and s.room == room)]
        if len(self.subscriptions) < original_len:
            self._save()

    def get_subscriptions_by_room(self, room: str) -> list[Subscription]:
        """Obtiene todas las suscripciones de un room."""
        return [s for s in self.subscriptions if s.room == room]

    def get_subscriptions_by_participant(self, participant: str) -> list[Subscription]:
        """Obtiene todas las suscripciones de un participant."""
        return [s for s in self.subscriptions if s.participant == participant]
