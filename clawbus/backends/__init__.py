"""clawbus.backends — Agent backends interface and factory."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from clawbus import (
    BACKEND_CLAUDE_CODE,
    BACKEND_OPENCLAW,
    BACKEND_OPENCODE,
)


class Backend(ABC):
    """Abstract base for agent turn backends."""

    def __init__(self, gateway: str, participant_alias: str) -> None:
        self.gateway: str = gateway
        self.participant_alias: str = participant_alias

    @abstractmethod
    async def run_turn(self, text: str, session_id: str) -> str:
        """Execute one agent turn.

        Args:
            text: Prompt/message to process.
            session_id: Persistent session identifier per subscription.

        Returns:
            Agent response text.

        Raises:
            TimeoutError: If the turn exceeds the configured timeout.
            ConnectionError: If the backend gateway is unreachable.
        """
        ...


def create_backend(
    backend_type: str, gateway: str, participant_alias: str
) -> Backend:
    """Factory: returns a Backend instance for the given type."""
    if backend_type == BACKEND_CLAUDE_CODE:
        from clawbus.backends.claude_code import ClaudeCodeBackend
        return ClaudeCodeBackend(gateway, participant_alias)
    if backend_type == BACKEND_OPENCLAW:
        from clawbus.backends.openclaw import OpenClawBackend
        return OpenClawBackend(gateway, participant_alias)
    if backend_type == BACKEND_OPENCODE:
        from clawbus.backends.opencode import OpenCodeBackend
        return OpenCodeBackend(gateway, participant_alias)
    raise ValueError(f"Unknown backend type: {backend_type}")


__all__ = [
    "Backend",
    "ClaudeCodeBackend",
    "OpenClawBackend",
    "OpenCodeBackend",
    "create_backend",
]


def __getattr__(name: str) -> Any:
    if name == "ClaudeCodeBackend":
        from clawbus.backends.claude_code import ClaudeCodeBackend
        return ClaudeCodeBackend
    if name == "OpenClawBackend":
        from clawbus.backends.openclaw import OpenClawBackend
        return OpenClawBackend
    if name == "OpenCodeBackend":
        from clawbus.backends.opencode import OpenCodeBackend
        return OpenCodeBackend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
