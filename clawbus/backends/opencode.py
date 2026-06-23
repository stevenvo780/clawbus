"""clawbus.backends.opencode — OpenCode HTTP SSE streaming backend."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import AsyncIterator

import httpx

from clawbus.backends import Backend

_log = logging.getLogger("clawbus.backends.opencode")

_DEFAULT_TIMEOUT_S = 120.0

_SSE_DONE_MARKER = "[DONE]"


class OpenCodeBackend(Backend):
    """Posts agent turns to an OpenCode gateway and consumes the SSE stream."""

    def __init__(
        self,
        gateway: str,
        participant_alias: str,
        timeout_s: float | None = None,
    ) -> None:
        super().__init__(gateway, participant_alias)
        self._timeout_s = timeout_s or float(
            os.getenv("CLAWBUS_TURN_TIMEOUT_S", str(_DEFAULT_TIMEOUT_S))
        )

    async def _iter_sse_events(
        self, client: httpx.AsyncClient, url: str, payload: dict[str, object]
    ) -> AsyncIterator[str]:
        """Stream SSE events and yield each 'data:' chunk value."""
        async with client.stream(
            "POST",
            url,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "text/event-stream",
            },
        ) as resp:
            if resp.status_code >= 500:
                body = await resp.aread()
                raise ConnectionError(
                    f"OpenCode gateway server error {resp.status_code}: "
                    f"{body.decode('utf-8', 'replace')[:200]}"
                )
            resp.raise_for_status()

            current_data: list[str] = []
            async for line in resp.aiter_lines():
                if line.startswith(":"):
                    continue
                if line.startswith("data:"):
                    value = line[5:].lstrip()
                    current_data.append(value)
                    continue
                if line == "" and current_data:
                    full_data = "".join(current_data)
                    current_data.clear()
                    yield full_data

            if current_data:
                full_data = "".join(current_data)
                yield full_data

    async def run_turn(self, text: str, session_id: str) -> str:
        url = f"{self.gateway.rstrip('/')}/v1/agents/{self.participant_alias}/turn"

        payload: dict[str, object] = {
            "text": text,
            "session_id": session_id,
        }

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(self._timeout_s),
        ) as client:
            chunks: list[str] = []

            try:
                async for data in self._iter_sse_events(client, url, payload):
                    if data == _SSE_DONE_MARKER:
                        break
                    try:
                        chunk = json.loads(data)
                    except (json.JSONDecodeError, TypeError):
                        chunks.append(data)
                        continue
                    if isinstance(chunk, dict) and "text" in chunk:
                        chunks.append(str(chunk["text"]))
                    elif isinstance(chunk, str):
                        chunks.append(chunk)
                    else:
                        chunks.append(data)

            except httpx.TimeoutException:
                raise asyncio.TimeoutError(
                    f"OpenCode turn timed out after {self._timeout_s}s"
                ) from None
            except httpx.HTTPError as exc:
                raise ConnectionError(
                    f"OpenCode gateway unreachable at {url}: {exc}"
                ) from exc

        response = "".join(chunks)
        _log.info(
            "opencode turn %s -> %d chars",
            session_id[:12],
            len(response),
        )
        return response
