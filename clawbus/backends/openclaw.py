"""clawbus.backends.openclaw — OpenClaw (Dave/Erin) HTTP backend."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import httpx

from clawbus.backends import Backend

_log = logging.getLogger("clawbus.backends.openclaw")

_DEFAULT_TIMEOUT_S = 120.0


class OpenClawBackend(Backend):
    """Posts agent turns to an OpenClaw gateway (Dave or Erin)."""

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
        self._http: httpx.AsyncClient | None = None

    async def _ensure_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                timeout=httpx.Timeout(self._timeout_s),
            )
        return self._http

    async def run_turn(self, text: str, session_id: str) -> str:
        http = await self._ensure_http()
        url = f"{self.gateway.rstrip('/')}/agent"

        payload: dict[str, Any] = {
            "text": text,
            "session": session_id,
        }

        try:
            resp = await http.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
        except httpx.TimeoutException:
            raise asyncio.TimeoutError(
                f"OpenClaw turn timed out after {self._timeout_s}s"
            ) from None
        except httpx.HTTPError as exc:
            raise ConnectionError(
                f"OpenClaw gateway unreachable at {url}: {exc}"
            ) from exc

        if resp.status_code >= 500:
            raise ConnectionError(
                f"OpenClaw gateway server error {resp.status_code}: {resp.text[:200]}"
            )

        resp.raise_for_status()

        try:
            data = resp.json()
        except ValueError as exc:
            raise ConnectionError(
                f"OpenClaw gateway returned non-JSON response: {resp.text[:200]}"
            ) from exc

        if not isinstance(data, dict):
            raise ConnectionError(
                f"OpenClaw gateway returned non-object response"
            )

        if "error" in data:
            raise ConnectionError(
                f"OpenClaw turn error: {data['error']}"
            )

        response: str = data.get("response", "")
        if not isinstance(response, str):
            raise ConnectionError(
                f"OpenClaw gateway returned non-string response field"
            )

        _log.info(
            "openclaw turn %s -> %d chars",
            session_id[:12],
            len(response),
        )
        return response

    async def aclose(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None
