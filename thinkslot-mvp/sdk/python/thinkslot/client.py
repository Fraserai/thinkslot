from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Literal
from urllib.parse import urlparse, urlunparse

import httpx
import websockets

logger = logging.getLogger("thinkslot.sdk.python")

SessionStatus = Literal["waiting", "resume", "expired"]


class ThinkslotError(Exception):
    pass


class SessionExpiredError(ThinkslotError):
    pass


@dataclass(slots=True)
class ThinkslotSession:
    session_id: str
    expires_at: datetime
    base_url: str
    request_timeout_sec: float = 10.0

    async def status(self) -> SessionStatus:
        async with httpx.AsyncClient(timeout=self.request_timeout_sec) as client:
            response = await client.get(f"{self.base_url}/sessions/{self.session_id}/status")
            response.raise_for_status()
            payload = response.json()
            return payload["status"]

    async def wait_until_resume(self, poll_interval_sec: float = 2.0) -> None:
        while True:
            current_status = await self.status()
            if current_status == "resume":
                return
            if current_status == "expired":
                raise SessionExpiredError(f"Thinkslot session {self.session_id} expired before resume")
            await asyncio.sleep(poll_interval_sec)

    async def ads_stream(self) -> AsyncIterator[dict[str, Any]]:
        try:
            async for ad in self._ads_stream_ws():
                yield ad
            return
        except Exception as exc:
            logger.warning("WebSocket ads stream failed, falling back to SSE: %s", exc)

        async for ad in self._ads_stream_sse():
            yield ad

    async def _ads_stream_ws(self) -> AsyncIterator[dict[str, Any]]:
        ws_url = self._ws_url()
        async with websockets.connect(ws_url) as websocket:
            while True:
                raw = await websocket.recv()
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")

                payload = json.loads(raw)
                if isinstance(payload, dict) and payload.get("status") in {"resume", "expired"}:
                    return
                if isinstance(payload, dict) and "ad_id" in payload:
                    yield payload

    async def _ads_stream_sse(self) -> AsyncIterator[dict[str, Any]]:
        sse_url = f"{self.base_url}/sse/{self.session_id}"
        buffer = ""

        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("GET", sse_url, headers={"Accept": "text/event-stream"}) as response:
                response.raise_for_status()
                async for chunk in response.aiter_text():
                    buffer += chunk
                    while "\n\n" in buffer:
                        event, buffer = buffer.split("\n\n", 1)
                        data_lines = [
                            line.replace("data:", "", 1).strip()
                            for line in event.splitlines()
                            if line.startswith("data:")
                        ]
                        if not data_lines:
                            continue

                        payload = json.loads("\n".join(data_lines))
                        if isinstance(payload, dict) and payload.get("status") in {"resume", "expired"}:
                            return
                        if isinstance(payload, dict) and "ad_id" in payload:
                            yield payload

    def _ws_url(self) -> str:
        parsed = urlparse(self.base_url)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        return urlunparse((scheme, parsed.netloc, f"/ws/{self.session_id}", "", "", ""))


async def enter(
    *,
    task_id: str,
    estimated_duration_sec: int,
    metadata: dict[str, Any] | None = None,
    base_url: str | None = None,
    request_timeout_sec: float = 10.0,
) -> ThinkslotSession:
    resolved_base_url = (base_url or os.getenv("THINKSLOT_BASE_URL") or "http://localhost:8000").rstrip("/")
    request_payload = {
        "task_id": task_id,
        "estimated_duration_sec": estimated_duration_sec,
        "metadata": metadata or {},
    }

    async with httpx.AsyncClient(timeout=request_timeout_sec) as client:
        response = await client.post(f"{resolved_base_url}/enter", json=request_payload)
        response.raise_for_status()
        payload = response.json()

    return ThinkslotSession(
        session_id=payload["session_id"],
        expires_at=datetime.fromisoformat(payload["expires_at"]),
        base_url=resolved_base_url,
        request_timeout_sec=request_timeout_sec,
    )
