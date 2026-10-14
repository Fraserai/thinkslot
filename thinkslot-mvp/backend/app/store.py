from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import uuid4

from redis.asyncio import Redis
from redis.asyncio import from_url as redis_from_url

from .models import AdPayload, SessionRecord, SessionStatus

logger = logging.getLogger("thinkslot.store")

AdAction = Literal["click", "claim", "tool_used"]
RebateCurrency = Literal["tokens", "usd"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SessionStore:
    def __init__(self, redis_url: str, session_expiry_buffer_sec: int) -> None:
        self._redis: Redis = redis_from_url(redis_url, decode_responses=True)
        self._redis_available = False
        self._buffer_sec = session_expiry_buffer_sec
        self._post_expiry_retention_sec = 3_600
        self._memory: dict[str, SessionRecord] = {}
        self._recent_ads_memory: deque[dict[str, Any]] = deque(maxlen=200)
        self._revenue_events_memory: deque[dict[str, Any]] = deque(maxlen=500)
        self._last_ad_by_session: dict[str, str] = {}
        self._session_revenue_usd: dict[str, float] = {}
        self._session_rebate_usd: dict[str, float] = {}
        self._session_runner_id: dict[str, str] = {}
        self._slot_exit_emitted: set[str] = set()
        self._lock = asyncio.Lock()

    async def startup(self) -> None:
        try:
            await self._redis.ping()
            self._redis_available = True
            logger.info("Connected to Redis session store")
        except Exception as exc:  # pragma: no cover
            self._redis_available = False
            logger.warning("Redis unavailable; using in-memory session store only: %s", exc)

    async def shutdown(self) -> None:
        if self._redis_available:
            if hasattr(self._redis, "aclose"):
                await self._redis.aclose()
            else:  # pragma: no cover
                await self._redis.close()

    async def create_session(
        self,
        task_id: str,
        estimated_duration_sec: int,
        metadata: dict[str, Any],
    ) -> SessionRecord:
        now = _utc_now()
        record = SessionRecord(
            session_id=uuid4().hex,
            task_id=task_id,
            estimated_duration_sec=estimated_duration_sec,
            metadata=metadata,
            status="waiting",
            created_at=now,
            expires_at=now + timedelta(seconds=estimated_duration_sec + self._buffer_sec),
        )
        await self._save(record)
        return record

    async def get_session(self, session_id: str) -> SessionRecord | None:
        async with self._lock:
            local_record = self._memory.get(session_id)

        if local_record is None and self._redis_available:
            redis_record = await self._load_from_redis(session_id)
            if redis_record is not None:
                async with self._lock:
                    self._memory[session_id] = redis_record
                local_record = redis_record

        if local_record is None:
            return None

        expired_duration_sec = self._refresh_expiry(local_record)
        if expired_duration_sec is not None:
            await self._save(local_record)
            await self.record_slot_exit(
                session_id=local_record.session_id,
                actual_duration_sec=expired_duration_sec,
                revenue_generated_usd=self._session_revenue_usd.get(local_record.session_id, 0.0),
                rebate_issued_usd=self._session_rebate_usd.get(local_record.session_id, 0.0),
            )

        return local_record

    async def get_status(self, session_id: str) -> SessionStatus | None:
        record = await self.get_session(session_id)
        return record.status if record is not None else None

    async def resume(self, session_id: str) -> SessionRecord | None:
        record = await self.get_session(session_id)
        if record is None:
            return None

        if record.status != "waiting":
            return record

        record.status = "resume"
        await self._save(record)

        actual_duration_sec = max(0, int((_utc_now() - record.created_at).total_seconds()))
        await self.record_slot_exit(
            session_id=record.session_id,
            actual_duration_sec=actual_duration_sec,
            revenue_generated_usd=self._session_revenue_usd.get(record.session_id, 0.0),
            rebate_issued_usd=self._session_rebate_usd.get(record.session_id, 0.0),
        )
        return record

    async def list_sessions(self, limit: int = 200) -> list[SessionRecord]:
        async with self._lock:
            session_map = dict(self._memory)

        if self._redis_available:
            session_ids = await self._scan_session_ids(max_items=limit * 4)
            for session_id in session_ids:
                if session_id in session_map:
                    continue
                redis_record = await self._load_from_redis(session_id)
                if redis_record is not None:
                    session_map[session_id] = redis_record

        records = sorted(session_map.values(), key=lambda item: item.created_at, reverse=True)
        hydrated_records: list[SessionRecord] = []
        for record in records:
            expired_duration_sec = self._refresh_expiry(record)
            if expired_duration_sec is not None:
                await self._save(record)
                await self.record_slot_exit(
                    session_id=record.session_id,
                    actual_duration_sec=expired_duration_sec,
                    revenue_generated_usd=self._session_revenue_usd.get(record.session_id, 0.0),
                    rebate_issued_usd=self._session_rebate_usd.get(record.session_id, 0.0),
                )
            hydrated_records.append(record)
            if len(hydrated_records) >= limit:
                break

        return hydrated_records

    async def record_slot_enter(
        self,
        *,
        session_id: str,
        task_id: str,
        estimated_duration_sec: int,
        filled: bool,
        runner_id: str | None,
    ) -> None:
        payload: dict[str, Any] = {
            "session_id": session_id,
            "task_id": task_id,
            "estimated_duration_sec": int(estimated_duration_sec),
            "filled": bool(filled),
        }
        if runner_id:
            payload["runner_id"] = runner_id
            async with self._lock:
                self._session_runner_id[session_id] = runner_id

        await self._record_revenue_event("thinkslot.slot_enter", payload)

    async def record_slot_exit(
        self,
        *,
        session_id: str,
        actual_duration_sec: int,
        revenue_generated_usd: float,
        rebate_issued_usd: float,
    ) -> None:
        async with self._lock:
            if session_id in self._slot_exit_emitted:
                return
            self._slot_exit_emitted.add(session_id)

        payload = {
            "session_id": session_id,
            "actual_duration_sec": max(0, int(actual_duration_sec)),
            "revenue_generated_usd": round(float(revenue_generated_usd), 6),
            "rebate_issued_usd": round(float(rebate_issued_usd), 6),
        }
        await self._record_revenue_event("thinkslot.slot_exit", payload)

    async def record_ad_impression(
        self,
        *,
        ad_id: str,
        session_id: str,
        seconds_shown: float,
        revenue_per_impression_usd: float,
    ) -> None:
        revenue_value = max(0.0, float(revenue_per_impression_usd))
        await self._add_session_revenue(session_id, revenue_value)

        payload = {
            "ad_id": ad_id,
            "session_id": session_id,
            "seconds_shown": round(float(seconds_shown), 3),
            "revenue_per_impression_usd": round(revenue_value, 6),
        }
        await self._record_revenue_event("thinkslot.ad_impression", payload)

    async def record_ad_interaction(
        self,
        *,
        ad_id: str,
        session_id: str,
        action: AdAction,
        value_usd: float,
    ) -> None:
        value = max(0.0, float(value_usd))
        await self._add_session_revenue(session_id, value)

        payload = {
            "ad_id": ad_id,
            "session_id": session_id,
            "action": action,
            "value_usd": round(value, 6),
        }
        await self._record_revenue_event("thinkslot.ad_interaction", payload)

    async def record_ad_conversion(
        self,
        *,
        ad_id: str,
        session_id: str,
        action: AdAction,
        value_usd: float,
    ) -> None:
        value = max(0.0, float(value_usd))
        await self._add_session_revenue(session_id, value)

        payload = {
            "ad_id": ad_id,
            "session_id": session_id,
            "action": action,
            "value_usd": round(value, 6),
        }
        await self._record_revenue_event("thinkslot.ad_conversion", payload)

    async def record_rebate_issued(
        self,
        *,
        session_id: str,
        amount: float,
        currency: RebateCurrency,
        runner_id: str | None,
    ) -> None:
        rebate_usd = float(amount) if currency == "usd" else 0.0

        async with self._lock:
            self._session_rebate_usd[session_id] = self._session_rebate_usd.get(session_id, 0.0) + rebate_usd
            if runner_id:
                self._session_runner_id[session_id] = runner_id

        payload: dict[str, Any] = {
            "session_id": session_id,
            "amount": round(float(amount), 6),
            "currency": currency,
        }
        if runner_id:
            payload["runner_id"] = runner_id

        await self._record_revenue_event("thinkslot.rebate_issued", payload)

    async def record_ad_served(self, session_id: str, ad: AdPayload) -> None:
        ad_event = {
            "served_at": _utc_now().isoformat(),
            "session_id": session_id,
            "ad_id": ad.ad_id,
            "type": ad.type,
            "title": ad.title,
            "reward_amount": ad.reward.amount,
            "tool_name": ad.action.tool_name,
        }

        async with self._lock:
            self._recent_ads_memory.appendleft(ad_event)
            self._last_ad_by_session[session_id] = ad.ad_id

        if not self._redis_available:
            return

        payload = json.dumps(ad_event)
        await self._redis.lpush(self._redis_recent_ads_key(), payload)
        await self._redis.ltrim(self._redis_recent_ads_key(), 0, 199)

    async def recent_ads(self, limit: int = 10) -> list[dict[str, Any]]:
        if self._redis_available:
            raw_ads = await self._redis.lrange(self._redis_recent_ads_key(), 0, limit - 1)
            parsed_ads: list[dict[str, Any]] = []
            for raw_ad in raw_ads:
                try:
                    parsed_ads.append(json.loads(raw_ad))
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed ad event from Redis")
            if parsed_ads:
                return parsed_ads

        async with self._lock:
            return list(self._recent_ads_memory)[:limit]

    async def revenue_events(self, limit: int = 500) -> list[dict[str, Any]]:
        if self._redis_available:
            raw_events = await self._redis.lrange(self._redis_revenue_events_key(), 0, limit - 1)
            parsed_events: list[dict[str, Any]] = []
            for raw_event in raw_events:
                try:
                    parsed_events.append(json.loads(raw_event))
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed revenue event from Redis")
            if parsed_events:
                return parsed_events

        async with self._lock:
            return list(self._revenue_events_memory)[:limit]

    async def recent_revenue_events(self, limit: int = 20) -> list[dict[str, Any]]:
        return await self.revenue_events(limit=limit)

    async def last_ad_id(self, session_id: str) -> str | None:
        async with self._lock:
            return self._last_ad_by_session.get(session_id)

    async def session_runner_id(self, session_id: str) -> str | None:
        async with self._lock:
            return self._session_runner_id.get(session_id)

    async def _record_revenue_event(self, event_name: str, payload: dict[str, Any]) -> None:
        event = {
            "timestamp": _utc_now().isoformat(),
            "event": event_name,
            **payload,
        }

        async with self._lock:
            self._revenue_events_memory.appendleft(event)

        if not self._redis_available:
            return

        encoded = json.dumps(event)
        await self._redis.lpush(self._redis_revenue_events_key(), encoded)
        await self._redis.ltrim(self._redis_revenue_events_key(), 0, 499)

    async def _add_session_revenue(self, session_id: str, revenue_usd: float) -> None:
        async with self._lock:
            self._session_revenue_usd[session_id] = self._session_revenue_usd.get(session_id, 0.0) + revenue_usd

    async def _save(self, record: SessionRecord) -> None:
        async with self._lock:
            self._memory[record.session_id] = record

        if not self._redis_available:
            return

        key = self._redis_key(record.session_id)
        payload = {
            "session_id": record.session_id,
            "task_id": record.task_id,
            "estimated_duration_sec": str(record.estimated_duration_sec),
            "metadata": json.dumps(record.metadata),
            "status": record.status,
            "created_at": record.created_at.isoformat(),
            "expires_at": record.expires_at.isoformat(),
        }

        await self._redis.hset(key, mapping=payload)
        ttl_sec = max(
            1,
            int((record.expires_at - _utc_now()).total_seconds()) + self._post_expiry_retention_sec,
        )
        await self._redis.expire(key, ttl_sec)

    async def _load_from_redis(self, session_id: str) -> SessionRecord | None:
        key = self._redis_key(session_id)
        raw = await self._redis.hgetall(key)
        if not raw:
            return None

        try:
            return SessionRecord(
                session_id=raw["session_id"],
                task_id=raw["task_id"],
                estimated_duration_sec=int(raw["estimated_duration_sec"]),
                metadata=json.loads(raw["metadata"]),
                status=raw["status"],
                created_at=datetime.fromisoformat(raw["created_at"]),
                expires_at=datetime.fromisoformat(raw["expires_at"]),
            )
        except Exception as exc:
            logger.error("Invalid session payload in Redis for %s: %s", session_id, exc)
            return None

    @staticmethod
    def _refresh_expiry(record: SessionRecord) -> int | None:
        if record.status == "waiting" and _utc_now() >= record.expires_at:
            record.status = "expired"
            return max(0, int((record.expires_at - record.created_at).total_seconds()))
        return None

    @staticmethod
    def _redis_key(session_id: str) -> str:
        return f"thinkslot:session:{session_id}"

    @staticmethod
    def _redis_recent_ads_key() -> str:
        return "thinkslot:recent_ads"

    @staticmethod
    def _redis_revenue_events_key() -> str:
        return "thinkslot:revenue_events"

    async def _scan_session_ids(self, max_items: int = 800) -> list[str]:
        if not self._redis_available:
            return []

        session_ids: list[str] = []
        cursor: int | str = 0

        while True:
            cursor, keys = await self._redis.scan(cursor=cursor, match="thinkslot:session:*", count=200)
            for key in keys:
                session_ids.append(key.rsplit(":", 1)[-1])
                if len(session_ids) >= max_items:
                    return session_ids
            if cursor in (0, "0"):
                break

        return session_ids
