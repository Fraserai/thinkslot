from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from starlette.websockets import WebSocketState

from .ad_catalog import build_ad
from .config import settings
from .models import EnterRequest, EnterResponse, SessionRecord, SessionStatusResponse
from .rate_limit import SlidingWindowRateLimiter
from .store import SessionStore

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("thinkslot.api")

store = SessionStore(
    redis_url=settings.redis_url,
    session_expiry_buffer_sec=settings.session_expiry_buffer_sec,
)
limiter = SlidingWindowRateLimiter(
    max_requests=settings.enter_rate_limit_requests,
    window_sec=settings.enter_rate_limit_window_sec,
)

ADMIN_HTML = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Thinkslot Admin Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
  </head>
  <body class="min-h-screen bg-slate-950 text-slate-100">
    <div class="mx-auto w-full max-w-7xl px-6 py-8">
      <header class="mb-8 rounded-2xl border border-slate-800 bg-slate-900/70 p-6 shadow-2xl shadow-slate-950/40">
        <div class="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
          <div>
            <p class="text-xs uppercase tracking-[0.22em] text-emerald-400">Thinkslot v0.1.2</p>
            <h1 class="mt-2 text-3xl font-bold tracking-tight text-white">CRO Revenue Dashboard</h1>
            <p class="mt-2 text-sm text-slate-300">Live monetization telemetry for active think slots.</p>
          </div>
          <div class="rounded-lg border border-slate-700 bg-slate-900 px-4 py-2 text-sm text-slate-300">
            Last refresh:
            <span id="last-refresh" class="font-medium text-slate-100">never</span>
          </div>
        </div>
      </header>

      <section class="mb-8 grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-5">
        <article class="rounded-xl border border-slate-800 bg-slate-900/70 p-5 lg:col-span-2">
          <p class="text-xs uppercase tracking-wider text-slate-400">Total Monetized Think-Seconds Today</p>
          <p id="think-seconds" class="mt-3 text-4xl font-semibold text-white">0</p>
        </article>
        <article class="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
          <p class="text-xs uppercase tracking-wider text-slate-400">Revenue Today</p>
          <p id="revenue-today" class="mt-3 text-2xl font-semibold text-emerald-300">$0.00</p>
        </article>
        <article class="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
          <p class="text-xs uppercase tracking-wider text-slate-400">Revenue This Week</p>
          <p id="revenue-week" class="mt-3 text-2xl font-semibold text-emerald-300">$0.00</p>
        </article>
        <article class="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
          <p class="text-xs uppercase tracking-wider text-slate-400">Fill Rate</p>
          <p id="fill-rate" class="mt-3 text-2xl font-semibold text-white">0.00%</p>
        </article>
        <article class="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
          <p class="text-xs uppercase tracking-wider text-slate-400">eCPM (Live)</p>
          <p id="ecpm-live" class="mt-3 text-2xl font-semibold text-white">$0.00</p>
        </article>
      </section>

      <section class="mb-8 rounded-2xl border border-slate-800 bg-slate-900/70 p-5">
        <h2 class="mb-4 text-xl font-semibold text-white">Top Earners</h2>
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-slate-800 text-sm">
            <thead>
              <tr class="text-left text-slate-400">
                <th class="px-3 py-2 font-medium">Runner ID</th>
                <th class="px-3 py-2 font-medium">Earnings (USD)</th>
                <th class="px-3 py-2 font-medium">Sessions</th>
              </tr>
            </thead>
            <tbody id="top-earners-table" class="divide-y divide-slate-800 text-slate-200"></tbody>
          </table>
        </div>
      </section>

      <section class="mb-8 rounded-2xl border border-slate-800 bg-slate-900/70 p-5">
        <div class="mb-4 flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <h2 class="text-xl font-semibold text-white">Recent Revenue Events (Last 20)</h2>
          <label class="inline-flex items-center gap-2 text-sm text-slate-300">
            Event Type
            <select id="event-filter" class="rounded-lg border border-slate-700 bg-slate-900 px-2 py-1 text-slate-100">
              <option value="all">All</option>
            </select>
          </label>
        </div>
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-slate-800 text-sm">
            <thead>
              <tr class="text-left text-slate-400">
                <th class="px-3 py-2 font-medium">Time</th>
                <th class="px-3 py-2 font-medium">Event</th>
                <th class="px-3 py-2 font-medium">Session</th>
                <th class="px-3 py-2 font-medium">Ad</th>
                <th class="px-3 py-2 font-medium">Action</th>
                <th class="px-3 py-2 font-medium">Value</th>
              </tr>
            </thead>
            <tbody id="revenue-events-table" class="divide-y divide-slate-800 text-slate-200"></tbody>
          </table>
        </div>
      </section>

      <section class="mb-8 rounded-2xl border border-slate-800 bg-slate-900/70 p-5">
        <div class="mb-4 flex items-center justify-between">
          <h2 class="text-xl font-semibold text-white">Sessions</h2>
          <button id="refresh-btn" class="rounded-lg border border-slate-700 bg-slate-800 px-3 py-2 text-sm text-slate-100 transition hover:border-emerald-400 hover:text-emerald-300">
            Refresh Now
          </button>
        </div>
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-slate-800 text-sm">
            <thead>
              <tr class="text-left text-slate-400">
                <th class="px-3 py-2 font-medium">Session ID</th>
                <th class="px-3 py-2 font-medium">Task ID</th>
                <th class="px-3 py-2 font-medium">Status</th>
                <th class="px-3 py-2 font-medium">Running</th>
                <th class="px-3 py-2 font-medium">Metadata</th>
                <th class="px-3 py-2 font-medium">Action</th>
              </tr>
            </thead>
            <tbody id="sessions-table" class="divide-y divide-slate-800 text-slate-200"></tbody>
          </table>
        </div>
      </section>

      <section class="rounded-2xl border border-slate-800 bg-slate-900/70 p-5">
        <h2 class="mb-4 text-xl font-semibold text-white">Recent Ads Served (Last 10)</h2>
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-slate-800 text-sm">
            <thead>
              <tr class="text-left text-slate-400">
                <th class="px-3 py-2 font-medium">Time</th>
                <th class="px-3 py-2 font-medium">Session ID</th>
                <th class="px-3 py-2 font-medium">Ad ID</th>
                <th class="px-3 py-2 font-medium">Type</th>
                <th class="px-3 py-2 font-medium">Title</th>
                <th class="px-3 py-2 font-medium">Reward</th>
              </tr>
            </thead>
            <tbody id="ads-table" class="divide-y divide-slate-800 text-slate-200"></tbody>
          </table>
        </div>
      </section>
    </div>

    <script>
      const sessionsTable = document.getElementById("sessions-table");
      const adsTable = document.getElementById("ads-table");
      const topEarnersTable = document.getElementById("top-earners-table");
      const revenueEventsTable = document.getElementById("revenue-events-table");
      const eventFilter = document.getElementById("event-filter");
      const refreshBtn = document.getElementById("refresh-btn");
      const lastRefresh = document.getElementById("last-refresh");
      const thinkSeconds = document.getElementById("think-seconds");
      const revenueToday = document.getElementById("revenue-today");
      const revenueWeek = document.getElementById("revenue-week");
      const fillRate = document.getElementById("fill-rate");
      const ecpmLive = document.getElementById("ecpm-live");

      let allRevenueEvents = [];

      function escapeHtml(value) {
        return String(value)
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#039;");
      }

      function formatUsd(value) {
        const number = Number(value || 0);
        return `$${number.toFixed(4)}`;
      }

      function prettyDuration(totalSec) {
        const sec = Math.max(0, Number(totalSec || 0));
        const h = Math.floor(sec / 3600);
        const m = Math.floor((sec % 3600) / 60);
        const s = sec % 60;
        if (h > 0) return `${h}h ${m}m ${s}s`;
        if (m > 0) return `${m}m ${s}s`;
        return `${s}s`;
      }

      function statusBadge(status) {
        const style = status === "waiting"
          ? "bg-amber-500/20 text-amber-300"
          : status === "resume"
          ? "bg-emerald-500/20 text-emerald-300"
          : "bg-slate-700 text-slate-200";
        return `<span class="rounded-full px-2 py-1 text-xs font-medium ${style}">${escapeHtml(status)}</span>`;
      }

      function renderTopEarners(rows) {
        if (!rows.length) {
          topEarnersTable.innerHTML = `
            <tr><td colspan="3" class="px-3 py-6 text-center text-slate-400">No earnings data yet.</td></tr>
          `;
          return;
        }

        topEarnersTable.innerHTML = rows.map((row) => `
          <tr>
            <td class="px-3 py-2 font-mono text-xs">${escapeHtml(row.runner_id)}</td>
            <td class="px-3 py-2">${formatUsd(row.earnings_usd)}</td>
            <td class="px-3 py-2">${escapeHtml(String(row.session_count))}</td>
          </tr>
        `).join("");
      }

      function renderRevenueEvents(events) {
        if (!events.length) {
          revenueEventsTable.innerHTML = `
            <tr><td colspan="6" class="px-3 py-6 text-center text-slate-400">No matching revenue events.</td></tr>
          `;
          return;
        }

        revenueEventsTable.innerHTML = events.map((event) => {
          const value = event.revenue_per_impression_usd ?? event.value_usd ?? event.revenue_generated_usd ?? event.amount ?? 0;
          return `
            <tr>
              <td class="px-3 py-2 text-slate-300">${escapeHtml(new Date(event.timestamp).toLocaleTimeString())}</td>
              <td class="px-3 py-2">${escapeHtml(event.event || "")}</td>
              <td class="px-3 py-2 font-mono text-xs">${escapeHtml(event.session_id || "-")}</td>
              <td class="px-3 py-2 font-mono text-xs">${escapeHtml(event.ad_id || "-")}</td>
              <td class="px-3 py-2">${escapeHtml(event.action || "-")}</td>
              <td class="px-3 py-2">${escapeHtml(String(value))}</td>
            </tr>
          `;
        }).join("");
      }

      function applyRevenueFilter() {
        const selected = eventFilter.value;
        const filtered = selected === "all"
          ? allRevenueEvents
          : allRevenueEvents.filter((event) => event.event === selected);
        renderRevenueEvents(filtered);
      }

      function updateRevenueFilterOptions(events) {
        const selected = eventFilter.value || "all";
        const types = [...new Set(events.map((event) => event.event).filter(Boolean))].sort();

        eventFilter.innerHTML = [
          '<option value="all">All</option>',
          ...types.map((type) => `<option value="${escapeHtml(type)}">${escapeHtml(type)}</option>`),
        ].join("");

        if (selected !== "all" && types.includes(selected)) {
          eventFilter.value = selected;
        } else {
          eventFilter.value = "all";
        }
      }

      async function resumeSession(sessionId) {
        try {
          const response = await fetch(`/admin/resume/${encodeURIComponent(sessionId)}`, { method: "POST" });
          if (!response.ok) {
            throw new Error(`Resume failed (${response.status})`);
          }
          await loadDashboard();
        } catch (error) {
          console.error(error);
          alert("Unable to resume session.");
        }
      }

      function renderSessions(sessions) {
        if (!sessions.length) {
          sessionsTable.innerHTML = `
            <tr><td colspan="6" class="px-3 py-6 text-center text-slate-400">No active sessions.</td></tr>
          `;
          return;
        }

        sessionsTable.innerHTML = sessions.map((session) => `
          <tr>
            <td class="px-3 py-2 font-mono text-xs">${escapeHtml(session.session_id)}</td>
            <td class="px-3 py-2">${escapeHtml(session.task_id)}</td>
            <td class="px-3 py-2">${statusBadge(session.status)}</td>
            <td class="px-3 py-2">${prettyDuration(session.running_sec)}</td>
            <td class="px-3 py-2 text-slate-300">${escapeHtml(session.metadata_summary)}</td>
            <td class="px-3 py-2">
              <button
                class="rounded-md bg-emerald-500 px-2 py-1 text-xs font-medium text-emerald-950 transition hover:bg-emerald-400 disabled:cursor-not-allowed disabled:opacity-50"
                onclick="resumeSession('${escapeHtml(session.session_id)}')"
                ${session.status !== "waiting" ? "disabled" : ""}
              >
                Resume
              </button>
            </td>
          </tr>
        `).join("");
      }

      function renderAds(ads) {
        if (!ads.length) {
          adsTable.innerHTML = `
            <tr><td colspan="6" class="px-3 py-6 text-center text-slate-400">No ads served yet.</td></tr>
          `;
          return;
        }

        adsTable.innerHTML = ads.map((ad) => `
          <tr>
            <td class="px-3 py-2 text-slate-300">${escapeHtml(new Date(ad.served_at).toLocaleTimeString())}</td>
            <td class="px-3 py-2 font-mono text-xs">${escapeHtml(ad.session_id)}</td>
            <td class="px-3 py-2 font-mono text-xs">${escapeHtml(ad.ad_id)}</td>
            <td class="px-3 py-2">${escapeHtml(ad.type)}</td>
            <td class="px-3 py-2">${escapeHtml(ad.title)}</td>
            <td class="px-3 py-2">${escapeHtml(String(ad.reward_amount))} tokens</td>
          </tr>
        `).join("");
      }

      async function loadDashboard() {
        const response = await fetch("/admin/api/dashboard", { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`Dashboard API failed (${response.status})`);
        }

        const payload = await response.json();
        renderSessions(payload.sessions || []);
        renderAds(payload.recent_ads || []);
        renderTopEarners(payload.top_10_agents_by_earnings || []);

        allRevenueEvents = payload.recent_revenue_events || [];
        updateRevenueFilterOptions(allRevenueEvents);
        applyRevenueFilter();

        thinkSeconds.textContent = Number(payload.total_monetized_think_seconds_today || 0).toLocaleString();
        revenueToday.textContent = formatUsd(payload.revenue_today_usd || 0);
        revenueWeek.textContent = formatUsd(payload.revenue_week_usd || 0);
        fillRate.textContent = `${Number(payload.fill_rate_percent || 0).toFixed(2)}%`;
        ecpmLive.textContent = formatUsd(payload.eCPM || 0);
        lastRefresh.textContent = new Date(payload.generated_at).toLocaleTimeString();
      }

      refreshBtn.addEventListener("click", async () => {
        try {
          await loadDashboard();
        } catch (error) {
          console.error(error);
        }
      });

      eventFilter.addEventListener("change", applyRevenueFilter);
      window.resumeSession = resumeSession;

      async function poll() {
        try {
          await loadDashboard();
        } catch (error) {
          console.error(error);
        }
      }

      poll();
      setInterval(poll, 3000);
    </script>
  </body>
</html>
"""


@asynccontextmanager
async def lifespan(_: FastAPI):
    await store.startup()
    yield
    await store.shutdown()


app = FastAPI(
    title="Thinkslot MVP",
    version="0.1.2",
    description="Agents think. You earn.",
    lifespan=lifespan,
)


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1_000
    logger.info("%s %s -> %s (%.2fms)", request.method, request.url.path, response.status_code, elapsed_ms)
    return response


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.exception_handler(Exception)
async def generic_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled error: %s", exc)
    return JSONResponse(status_code=500, content={"detail": "internal_server_error"})


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def _request_key(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for", "")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "anonymous"


def _metadata_summary(metadata: dict[str, Any], max_length: int = 96) -> str:
    if not metadata:
        return "-"
    try:
        compact = json.dumps(metadata, separators=(",", ":"), ensure_ascii=True)
    except (TypeError, ValueError):
        return "invalid metadata"
    if len(compact) <= max_length:
        return compact
    return f"{compact[:max_length - 3]}..."


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _parse_event_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _runner_id_from_metadata(metadata: dict[str, Any], session_id: str) -> str:
    candidate = metadata.get("runner_id") or metadata.get("agent_id") or metadata.get("agent")
    if isinstance(candidate, str) and candidate.strip():
        return candidate.strip()
    return f"runner_{session_id[:8]}"


async def _emit_resume_events(record: SessionRecord) -> None:
    last_ad_id = await store.last_ad_id(record.session_id)
    if last_ad_id:
        await store.record_ad_interaction(
            ad_id=last_ad_id,
            session_id=record.session_id,
            action="claim",
            value_usd=0.0,
        )
        await store.record_ad_conversion(
            ad_id=last_ad_id,
            session_id=record.session_id,
            action="tool_used",
            value_usd=0.125,
        )

    runner_id = _runner_id_from_metadata(record.metadata, record.session_id)
    await store.record_rebate_issued(
        session_id=record.session_id,
        amount=25,
        currency="tokens",
        runner_id=runner_id,
    )


def _build_revenue_metrics(
    *,
    events: list[dict[str, Any]],
    sessions: list[SessionRecord],
    now: datetime,
) -> dict[str, Any]:
    start_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_week = start_today - timedelta(days=7)

    slot_enter_total = 0
    slot_enter_filled = 0
    impressions_today = 0
    revenue_today_usd = 0.0
    revenue_week_usd = 0.0
    completed_think_seconds_today = 0

    session_runner_map: dict[str, str] = {}
    session_revenue_map: dict[str, float] = {}

    for event in events:
        event_name = str(event.get("event", ""))
        session_id = str(event.get("session_id", "")) if event.get("session_id") is not None else ""
        event_timestamp = _parse_event_timestamp(event.get("timestamp")) or now

        in_today = event_timestamp >= start_today
        in_week = event_timestamp >= start_week

        if event_name == "thinkslot.slot_enter":
            slot_enter_total += 1
            if bool(event.get("filled", False)):
                slot_enter_filled += 1

            runner_id = event.get("runner_id")
            if not isinstance(runner_id, str) or not runner_id.strip():
                runner_id = f"runner_{session_id[:8]}" if session_id else "runner_unknown"
            if session_id:
                session_runner_map[session_id] = runner_id
            continue

        if event_name == "thinkslot.slot_exit":
            duration = max(0, int(_safe_float(event.get("actual_duration_sec"))))
            revenue_value = _safe_float(event.get("revenue_generated_usd"))
            if in_today:
                completed_think_seconds_today += duration
                revenue_today_usd += revenue_value
            if in_week:
                revenue_week_usd += revenue_value
            if session_id:
                session_revenue_map[session_id] = session_revenue_map.get(session_id, 0.0) + revenue_value
            continue

        if event_name == "thinkslot.ad_impression":
            revenue_value = _safe_float(event.get("revenue_per_impression_usd"))
            if in_today:
                impressions_today += 1
                revenue_today_usd += revenue_value
            if in_week:
                revenue_week_usd += revenue_value
            if session_id:
                session_revenue_map[session_id] = session_revenue_map.get(session_id, 0.0) + revenue_value
            continue

        if event_name in {"thinkslot.ad_interaction", "thinkslot.ad_conversion"}:
            revenue_value = _safe_float(event.get("value_usd"))
            if in_today:
                revenue_today_usd += revenue_value
            if in_week:
                revenue_week_usd += revenue_value
            if session_id:
                session_revenue_map[session_id] = session_revenue_map.get(session_id, 0.0) + revenue_value

    active_think_seconds_today = 0
    for record in sessions:
        if record.status != "waiting":
            continue
        if record.created_at < start_today:
            continue
        active_think_seconds_today += max(0, int((now - record.created_at).total_seconds()))

    total_monetized_think_seconds_today = completed_think_seconds_today + active_think_seconds_today
    fill_rate_percent = (slot_enter_filled / slot_enter_total * 100.0) if slot_enter_total else 0.0
    ecpm_live = (revenue_today_usd / impressions_today * 1000.0) if impressions_today else 0.0

    runner_totals: dict[str, float] = {}
    runner_sessions: dict[str, set[str]] = {}

    for session_id, revenue_value in session_revenue_map.items():
        runner_id = session_runner_map.get(session_id) or f"runner_{session_id[:8]}"
        runner_totals[runner_id] = runner_totals.get(runner_id, 0.0) + revenue_value
        runner_sessions.setdefault(runner_id, set()).add(session_id)

    for session_id, runner_id in session_runner_map.items():
        runner_totals.setdefault(runner_id, 0.0)
        runner_sessions.setdefault(runner_id, set()).add(session_id)

    top_10_agents_by_earnings = [
        {
            "runner_id": runner_id,
            "earnings_usd": round(earnings, 6),
            "session_count": len(runner_sessions.get(runner_id, set())),
        }
        for runner_id, earnings in sorted(runner_totals.items(), key=lambda item: item[1], reverse=True)[:10]
    ]

    return {
        "total_monetized_think_seconds_today": total_monetized_think_seconds_today,
        "revenue_today_usd": round(revenue_today_usd, 6),
        "revenue_week_usd": round(revenue_week_usd, 6),
        "fill_rate_percent": round(fill_rate_percent, 6),
        "eCPM": round(ecpm_live, 6),
        "top_10_agents_by_earnings": top_10_agents_by_earnings,
        "recent_revenue_events": events[:20],
    }


@app.post("/enter", response_model=EnterResponse)
async def enter(payload: EnterRequest, request: Request) -> EnterResponse:
    key = _request_key(request)
    allowed, retry_after = await limiter.allow(key)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded. Retry in {retry_after} seconds.",
        )

    record = await store.create_session(
        task_id=payload.task_id,
        estimated_duration_sec=payload.estimated_duration_sec,
        metadata=payload.metadata,
    )

    await store.record_slot_enter(
        session_id=record.session_id,
        task_id=record.task_id,
        estimated_duration_sec=record.estimated_duration_sec,
        filled=bool(payload.metadata.get("filled", True)),
        runner_id=_runner_id_from_metadata(payload.metadata, record.session_id),
    )
    return EnterResponse(session_id=record.session_id, expires_at=record.expires_at)


@app.get("/sessions/{session_id}/status", response_model=SessionStatusResponse)
async def session_status(session_id: str) -> SessionStatusResponse:
    status_value = await store.get_status(session_id)
    if status_value is None:
        raise HTTPException(status_code=404, detail="session_not_found")
    return SessionStatusResponse(status=status_value)


@app.post("/internal/resume/{session_id}")
async def resume_session(
    session_id: str,
    x_internal_api_key: str | None = Header(default=None),
) -> dict[str, Any]:
    if x_internal_api_key != settings.internal_api_key:
        raise HTTPException(status_code=401, detail="unauthorized")

    status_before = await store.get_status(session_id)
    record = await store.resume(session_id)
    if record is None:
        raise HTTPException(status_code=404, detail="session_not_found")

    if status_before == "waiting" and record.status == "resume":
        await _emit_resume_events(record)

    return {"session_id": record.session_id, "status": record.status}


@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard() -> HTMLResponse:
    return HTMLResponse(content=ADMIN_HTML)


@app.get("/admin/api/dashboard")
async def admin_dashboard_data() -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    sessions = await store.list_sessions(limit=200)
    active_sessions: list[dict[str, Any]] = []
    for record in sessions:
        if record.status == "expired":
            continue
        active_sessions.append(
            {
                "session_id": record.session_id,
                "task_id": record.task_id,
                "status": record.status,
                "running_sec": max(0, int((now - record.created_at).total_seconds())),
                "metadata_summary": _metadata_summary(record.metadata),
            }
        )

    recent_ads = await store.recent_ads(limit=10)
    revenue_events = await store.revenue_events(limit=500)
    metrics = _build_revenue_metrics(events=revenue_events, sessions=sessions, now=now)

    return {
        "generated_at": now.isoformat(),
        "sessions": active_sessions,
        "recent_ads": recent_ads,
        **metrics,
    }


@app.post("/admin/resume/{session_id}")
async def admin_resume_session(session_id: str) -> dict[str, Any]:
    status_before = await store.get_status(session_id)
    record = await store.resume(session_id)
    if record is None:
        raise HTTPException(status_code=404, detail="session_not_found")

    if status_before == "waiting" and record.status == "resume":
        await _emit_resume_events(record)

    return {"session_id": record.session_id, "status": record.status}


@app.websocket("/ws/{session_id}")
async def websocket_ads(websocket: WebSocket, session_id: str) -> None:
    await websocket.accept()
    record = await store.get_session(session_id)
    if record is None:
        await websocket.send_json({"detail": "session_not_found"})
        await websocket.close(code=1008)
        return

    sequence_number = 0

    try:
        while True:
            session_state = await store.get_status(session_id)
            if session_state != "waiting":
                await websocket.send_json({"status": session_state})
                break

            show_seconds = random.uniform(settings.ad_min_interval_sec, settings.ad_max_interval_sec)
            ad = build_ad(sequence_number)
            await websocket.send_json(ad.model_dump())
            await store.record_ad_served(session_id, ad)
            await store.record_ad_impression(
                ad_id=ad.ad_id,
                session_id=session_id,
                seconds_shown=show_seconds,
                revenue_per_impression_usd=0.0025,
            )
            sequence_number += 1

            await asyncio.sleep(show_seconds)
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for session=%s", session_id)
    except Exception as exc:
        logger.exception("WebSocket stream failed for session=%s: %s", session_id, exc)
    finally:
        if websocket.application_state != WebSocketState.DISCONNECTED:
            await websocket.close()


@app.get("/sse/{session_id}")
async def sse_ads(session_id: str) -> StreamingResponse:
    record = await store.get_session(session_id)
    if record is None:
        raise HTTPException(status_code=404, detail="session_not_found")

    async def event_stream():
        sequence_number = 0

        while True:
            session_state = await store.get_status(session_id)
            if session_state != "waiting":
                payload = json.dumps({"status": session_state})
                yield f"event: session\ndata: {payload}\n\n"
                break

            show_seconds = random.uniform(settings.ad_min_interval_sec, settings.ad_max_interval_sec)
            ad = build_ad(sequence_number)
            await store.record_ad_served(session_id, ad)
            await store.record_ad_impression(
                ad_id=ad.ad_id,
                session_id=session_id,
                seconds_shown=show_seconds,
                revenue_per_impression_usd=0.0025,
            )
            sequence_number += 1
            yield f"event: ad\ndata: {ad.model_dump_json()}\n\n"
            await asyncio.sleep(show_seconds)

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)
