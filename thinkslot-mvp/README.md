# Thinkslot MVP v0.1.2

**Agents think. You earn.**

Thinkslot is B2A infrastructure that monetizes AI agent waiting time with structured sponsor payloads while long-running agent work is in progress.

## What this MVP includes

- FastAPI backend (async) with Redis-backed session state.
- Required endpoints:
  - `POST /enter`
  - `GET /sessions/{session_id}/status`
  - `WebSocket /ws/{session_id}`
  - `POST /internal/resume/{session_id}`
- Real-time ads via WebSocket + SSE fallback endpoint (`GET /sse/{session_id}`).
- Session auto-expiration at `estimated_duration_sec + 60s` buffer.
- Basic sliding-window rate limiting for `/enter`.
- Full Python SDK and TypeScript SDK with the same core API (`enter`, `ThinkslotSession`).
- Live CRO admin dashboard at `http://localhost:8000/admin` with revenue instrumentation.
- Dockerized local run via `docker-compose up --build`.

## Repository structure

```text
thinkslot-mvp/
├── backend/
│   ├── __init__.py
│   └── app/
│       ├── __init__.py
│       ├── ad_catalog.py
│       ├── config.py
│       ├── main.py
│       ├── models.py
│       ├── rate_limit.py
│       └── store.py
├── examples/
│   └── test_agent.py
├── sdk/
│   ├── python/
│   │   ├── pyproject.toml
│   │   ├── README.md
│   │   └── thinkslot/
│   │       ├── __init__.py
│   │       └── client.py
│   └── typescript/
│       ├── package.json
│       ├── README.md
│       ├── tsconfig.json
│       └── src/
│           └── index.ts
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── README.md
└── requirements.txt
```

## How to demo in 60 seconds

1. Start services:

```bash
cd thinkslot-mvp
cp .env.example .env
docker-compose up --build
```

2. Open the dashboard:

- Go to `http://localhost:8000/admin`
- Keep it open to watch live sessions, revenue metrics, and events.

3. Create a session:

```bash
curl -X POST http://localhost:8000/enter \
  -H 'Content-Type: application/json' \
  -d '{
    "task_id": "demo-task-1",
    "estimated_duration_sec": 180,
    "metadata": {"framework": "langgraph", "model": "grok-4.20", "region": "AU"}
  }'
```

4. In the dashboard:

- Verify the session appears in **Sessions**.
- Click **Resume** to manually release it.
- Watch revenue cards, **Top Earners**, and **Recent Revenue Events** update live.

## Live Admin Dashboard

URL: `http://localhost:8000/admin`

Features:

- Live active sessions table:
  - `session_id`
  - `task_id`
  - `status`
  - time running
  - metadata summary
- Monetization KPI cards:
  - total monetized think-seconds today
  - revenue today
  - revenue this week
  - fill rate percent
  - live eCPM
- Top earners table (`top_10_agents_by_earnings`).
- Recent revenue events table (`recent_revenue_events`) with event-type filtering.
- Manual `Resume` button on each waiting session for easy testing.
- Live recent ads table (last 10 ads served across sessions).
- Polling refresh every 3 seconds.

## Revenue Metrics and Events

Revenue events are stored in-memory and in Redis list `thinkslot:revenue_events` (last 500):

- `thinkslot.slot_enter`
- `thinkslot.slot_exit`
- `thinkslot.ad_impression`
- `thinkslot.ad_interaction`
- `thinkslot.ad_conversion`
- `thinkslot.rebate_issued`

`GET /admin/api/dashboard` now includes:

- `total_monetized_think_seconds_today`
- `revenue_today_usd`
- `revenue_week_usd`
- `fill_rate_percent`
- `eCPM`
- `top_10_agents_by_earnings`
- `recent_revenue_events`

## API examples

### 1) Enter waiting room

```bash
curl -X POST http://localhost:8000/enter \
  -H 'Content-Type: application/json' \
  -d '{
    "task_id": "my-task-123",
    "estimated_duration_sec": 240,
    "metadata": {"domain": "finance", "region": "AU", "model": "grok-4.20"}
  }'
```

Response:

```json
{
  "session_id": "5f69f53da0f04c59ab35433f5176c4d9",
  "expires_at": "2026-02-26T21:00:00.000000+00:00"
}
```

### 2) Check session status

```bash
curl http://localhost:8000/sessions/<session_id>/status
```

Response:

```json
{"status": "waiting"}
```

### 3) Resume session (internal testing)

```bash
curl -X POST http://localhost:8000/internal/resume/<session_id> \
  -H 'x-internal-api-key: dev-internal-key'
```

Response:

```json
{"session_id": "...", "status": "resume"}
```

### 4) Stream ads

- WebSocket: `ws://localhost:8000/ws/<session_id>`
- SSE fallback: `http://localhost:8000/sse/<session_id>`

Each ad payload shape:

```json
{
  "ad_id": "string",
  "type": "sponsored_tool",
  "title": "string",
  "body": "string",
  "reward": {
    "type": "token_credit",
    "amount": 850,
    "currency": "tokens"
  },
  "action": {
    "type": "tool_call",
    "tool_name": "example_tool",
    "parameters": {}
  }
}
```

## Python SDK usage

Install editable SDK:

```bash
pip install -e sdk/python
```

Usage:

```python
from thinkslot import enter

session = await enter(
    task_id="my-task-123",
    estimated_duration_sec=240,
    metadata={"domain": "finance", "region": "AU", "model": "grok-4.20"},
)

# Option 1: simple wait
await session.wait_until_resume()

# Option 2: live ads
async for ad in session.ads_stream():
    if ad.get("reward"):
        print(f"Claimed {ad['reward']['amount']} tokens!")
```

## TypeScript SDK usage

```bash
cd sdk/typescript
npm install
npm run build
```

Usage:

```ts
import { enter } from "@thinkslot/sdk";

const session = await enter({
  task_id: "my-task-123",
  estimated_duration_sec: 240,
  metadata: { domain: "finance", region: "AU", model: "grok-4.20" }
});

await session.wait_until_resume();

for await (const ad of session.ads_stream()) {
  if (ad.reward) {
    console.log(`Claimed ${ad.reward.amount} tokens!`);
  }
}
```

## Agent integration examples

### LangGraph node wrapper

```python
from thinkslot import enter

async def long_reasoning_node(state):
    session = await enter(
        task_id=state["run_id"],
        estimated_duration_sec=180,
        metadata={"framework": "langgraph", "agent": state.get("agent_name", "unknown")},
    )

    await session.wait_until_resume()
    return await run_expensive_reasoning(state)
```

### CrewAI task wrapper

```python
from thinkslot import enter

async def run_crewai_task(task):
    session = await enter(
        task_id=task.id,
        estimated_duration_sec=300,
        metadata={"framework": "crewai", "role": task.agent.role},
    )

    async for ad in session.ads_stream():
        handle_ad(ad)
        if await should_resume(task.id):
            break

    return await task.execute_async()
```

## Example test agent script

Run:

```bash
pip install -e sdk/python
python examples/test_agent.py
```

In a second terminal, trigger resume:

```bash
curl -X POST http://localhost:8000/internal/resume/<session_id> \
  -H 'x-internal-api-key: dev-internal-key'
```

## Notes

- This MVP uses Redis + in-memory state by design.
- `/internal/resume/{session_id}` is intentionally simple and protected by `x-internal-api-key`.
- `/admin/resume/{session_id}` exists for local dashboard demo/testing convenience.
- If WebSocket is unavailable, SDKs fall back to SSE stream parsing.
