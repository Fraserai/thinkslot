from __future__ import annotations

import asyncio
import contextlib

from thinkslot import enter


async def main() -> None:
    session = await enter(
        task_id="my-task-123",
        estimated_duration_sec=240,
        metadata={"domain": "finance", "region": "AU", "model": "grok-4.20"},
    )

    async def consume_ads() -> None:
        async for ad in session.ads_stream():
            if ad.get("reward"):
                print(f"Claimed {ad['reward']['amount']} tokens! ({ad['title']})")

    ads_task = asyncio.create_task(consume_ads())

    try:
        await session.wait_until_resume()
        print("Session resumed. Returning control to agent.")
    finally:
        ads_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ads_task


if __name__ == "__main__":
    asyncio.run(main())
