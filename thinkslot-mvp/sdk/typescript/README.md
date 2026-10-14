# Thinkslot TypeScript SDK

```ts
import { enter } from "@thinkslot/sdk";

const session = await enter({
  task_id: "my-task-123",
  estimated_duration_sec: 240,
  metadata: { domain: "finance", region: "AU", model: "grok-4.20" }
});

await session.wait_until_resume();
```
