import WebSocket from "ws";

export type SessionStatus = "waiting" | "resume" | "expired";
export type AdType = "sponsored_tool" | "token_credit" | "discount" | "message";

export type Reward = {
  type: "token_credit";
  amount: number;
  currency: "tokens";
};

export type Action = {
  type: "tool_call";
  tool_name: string;
  parameters: Record<string, unknown>;
};

export type AdPayload = {
  ad_id: string;
  type: AdType;
  title: string;
  body: string;
  reward: Reward;
  action: Action;
};

export type EnterRequest = {
  task_id: string;
  estimated_duration_sec: number;
  metadata?: Record<string, unknown>;
  base_url?: string;
  request_timeout_ms?: number;
};

export class SessionExpiredError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SessionExpiredError";
  }
}

export class ThinkslotSession {
  public readonly session_id: string;
  public readonly expires_at: string;

  private readonly baseUrl: string;
  private readonly requestTimeoutMs: number;

  constructor(args: {
    session_id: string;
    expires_at: string;
    base_url: string;
    request_timeout_ms?: number;
  }) {
    this.session_id = args.session_id;
    this.expires_at = args.expires_at;
    this.baseUrl = args.base_url;
    this.requestTimeoutMs = args.request_timeout_ms ?? 10_000;
  }

  async status(): Promise<SessionStatus> {
    const response = await this.fetchWithTimeout(`${this.baseUrl}/sessions/${this.session_id}/status`);
    if (!response.ok) {
      throw new Error(`Failed to fetch session status (${response.status})`);
    }

    const payload = (await response.json()) as { status: SessionStatus };
    return payload.status;
  }

  async wait_until_resume(poll_interval_ms = 2_000): Promise<void> {
    while (true) {
      const currentStatus = await this.status();
      if (currentStatus === "resume") {
        return;
      }
      if (currentStatus === "expired") {
        throw new SessionExpiredError(`Thinkslot session ${this.session_id} expired before resume`);
      }
      await sleep(poll_interval_ms);
    }
  }

  async *ads_stream(): AsyncGenerator<AdPayload> {
    try {
      for await (const ad of this.ads_stream_ws()) {
        yield ad;
      }
      return;
    } catch {
      // WebSocket fallback to SSE.
    }

    for await (const ad of this.ads_stream_sse()) {
      yield ad;
    }
  }

  async waitUntilResume(pollIntervalMs = 2_000): Promise<void> {
    await this.wait_until_resume(pollIntervalMs);
  }

  async *adsStream(): AsyncGenerator<AdPayload> {
    for await (const ad of this.ads_stream()) {
      yield ad;
    }
  }

  private async *ads_stream_ws(): AsyncGenerator<AdPayload> {
    const wsUrl = this.baseUrl.replace(/^http/, "ws") + `/ws/${this.session_id}`;
    const ws = new WebSocket(wsUrl);

    await new Promise<void>((resolve, reject) => {
      const onOpen = () => {
        cleanup();
        resolve();
      };
      const onError = (error: Error) => {
        cleanup();
        reject(error);
      };
      const cleanup = () => {
        ws.off("open", onOpen);
        ws.off("error", onError);
      };

      ws.on("open", onOpen);
      ws.on("error", onError);
    });

    const queue: Array<AdPayload | { status: SessionStatus }> = [];
    let closed = false;
    let streamError: Error | null = null;
    let wake: (() => void) | null = null;

    ws.on("message", (chunk) => {
      const text = typeof chunk === "string" ? chunk : chunk.toString("utf-8");
      const payload = JSON.parse(text) as AdPayload | { status: SessionStatus };
      queue.push(payload);
      if (wake) {
        wake();
        wake = null;
      }
    });

    ws.on("close", () => {
      closed = true;
      if (wake) {
        wake();
        wake = null;
      }
    });

    ws.on("error", (error) => {
      streamError = error;
      closed = true;
      if (wake) {
        wake();
        wake = null;
      }
    });

    try {
      while (true) {
        while (queue.length > 0) {
          const payload = queue.shift();
          if (!payload) {
            continue;
          }
          if ("status" in payload) {
            return;
          }
          yield payload;
        }

        if (closed) {
          if (streamError) {
            throw streamError;
          }
          return;
        }

        await new Promise<void>((resolve) => {
          wake = resolve;
        });
      }
    } finally {
      ws.close();
    }
  }

  private async *ads_stream_sse(): AsyncGenerator<AdPayload> {
    const response = await this.fetchWithTimeout(`${this.baseUrl}/sse/${this.session_id}`, {
      headers: { Accept: "text/event-stream" },
    });

    if (!response.ok || !response.body) {
      throw new Error(`SSE stream failed (${response.status})`);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        return;
      }

      buffer += decoder.decode(value, { stream: true });
      while (buffer.includes("\n\n")) {
        const index = buffer.indexOf("\n\n");
        const eventBlock = buffer.slice(0, index);
        buffer = buffer.slice(index + 2);

        const dataLines = eventBlock
          .split(/\r?\n/)
          .filter((line) => line.startsWith("data:"))
          .map((line) => line.slice(5).trim());

        if (dataLines.length === 0) {
          continue;
        }

        const payload = JSON.parse(dataLines.join("\n")) as AdPayload | { status: SessionStatus };
        if ("status" in payload) {
          return;
        }
        yield payload;
      }
    }
  }

  private fetchWithTimeout(url: string, init: RequestInit = {}): Promise<Response> {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.requestTimeoutMs);

    return fetch(url, { ...init, signal: controller.signal })
      .finally(() => {
        clearTimeout(timeoutId);
      });
  }
}

export async function enter(args: EnterRequest): Promise<ThinkslotSession> {
  const {
    task_id,
    estimated_duration_sec,
    metadata = {},
    base_url = process.env.THINKSLOT_BASE_URL ?? "http://localhost:8000",
    request_timeout_ms = 10_000,
  } = args;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), request_timeout_ms);

  const response = await fetch(`${base_url.replace(/\/$/, "")}/enter`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      task_id,
      estimated_duration_sec,
      metadata,
    }),
    signal: controller.signal,
  }).finally(() => clearTimeout(timeoutId));

  if (!response.ok) {
    throw new Error(`Failed to enter Thinkslot session (${response.status})`);
  }

  const payload = (await response.json()) as { session_id: string; expires_at: string };
  return new ThinkslotSession({
    session_id: payload.session_id,
    expires_at: payload.expires_at,
    base_url: base_url.replace(/\/$/, ""),
    request_timeout_ms,
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
