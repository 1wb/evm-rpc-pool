import { describe, expect, it } from "vitest";
import { KeyRateLimiter } from "../src/limiter";
import { meteredFetch, PoolMeter } from "../src/meter";

const URL_A = "https://a.example/rpc";

function jsonRes(body: unknown, status = 200, headers?: Record<string, string>): Response {
  return new Response(JSON.stringify(body), { status, headers });
}

function fakeBase(responses: Response[], log: string[]): typeof fetch {
  let i = 0;
  return (async (input: RequestInfo | URL) => {
    log.push(String(typeof input === "string" ? input : input instanceof URL ? input.href : (input as Request).url));
    const res = responses[Math.min(i, responses.length - 1)];
    i += 1;
    return res;
  }) as typeof fetch;
}

function rpcInit(method: string): RequestInit {
  return { method: "POST", body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params: [] }) } as RequestInit;
}

const settle = () => new Promise((r) => setTimeout(r, 0));

describe("PoolMeter", () => {
  it("按 endpointId×方法聚合 requests/ok/429/402/rpcError/otherErrors/localThrottled 与 push", () => {
    const meter = new PoolMeter();
    meter.registerEndpoints([URL_A]);
    meter.record(URL_A, { method: "eth_getLogs", status: 429 });
    meter.record(URL_A, { method: "eth_call", rpcError: true });
    meter.record(URL_A, { method: "eth_call", status: 200 });
    meter.record(URL_A, { method: "eth_call", status: 503 });
    meter.record(URL_A, { method: "eth_call", localThrottled: true }); // 未发请求：不计 requests
    meter.recordPush(URL_A);
    meter.recordPush(URL_A);
    meter.recordPush(URL_A);
    const u = meter.snapshotDelta()[URL_A];
    expect(u.push).toBe(3);
    expect(u.methods.eth_getLogs).toMatchObject({ requests: 1, r429: 1 });
    expect(u.methods.eth_call).toMatchObject({
      requests: 3,
      ok: 1,
      rpcError: 1,
      otherErrors: 1,
      localThrottled: 1,
    });
  });

  it("snapshotDelta 窗口增量：confirm 后归零、新事件只出现在下一窗口", () => {
    const meter = new PoolMeter();
    meter.registerEndpoints([URL_A]);
    meter.record(URL_A, { method: "eth_call", status: 200 });
    expect(meter.snapshotDelta()[URL_A].methods.eth_call.requests).toBe(1);
    meter.confirmDelta();
    expect(meter.snapshotDelta()[URL_A]).toBeUndefined();
    meter.record(URL_A, { method: "eth_call", status: 200 });
    expect(meter.snapshotDelta()[URL_A].methods.eth_call.requests).toBe(1);
  });

  it("URL×方法分格互不串扰", () => {
    const meter = new PoolMeter();
    meter.record(URL_A, { method: "eth_call", status: 200 });
    meter.record("https://b.example/rpc", { method: "eth_getLogs", status: 429 });
    const d = meter.snapshotDelta();
    expect(d[URL_A].methods.eth_call.ok).toBe(1);
    expect(d["https://b.example/rpc"].methods.eth_getLogs.r429).toBe(1);
  });
});

describe("meteredFetch", () => {
  it("200 + JSON-RPC error body → rpcError 计数且不算 ok；200 + result → ok", async () => {
    const meter = new PoolMeter();
    const limiter = new KeyRateLimiter({});
    const log: string[] = [];
    const f = meteredFetch(meter, limiter, fakeBase([jsonRes({ jsonrpc: "2.0", id: 1, error: { code: -32000, message: "boom" } })], log));
    const res = await f(URL_A, rpcInit("eth_call"));
    expect(res.status).toBe(200);
    await settle();
    expect(log).toEqual([URL_A]);
    expect(meter.snapshotDelta()[URL_A].methods.eth_call).toMatchObject({ requests: 1, rpcError: 1, ok: 0 });

    meter.confirmDelta();
    const f2 = meteredFetch(meter, limiter, fakeBase([jsonRes({ jsonrpc: "2.0", id: 1, result: "0x1" })], []));
    await f2(URL_A, rpcInit("eth_call"));
    await settle();
    expect(meter.snapshotDelta()[URL_A].methods.eth_call).toMatchObject({ requests: 1, ok: 1, rpcError: 0 });
  });

  it("429 + retry-after 头 → limiter.setPenalty 边带被调（breaker 冷却随之）且计 r429", async () => {
    const meter = new PoolMeter();
    const limiter = new KeyRateLimiter({});
    const seen: Array<{ url: string; untilMs: number }> = [];
    limiter.addPenaltyListener((url, untilMs) => seen.push({ url, untilMs }));
    const f = meteredFetch(meter, limiter, fakeBase([jsonRes("rate limited", 429, { "retry-after": "45" })], []));
    await f(URL_A, rpcInit("eth_getLogs"));
    expect(seen).toHaveLength(1);
    expect(seen[0].url).toBe(URL_A);
    expect(seen[0].untilMs - Date.now()).toBeGreaterThan(40_000);
    expect(meter.snapshotDelta()[URL_A].methods.eth_getLogs).toMatchObject({ requests: 1, r429: 1 });
  });

  it("本地无令牌 → 不发请求（base 不被调）、计 localThrottled、抛 LocalThrottleError", async () => {
    const meter = new PoolMeter();
    const limiter = new KeyRateLimiter({ acquireWaitMs: 10 });
    limiter.registerEndpoints([{ url: URL_A, reqPerMin: 60 }]);
    while (limiter.hasToken(URL_A)) await limiter.acquireAndWait(URL_A, 10);
    const log: string[] = [];
    const f = meteredFetch(meter, limiter, fakeBase([jsonRes({ jsonrpc: "2.0", id: 1, result: "0x1" })], log));
    await expect(f(URL_A, rpcInit("eth_call"))).rejects.toThrow(/本地限速/);
    expect(log).toEqual([]);
    expect(meter.snapshotDelta()[URL_A].methods.eth_call).toMatchObject({ localThrottled: 1, requests: 0 });
  });
});
