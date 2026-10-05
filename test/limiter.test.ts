import { describe, expect, it } from "vitest";
import { isLocalThrottle, KeyRateLimiter, LocalThrottleError } from "../src/limiter";

const URL_A = "https://a.example/rpc";
const URL_B = "https://b.example/rpc";
const URL_C = "https://c.example/rpc";

function makeClock() {
  let t = 1_000_000;
  return { now: () => t, tick: (ms: number) => { t += ms; } };
}

/** sleep 注入：等待即时推进假时钟（与 now 同源），避免真实定时器 */
function tickingSleep(clock: ReturnType<typeof makeClock>) {
  return async (ms: number) => { clock.tick(ms); };
}

describe("KeyRateLimiter 令牌桶", () => {
  it("capacity 内连发立即可得；打满后 acquireAndWait 超时抛 LocalThrottleError（retryAfterMs>0）；恢复后可得", async () => {
    const clock = makeClock();
    const lim = new KeyRateLimiter({ now: clock.now, sleep: tickingSleep(clock) });
    lim.registerEndpoints([{ url: URL_A, reqPerMin: 60 }]); // 60/min = 1 令牌/秒

    for (let i = 0; i < 5; i++) await lim.acquireAndWait(URL_A, 10); // capacity 5：全立即可得
    let err: unknown;
    try {
      await lim.acquireAndWait(URL_A, 10);
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(LocalThrottleError);
    expect((err as LocalThrottleError).retryAfterMs).toBe(1000); // 下一令牌 1s 后

    clock.tick(1000);
    await lim.acquireAndWait(URL_A, 10); // 恢复 1 令牌后可得
  });

  it("maxWaitMs 充足时等待至令牌可用（等待型获取，不抛错）", async () => {
    const clock = makeClock();
    const lim = new KeyRateLimiter({ now: clock.now, sleep: tickingSleep(clock) });
    lim.registerEndpoints([{ url: URL_A, reqPerMin: 60 }]);
    for (let i = 0; i < 5; i++) await lim.acquireAndWait(URL_A, 10);
    await lim.acquireAndWait(URL_A, 5_000); // 等 1s 内有令牌：等待而非抛错
    expect(clock.now()).toBe(1_001_000);
  });

  it("同 budget 两 URL 共享桶：urlA 打满后 urlB 也限速；不同 budget 互不影响", async () => {
    const clock = makeClock();
    const lim = new KeyRateLimiter({ now: clock.now, sleep: tickingSleep(clock) });
    lim.registerEndpoints([
      { url: URL_A, budget: "infura", reqPerMin: 60 },
      { url: URL_B, budget: "infura", reqPerMin: 60 },
      { url: URL_C, budget: "drpc", reqPerMin: 60 },
    ]);
    while (lim.hasToken(URL_A)) await lim.acquireAndWait(URL_A, 10);
    let err: unknown;
    try {
      await lim.acquireAndWait(URL_B, 10);
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(LocalThrottleError);
    await lim.acquireAndWait(URL_C, 10); // 独立桶不受影响
  });

  it("hasToken 查询不消费；reqPerMin=0 不限速", async () => {
    const clock = makeClock();
    const lim = new KeyRateLimiter({ now: clock.now });
    lim.registerEndpoints([{ url: URL_A, reqPerMin: 0 }]);
    for (let i = 0; i < 100; i++) {
      expect(lim.hasToken(URL_A)).toBe(true);
      await lim.acquireAndWait(URL_A, 10);
    }
  });

  it("未登记 URL 视为不限速（防御：漏登记不致不可用）", async () => {
    const lim = new KeyRateLimiter({});
    await lim.acquireAndWait("https://unknown.example/rpc", 10);
    expect(lim.hasToken("https://unknown.example/rpc")).toBe(true);
  });

  it("urlAlias 启动预登记按 host 计数 host#kN，重复登记保持首见别名", () => {
    const lim = new KeyRateLimiter({});
    lim.registerEndpoints([
      { url: "https://lb.drpc.live/aaa" },
      { url: "https://a.example/rpc" },
      { url: "https://lb.drpc.live/bbb" },
    ]);
    expect(lim.urlAlias("https://lb.drpc.live/aaa")).toBe("lb.drpc.live#k1");
    expect(lim.urlAlias("https://a.example/rpc")).toBe("a.example#k1");
    expect(lim.urlAlias("https://lb.drpc.live/bbb")).toBe("lb.drpc.live#k2");
    lim.registerEndpoints([{ url: "https://lb.drpc.live/aaa" }]);
    expect(lim.urlAlias("https://lb.drpc.live/aaa")).toBe("lb.drpc.live#k1");
  });

  it("setPenalty 写入桶并触发监听；penalty 期间 acquire 不可得；过期自然失效", async () => {
    const clock = makeClock();
    const penalties: Array<{ url: string; untilMs: number }> = [];
    const lim = new KeyRateLimiter({
      now: clock.now,
      sleep: tickingSleep(clock),
      onPenalty: (url, untilMs) => penalties.push({ url, untilMs }),
    });
    lim.registerEndpoints([{ url: URL_A, reqPerMin: 60 }]);
    lim.setPenalty(URL_A, clock.now() + 30_000);
    lim.setPenalty(URL_A, clock.now() + 10_000); // 重复取更晚
    expect(penalties).toEqual([
      { url: URL_A, untilMs: 1_030_000 },
      { url: URL_A, untilMs: 1_010_000 },
    ]);
    expect(lim.hasToken(URL_A)).toBe(false);
    expect(lim.earliestTokenRecovery(URL_A)).toBe(1_030_000);
    let err: unknown;
    try {
      await lim.acquireAndWait(URL_A, 10);
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(LocalThrottleError);
    expect((err as LocalThrottleError).retryAfterMs).toBe(30_000);

    clock.tick(30_000);
    await lim.acquireAndWait(URL_A, 10); // penalty 过期后恢复
  });
});

describe("isLocalThrottle cause 链识别", () => {
  it("顶层即 LocalThrottleError", () => {
    expect(isLocalThrottle(new LocalThrottleError(1000))).toBe(true);
  });
  it("viem 包装一层（cause 指向）仍识别", () => {
    const inner = new LocalThrottleError(1000);
    const wrapped = new Error("HTTP request failed.", { cause: inner });
    expect(isLocalThrottle(wrapped)).toBe(true);
  });
  it("按 name 兜底（双副本/跨 realm instanceof 失效）", () => {
    const fake = new Error("本地限速");
    fake.name = "LocalThrottleError";
    expect(isLocalThrottle(fake)).toBe(true);
  });
  it("普通错误与非 Error 值返回 false", () => {
    expect(isLocalThrottle(new Error("x"))).toBe(false);
    expect(isLocalThrottle(new Error("x", { cause: new Error("y") }))).toBe(false);
    expect(isLocalThrottle(undefined)).toBe(false);
    expect(isLocalThrottle("boom")).toBe(false);
  });
});
