import { describe, expect, it } from "vitest";
import { PoolCoolingError, PoolThrottledError, RpcPool, type RpcPoolOptions } from "../src/pool";
import { RpcKindError } from "../src/classify";
import { KeyRateLimiter, LocalThrottleError } from "../src/limiter";

const URL_A = "https://a.example/rpc";
const URL_B = "https://b.example/rpc";

function makeClock() {
  let t = 1_000_000;
  return { now: () => t, tick: (ms: number) => { t += ms; } };
}

function twoPool(opts?: RpcPoolOptions) {
  return new RpcPool<string>(
    [
      { url: URL_A, client: "A" },
      { url: URL_B, client: "B" },
    ],
    opts,
  );
}

describe("RpcPool.call 粘性优先", () => {
  it("健康时流量恒在首选端点", async () => {
    const pool = twoPool();
    const seen: string[] = [];
    const pick = async (c: string) => { seen.push(c); return c; };
    await pool.call(pick);
    await pool.call(pick);
    expect(seen).toEqual(["A", "A"]);
  });

  it("quota：滑下家 + 冷却期跳过 + 到期半开探针 + 恢复回首选", async () => {
    const clock = makeClock();
    let aQuota = true;
    const pool = twoPool({ now: clock.now });
    const pick = async (c: string) => {
      if (c === "A" && aQuota) throw new RpcKindError("quota", "HTTP 429");
      return c;
    };
    expect(await pool.call(pick)).toBe("B");
    expect(pool.snapshot()[0]).toMatchObject({ host: "a.example", failures: 1, cooldownSec: 120 });

    clock.tick(60_000); // 冷却未到：不再打 A
    expect(await pool.call(pick)).toBe("B");

    clock.tick(60_000); // 冷却到期：放行 1 发探针，仍 quota → ×4 = 8min
    expect(await pool.call(pick)).toBe("B");
    expect(pool.snapshot()[0].cooldownSec).toBe(480);

    clock.tick(480_000); // 探针成功：完全恢复并回首选
    aQuota = false;
    expect(await pool.call(pick)).toBe("A");
    expect(pool.snapshot()[0]).toMatchObject({ failures: 0, cooldownSec: 0 });
  });

  it("rejected（RpcKindError）：不冷却不计数，每次仍先试首选", async () => {
    const pool = twoPool();
    let aCalls = 0;
    const pick = async (c: string) => {
      if (c === "A") { aCalls++; throw new RpcKindError("rejected", "method not found"); }
      return c;
    };
    expect(await pool.call(pick)).toBe("B");
    expect(await pool.call(pick)).toBe("B");
    expect(pool.snapshot()[0]).toMatchObject({ failures: 0, cooldownSec: 0 });
    expect(aCalls).toBe(2);
  });

  it("reverted（文案判定）：不冷却不计数", async () => {
    const pool = twoPool();
    const pick = async (c: string) => {
      if (c === "A") throw new Error("execution reverted");
      return c;
    };
    expect(await pool.call(pick)).toBe("B");
    expect(pool.snapshot()[0]).toMatchObject({ failures: 0, cooldownSec: 0 });
  });

  it("archive：30min 冷却", async () => {
    const clock = makeClock();
    const pool = twoPool({ now: clock.now });
    const pick = async (c: string) => {
      if (c === "A") throw new Error("archive node required");
      return c;
    };
    expect(await pool.call(pick)).toBe("B");
    expect(pool.snapshot()[0].cooldownSec).toBe(1800);
  });

  it("回调返回 null 视同 transient", async () => {
    const clock = makeClock();
    const pool = twoPool({ now: clock.now });
    expect(await pool.call(async (c) => (c === "A" ? null : c))).toBe("B");
    expect(pool.snapshot()[0].failures).toBe(1);
    expect(pool.snapshot()[0].cooldownSec).toBe(30);
  });

  it("全失败给聚合报错；再调用全冷却抛 PoolCoolingError", async () => {
    const clock = makeClock();
    const pool = twoPool({ now: clock.now });
    const boom = async () => { throw new RpcKindError("quota", "HTTP 429"); };
    await expect(pool.call(boom)).rejects.toThrow(/全部 2 端点失败/);
    const err = await pool.call(boom).catch((e) => e);
    expect(err).toBeInstanceOf(PoolCoolingError);
    expect((err as PoolCoolingError).retryAfterMs).toBeGreaterThan(0);
    expect((err as PoolCoolingError).message).toMatch(/冷却/);
  });

  it("空端点列表构造即抛错", () => {
    expect(() => new RpcPool([])).toThrow();
  });
});

describe("RpcPool 本地限速（rateLimiter 注入）", () => {
  /** sleep 注入：等待即时推进假时钟，配合 limiter 的静态 now */
  function tickingSleep(clock: ReturnType<typeof makeClock>) {
    return async (ms: number) => { clock.tick(ms); };
  }

  function throttledPoolFixture() {
    const clock = makeClock();
    const limiter = new KeyRateLimiter({ now: clock.now, sleep: tickingSleep(clock) });
    limiter.registerEndpoints([
      { url: URL_A, reqPerMin: 60 }, // 1 令牌/秒
      { url: URL_B, reqPerMin: 60 },
    ]);
    const pool = new RpcPool<string>(
      [
        { url: URL_A, client: "A" },
        { url: URL_B, client: "B" },
      ],
      { now: clock.now, rateLimiter: limiter },
    );
    return { clock, limiter, pool };
  }

  const throttledFn = (limiter: KeyRateLimiter) => async (c: string, url: string) => {
    await limiter.acquireAndWait(url, 10);
    return c;
  };

  it("两端点令牌打满 → call 抛 PoolThrottledError，ETA=逐候选 max 后取 min", async () => {
    const { clock, limiter, pool } = throttledPoolFixture();
    while (limiter.hasToken(URL_A)) await limiter.acquireAndWait(URL_A, 10);
    while (limiter.hasToken(URL_B)) await limiter.acquireAndWait(URL_B, 10);

    const err = await pool.call(throttledFn(limiter)).catch((e) => e);
    expect(err).toBeInstanceOf(PoolThrottledError);
    expect((err as PoolThrottledError).retryAfterMs).toBe(1000); // 两桶均 1s 后有令牌
  });

  it("一端点冷却、一端点限速 → ETA=逐候选 max 后 min（非双全局 min），且到期后经限速端点推进", async () => {
    const { clock, limiter, pool } = throttledPoolFixture();
    // A 吃一次 quota（冷却 120s），B 成功（消耗 1 令牌）
    const boom = async (c: string) => {
      if (c === "A") throw new RpcKindError("quota", "HTTP 402");
      return c;
    };
    expect(await pool.call(boom)).toBe("B");
    expect(pool.snapshot()[0].cooldownSec).toBe(120);
    while (limiter.hasToken(URL_B)) await limiter.acquireAndWait(URL_B, 10); // 打满 B

    // 有缺陷的双全局 min 会给出 min(min冷却=120s, min令牌=A未打满→now)=0 → 过早
    // 正确：A=max(120s, now)，B=max(0, 1s) → min=1s
    const err = await pool.call(throttledFn(limiter)).catch((e) => e);
    expect(err).toBeInstanceOf(PoolThrottledError);
    expect((err as PoolThrottledError).retryAfterMs).toBe(1000);

    clock.tick(1000); // B 拿到令牌即可服务（A 仍在冷却）
    expect(await pool.call(throttledFn(limiter))).toBe("B");
  });

  it("LocalThrottleError（含 viem 包装形态）不计失败不冷却：failures 不变、聚合为 PoolThrottledError", async () => {
    const { pool } = throttledPoolFixture();
    const wrappedThrow = async (c: string) => {
      void c;
      throw new Error("HTTP request failed.", { cause: new LocalThrottleError(5000) });
    };
    const err = await pool.call(wrappedThrow).catch((e) => e);
    expect(err).toBeInstanceOf(PoolThrottledError);
    expect((err as PoolThrottledError).retryAfterMs).toBe(5000);
    expect(pool.snapshot()[0]).toMatchObject({ failures: 0, cooldownSec: 0 });
    expect(pool.snapshot()[1]).toMatchObject({ failures: 0, cooldownSec: 0 });
  });

  it("无 rateLimiter 时行为不变（全冷却仍抛 PoolCoolingError）", async () => {
    const clock = makeClock();
    const pool = twoPool({ now: clock.now });
    await expect(pool.call(async () => { throw new RpcKindError("quota", "HTTP 402"); })).rejects.toThrow(/全部 2 端点失败/);
    await expect(pool.call(async (c) => c)).rejects.toBeInstanceOf(PoolCoolingError);
  });
});

const URL_C = "https://c.example/rpc";

describe("RpcPool.call selection=rotation", () => {
  it("活端点间轮转: 三端点依次 A→B→C→A", async () => {
    const pool = new RpcPool<string>(
      [
        { url: URL_A, client: "A" },
        { url: URL_B, client: "B" },
        { url: URL_C, client: "C" },
      ],
      { selection: "rotation" },
    );
    const seen: string[] = [];
    const pick = async (c: string) => { seen.push(c); return c; };
    for (let i = 0; i < 4; i++) await pool.call(pick);
    expect(seen).toEqual(["A", "B", "C", "A"]);
  });

  it("冷却端点当轮不占候选位, 冷却到期后轮转指针继续推进", async () => {
    const clock = makeClock();
    const pool = new RpcPool<string>(
      [
        { url: URL_A, client: "A" },
        { url: URL_B, client: "B" },
      ],
      { selection: "rotation", now: clock.now },
    );
    const seen: string[] = [];
    let bQuota = false;
    const pick = async (c: string) => {
      seen.push(c);
      if (c === "B" && bQuota) throw new RpcKindError("quota", "HTTP 429");
      return c;
    };
    expect(await pool.call(pick)).toBe("A"); // 指针0 → [A,B], A 成功
    bQuota = true;
    expect(await pool.call(pick)).toBe("A"); // 指针1 → [B,A], B quota → 冷却 → 滑 A
    expect(await pool.call(pick)).toBe("A"); // 活集=[A], B 不占候选位
    expect(await pool.call(pick)).toBe("A");
    clock.tick(120_000); // B 冷却到期(quota 基础 120s)
    bQuota = false;
    expect(await pool.call(pick)).toBe("A"); // 指针4, 活集[A,B] → start 0 → [A,B]
    expect(await pool.call(pick)).toBe("B"); // 指针5 → start 1 → [B,A]
    expect(seen).toEqual(["A", "B", "A", "A", "A", "A", "B"]);
    // 注：断言点在 B 成功调用（report("ok") 清零）之后，failures 必为 0；冷却行为已由 seen 序列证明
    expect(pool.snapshot()[1]).toMatchObject({ host: "b.example", failures: 0, cooldownSec: 0 });
  });

  it("全冷却时 PoolCoolingError 语义不变", async () => {
    const clock = makeClock();
    const pool = new RpcPool<string>([{ url: URL_A, client: "A" }], { selection: "rotation", now: clock.now });
    // 注：首败抛聚合错是 call() 既有契约（同现有 priority 用例），须 rejects 接住
    await expect(pool.call(async () => { throw new RpcKindError("quota", "HTTP 429"); })).rejects.toThrow(/全部 1 端点失败/);
    await expect(pool.call(async (c) => c)).rejects.toBeInstanceOf(PoolCoolingError);
  });
});

describe("RpcPool.call selection=priority (默认语义锁定)", () => {
  it("显式 priority 与默认一致: 恒首选", async () => {
    const pool = new RpcPool<string>(
      [
        { url: URL_A, client: "A" },
        { url: URL_B, client: "B" },
      ],
      { selection: "priority" },
    );
    const seen: string[] = [];
    const pick = async (c: string) => { seen.push(c); return c; };
    for (let i = 0; i < 3; i++) await pool.call(pick);
    expect(seen).toEqual(["A", "A", "A"]);
  });
});
