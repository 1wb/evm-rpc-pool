import { describe, expect, it, vi } from "vitest";
import { FetchRpcPool, type FetchLike, type RpcResponse } from "../src/fetch-pool";
import { PoolCoolingError, PoolThrottledError } from "../src/pool";
import { LocalThrottleError } from "../src/limiter";

// ---------- 假 fetch + 假时钟 ----------

type Body = { method: string; params: unknown[] };
interface Call { url: string; body: Body }
type Handler = (body: Body) => RpcResponse;

function ok(result: unknown): RpcResponse {
  return { ok: true, status: 200, json: async () => ({ jsonrpc: "2.0", id: 1, result }) };
}
function rpcError(message: string): RpcResponse {
  return { ok: true, status: 200, json: async () => ({ jsonrpc: "2.0", id: 1, error: { code: -32005, message } }) };
}
function http(status: number, headers?: Record<string, string>): RpcResponse {
  const h = new Map(Object.entries(headers ?? {}));
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => ({}),
    headers: { get: (name: string) => h.get(name.toLowerCase()) ?? null },
  };
}

function makeFetch(handlers: Record<string, Handler>) {
  const calls: Call[] = [];
  const fn: FetchLike = async (url, init) => {
    const body = JSON.parse(init.body) as Body;
    calls.push({ url, body });
    const h = handlers[url];
    if (!h) throw new Error(`unexpected fetch: ${url}`);
    return h(body);
  };
  return { fn, calls };
}

function makeClock() {
  let t = 1_000_000;
  return { now: () => t, tick: (ms: number) => { t += ms; } };
}

const URL_A = "https://a.example/rpc";
const URL_B = "https://b.example/rpc";
const URL_NODE = "https://base-rpc.publicnode.com";

function hex(n: number): string { return "0x" + n.toString(16); }
function getLogsParams(body: Body): { from: number; to: number } {
  expect(body.method).toBe("eth_getLogs");
  const [filter] = body.params as [{ fromBlock: string; toBlock: string }];
  return { from: parseInt(filter.fromBlock, 16), to: parseInt(filter.toBlock, 16) };
}

// ---------- 通用调用 ----------

describe("FetchRpcPool.call", () => {
  it("粘性优先：健康时流量恒在首选端点", async () => {
    const { fn, calls } = makeFetch({ [URL_A]: () => ok("A"), [URL_B]: () => ok("B") });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn });
    expect(await pool.call<string>("eth_blockNumber", [])).toBe("A");
    expect(await pool.call<string>("eth_blockNumber", [])).toBe("A");
    expect(calls.map((c) => c.url)).toEqual([URL_A, URL_A]);
  });

  it("quota（402）：滑下家 + 2min 冷却 + 冷却期跳过 + 到期半开探针", async () => {
    const clock = makeClock();
    let aQuota = true;
    const { fn, calls } = makeFetch({
      [URL_A]: () => (aQuota ? http(402) : ok("A")),
      [URL_B]: () => ok("B"),
    });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn, now: clock.now });

    expect(await pool.call<string>("eth_blockNumber", [])).toBe("B");
    expect(pool.snapshot()[0]).toMatchObject({ host: "a.example", failures: 1, cooldownSec: 120 });

    clock.tick(60_000);
    expect(await pool.call<string>("eth_blockNumber", [])).toBe("B");
    expect(calls.filter((c) => c.url === URL_A)).toHaveLength(1);

    clock.tick(60_000); // 探针，仍 402 → ×4 = 8min
    expect(await pool.call<string>("eth_blockNumber", [])).toBe("B");
    expect(pool.snapshot()[0].cooldownSec).toBe(480);

    clock.tick(480_000);
    aQuota = false;
    expect(await pool.call<string>("eth_blockNumber", [])).toBe("A");
    expect(pool.snapshot()[0]).toMatchObject({ failures: 0, cooldownSec: 0 });
  });

  it("429 + Retry-After 头：冷却直取 45s（聚合错误不出池，breaker 行为是可观察面）", async () => {
    const { fn } = makeFetch({ [URL_A]: () => http(429, { "retry-after": "45" }) });
    const pool = new FetchRpcPool([URL_A], { fetchImpl: fn });
    await expect(pool.call<string>("eth_blockNumber", [])).rejects.toThrow(/全部 1 端点失败/);
    expect(pool.snapshot()[0].cooldownSec).toBe(45); // 直取，非 rate 档 30s 阶梯
  });

  it("rate（429）：短梯 30s 起步 ×2——短时限流不再被停用 6 小时", async () => {
    const clock = makeClock();
    let aRate = true;
    const { fn } = makeFetch({
      [URL_A]: () => (aRate ? http(429) : ok("A")),
      [URL_B]: () => ok("B"),
    });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn, now: clock.now });

    expect(await pool.call<string>("eth_blockNumber", [])).toBe("B");
    expect(pool.snapshot()[0]).toMatchObject({ failures: 1, cooldownSec: 30 });

    clock.tick(30_000); // 冷却到期，A 放行为探针
    expect(await pool.call<string>("eth_blockNumber", [])).toBe("B");
    expect(pool.snapshot()[0].cooldownSec).toBe(60); // 30×2
  });

  it("rejected（200 带 JSON-RPC error）：不冷却不计数，仅滑下家", async () => {
    const { fn, calls } = makeFetch({
      [URL_A]: () => rpcError("method not found"),
      [URL_B]: () => ok("B"),
    });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn });
    expect(await pool.call<string>("eth_call", [])).toBe("B");
    expect(await pool.call<string>("eth_call", [])).toBe("B");
    expect(pool.snapshot()[0]).toMatchObject({ failures: 0, cooldownSec: 0 });
    expect(calls.filter((c) => c.url === URL_A)).toHaveLength(2);
  });

  it("reverted（200 带 JSON-RPC error 文案）：不冷却不计数", async () => {
    const { fn } = makeFetch({ [URL_A]: () => rpcError("execution reverted"), [URL_B]: () => ok("B") });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn });
    expect(await pool.call<string>("eth_call", [])).toBe("B");
    expect(pool.snapshot()[0]).toMatchObject({ failures: 0, cooldownSec: 0 });
  });

  it("null 结果视同 transient 滑下家", async () => {
    const { fn } = makeFetch({ [URL_A]: () => ok(null), [URL_B]: () => ok("B") });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn });
    expect(await pool.call<string>("eth_getBlockByNumber", ["0x1", false])).toBe("B");
    expect(pool.snapshot()[0].failures).toBe(1);
  });

  it("全失败聚合报错；再调用抛 PoolCoolingError", async () => {
    const clock = makeClock();
    const { fn } = makeFetch({ [URL_A]: () => http(429), [URL_B]: () => http(429) });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn, now: clock.now });
    await expect(pool.call("eth_blockNumber", [])).rejects.toThrow(/全部 2 端点失败/);
    const err = await pool.call("eth_blockNumber", []).catch((e) => e);
    expect(err).toBeInstanceOf(PoolCoolingError);
  });

  it("200 + JSON-RPC rate limit 文案 → rate 短梯 30s（不再落入不冷却的 rejected）", async () => {
    const clock = makeClock();
    const { fn } = makeFetch({
      [URL_A]: () => rpcError("rate limit exceeded"),
      [URL_B]: () => ok("B"),
    });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn, now: clock.now });
    expect(await pool.call<string>("eth_blockNumber", [])).toBe("B");
    expect(pool.snapshot()[0]).toMatchObject({ failures: 1, cooldownSec: 30 });
  });

  it("fetchImpl 抛 LocalThrottleError（含包装 cause）时原样透传为 PoolThrottledError，端点零冷却", async () => {
    const pool = new FetchRpcPool([URL_A], {
      fetchImpl: async () => { throw new Error("wrapped", { cause: new LocalThrottleError(2500) }); },
    });
    const err = await pool.call("eth_blockNumber", []).catch((e) => e);
    expect(err).toBeInstanceOf(PoolThrottledError);
    expect((err as PoolThrottledError).retryAfterMs).toBe(2500);
    expect(pool.snapshot()[0]).toMatchObject({ failures: 0, cooldownSec: 0 });
  });
});

// ---------- getLogs ----------

describe("FetchRpcPool.getLogs", () => {
  it("hostCaps 注入：能力表跳过不支持 topic 的端点；通用调用不受影响", async () => {
    const { fn, calls } = makeFetch({ [URL_NODE]: () => ok("N"), [URL_A]: () => ok([{ blockNumber: hex(1) }]) });
    const pool = new FetchRpcPool([URL_NODE, URL_A], {
      fetchImpl: fn,
      hostCaps: { "base-rpc.publicnode.com": { topicLogs: false } },
    });
    expect(await pool.call<string>("eth_blockNumber", [])).toBe("N");
    const logs = await pool.getLogs<{ blockNumber: string }>({ address: "0xp", topics: ["0xt"], fromBlock: 0, toBlock: 99 });
    expect(logs).toEqual([{ blockNumber: hex(1) }]);
    expect(calls.filter((c) => c.body.method === "eth_getLogs").map((c) => c.url)).toEqual([URL_A]);
  });

  it("按文案提取上限一次切到位并记忆，之后预分块不再试错", async () => {
    const { fn, calls } = makeFetch({
      [URL_A]: (body) => {
        const { from, to } = getLogsParams(body);
        if (to - from + 1 > 3000) return rpcError("Query returned more than 3000 blocks range");
        return ok([{ from }, { to }]);
      },
    });
    const pool = new FetchRpcPool([URL_A], { fetchImpl: fn });
    const logs = await pool.getLogs<unknown>({ address: "0xp", topics: ["0xt"], fromBlock: 0, toBlock: 9999 });
    expect(logs).toHaveLength(8);
    expect(pool.snapshot()[0].maxLogRange).toBe(3000n);

    const failed = calls.length;
    await pool.getLogs({ address: "0xp", topics: ["0xt"], fromBlock: 0, toBlock: 9999 });
    expect(calls.length - failed).toBe(4);
  });

  it("读不出上限对半二分，拆到下限仍失败滑下家；bigint 入参同样可用", async () => {
    const { fn, calls } = makeFetch({
      [URL_A]: () => rpcError("block range too wide"),
      [URL_B]: (body) => {
        const { from, to } = getLogsParams(body);
        expect([from, to]).toEqual([0, 9999]);
        return ok([{ full: true }]);
      },
    });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn, minChunk: 2000n }); // 锁旧地板场景
    const logs = await pool.getLogs<{ full: boolean }>({ address: "0xp", topics: ["0xt"], fromBlock: 0n, toBlock: 9999n });
    expect(logs).toEqual([{ full: true }]);
    const spans = calls
      .filter((c) => c.url === URL_A)
      .map((c) => { const { from, to } = getLogsParams(c.body); return to - from + 1; });
    expect(spans).toEqual([10000, 5000, 2500, 2000]);
  });

  it("quota 不触发拆分，直接滑下家", async () => {
    const { fn, calls } = makeFetch({
      [URL_A]: () => http(429),
      [URL_B]: (body) => { const { from, to } = getLogsParams(body); expect([from, to]).toEqual([0, 9999]); return ok([{ b: 1 }]); },
    });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn });
    const logs = await pool.getLogs<unknown>({ address: "0xp", topics: ["0xt"], fromBlock: 0, toBlock: 9999 });
    expect(logs).toEqual([{ b: 1 }]);
    expect(calls.filter((c) => c.url === URL_A)).toHaveLength(1);
  });

  it("from > to 返回空；请求体用 hex 编码块号", async () => {
    const { fn, calls } = makeFetch({ [URL_A]: () => ok([]) });
    const pool = new FetchRpcPool([URL_A], { fetchImpl: fn });
    expect(await pool.getLogs({ address: "0xp", topics: ["0xt"], fromBlock: 10, toBlock: 9 })).toEqual([]);
    expect(calls).toHaveLength(0);
    await pool.getLogs({ address: "0xp", topics: ["0xt"], fromBlock: 255, toBlock: 256 });
    const filter = (calls[0].body.params as [Record<string, string>])[0];
    expect(filter.fromBlock).toBe("0xff");
    expect(filter.toBlock).toBe("0x100");
  });

  it("200 + free plan rate limit → quota 长梯 120s；带 upgrade here 尾巴的 range 文案 → 拆片不冷却", async () => {
    const clock = makeClock();
    const { fn } = makeFetch({
      [URL_A]: () => rpcError("You reached free plan rate limit"),
      [URL_B]: (body) => {
        const { from, to } = getLogsParams(body);
        if (to - from + 1 > 10) return rpcError("Under your current plan, up to 10 blocks allowed. Upgrade here");
        return ok([{ ok: true }]);
      },
    });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn, now: clock.now });
    const logs = await pool.getLogs<{ ok: boolean }>({ address: "0xp", topics: ["0xt"], fromBlock: 0, toBlock: 29 });
    expect(logs).toHaveLength(3);
    expect(pool.snapshot()[0]).toMatchObject({ failures: 1, cooldownSec: 120 }); // URL_A: quota
    expect(pool.snapshot()[1]).toMatchObject({ failures: 0, cooldownSec: 0, maxTopicRange: 10n }); // URL_B: range
  });
});

// ---------- getLogs lane 自动派生 ----------

describe("getLogs lane 自动派生", () => {
  it("带 topic → topic lane（吃 topicLogRange caps）；空 topics → address lane", async () => {
    const topicCalls: number[] = [];
    const addrCalls: number[] = [];
    const { fn } = makeFetch({
      [URL_A]: (body) => {
        const { from, to } = getLogsParams(body);
        const hasTopics = ((body.params as [Record<string, unknown>])[0].topics as unknown[]).length > 0;
        const span = to - from + 1;
        if (hasTopics) { topicCalls.push(span); if (span > 3000) return rpcError("too wide"); }
        else { addrCalls.push(span); if (span > 1000) return rpcError("too wide"); }
        return ok([]);
      },
    });
    const pool = new FetchRpcPool([URL_A], {
      fetchImpl: fn,
      hostCaps: { "a.example": { topicLogRange: 3000, addressLogRange: 1000 } },
    });
    await pool.getLogs({ address: "0xp", topics: ["0xt"], fromBlock: 0, toBlock: 9999 });
    await pool.getLogs({ address: "0xp", topics: [], fromBlock: 0, toBlock: 9999 });
    expect(topicCalls.every((s) => s <= 3000)).toBe(true);
    expect(addrCalls.every((s) => s <= 1000)).toBe(true);
  });
});

// ---------- 工厂层专项 ----------

describe("FetchRpcPool 工厂", () => {
  it("默认使用 globalThis.fetch（bind 绑定）", async () => {
    const stub = vi.fn(async () => ok("G"));
    vi.stubGlobal("fetch", stub);
    try {
      const pool = new FetchRpcPool([URL_A]);
      expect(await pool.call<string>("eth_blockNumber", [])).toBe("G");
      expect(stub).toHaveBeenCalledTimes(1);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("单发超时：挂起的 fetch 被中止后按 transient 滑动", async () => {
    const slow: FetchLike = (_url, init) =>
      new Promise<RpcResponse>((_, rej) => {
        init.signal?.addEventListener("abort", () => rej(new Error("aborted")));
      });
    const pool = new FetchRpcPool([URL_A], { fetchImpl: slow, timeoutMs: 20 });
    await expect(pool.call("eth_blockNumber", [])).rejects.toThrow(/全部 1 端点失败/);
  });

  it("空 urls 构造即抛错", () => {
    expect(() => new FetchRpcPool([], { fetchImpl: async () => ok("x") })).toThrow();
  });
});

// ---------- 可空 JSON-RPC 方法（spec §5.4） ----------

describe("FetchRpcPool 可空方法", () => {
  it("默认可空方法（eth_getTransactionReceipt / eth_getTransactionByHash）返回 null 不冷却且直接返回 null", async () => {
    const { fn, calls } = makeFetch({ [URL_A]: () => ok(null), [URL_B]: () => ok("B") });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn });
    expect(await pool.call("eth_getTransactionReceipt", ["0xdead"])).toBeNull();
    expect(await pool.call("eth_getTransactionByHash", ["0xdead"])).toBeNull();
    expect(calls).toHaveLength(2); // 未滑到 URL_B
    expect(pool.snapshot()[0]).toMatchObject({ failures: 0, cooldownSec: 0 });
  });

  it("可空方法遇到缺失 result 键的畸形响应 {} 仍按 transient 滑下家（Review Focus #4）", async () => {
    const { fn } = makeFetch({
      [URL_A]: () => ({ ok: true, status: 200, json: async () => ({ jsonrpc: "2.0", id: 1 }) }),
      [URL_B]: () => ok(null),
    });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn });
    expect(await pool.call("eth_getTransactionReceipt", ["0xdead"])).toBeNull();
    expect(pool.snapshot()[0].failures).toBe(1); // URL_A 缺 result 记故障
    expect(pool.snapshot()[1].failures).toBe(0); // URL_B 返回合法 null 成功
  });

  it("nullableMethods 构造覆盖与单次 call({ allowNull }) 优先级生效", async () => {
    const { fn } = makeFetch({ [URL_A]: () => ok(null), [URL_B]: () => ok("B") });
    // 构造覆盖：白名单清空 → null 不放行，按 transient 滑下家
    const strictPool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn, nullableMethods: [] });
    expect(await strictPool.call("eth_getTransactionReceipt", ["0xdead"])).toBe("B");
    // 单次优先级：fresh pool（同样空白名单）上显式 allowNull 仍单次放行 null
    // （同池连断言会因首断言的 transient 冷却跳过 URL_A，无法到达放行路径）
    const perCallPool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn, nullableMethods: [] });
    expect(await perCallPool.call("eth_getBlockByNumber", ["0x1", false], { allowNull: true })).toBeNull();
  });
});

// ---------- getLogs 过滤与通道升级（spec §5.5） ----------

describe("FetchRpcPool.getLogs 过滤与通道升级", () => {
  it("getLogs 支持省略/空串/空数组 address、地址数组 OR、嵌套 topics OR 及显式 lane 覆盖", async () => {
    const { fn, calls } = makeFetch({
      [URL_NODE]: () => ok([{ n: 1 }]),
      [URL_A]: () => ok([{ a: 1 }]),
    });
    const pool = new FetchRpcPool([URL_NODE, URL_A], {
      fetchImpl: fn,
      hostCaps: { "base-rpc.publicnode.com": { topicLogRange: 0, addressLogRange: 1000 } },
    });
    // 1. 纯 topic 流（空串 address → 出站省略 address 键，走 topic lane → 跳过 URL_NODE 落 URL_A）
    await pool.getLogs({ address: "", topics: [["0xt0", "0xt1"]], fromBlock: 1, toBlock: 10 });
    const f1 = (calls[0].body.params as [Record<string, unknown>])[0];
    expect("address" in f1).toBe(false);
    expect(f1.topics).toEqual([["0xt0", "0xt1"]]);
    expect(calls[0].url).toBe(URL_A);

    // 2. 多地址 + 嵌套 topics + 显式 lane="address" → 命中 URL_NODE(addressLogRange=1000)
    await pool.getLogs({ address: ["0xp1", "0xp2"], topics: [["0xt0"]], fromBlock: 1, toBlock: 10, lane: "address" });
    const f2 = (calls[1].body.params as [Record<string, unknown>])[0];
    expect(f2.address).toEqual(["0xp1", "0xp2"]);
    expect(calls[1].url).toBe(URL_NODE);
  });
});
