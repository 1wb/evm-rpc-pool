import { describe, expect, it, vi } from "vitest";
import { FetchRpcPool, type FetchLike, type RpcResponse } from "../src/fetch-pool";
import { PoolCoolingError } from "../src/pool";

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
function http(status: number): RpcResponse {
  return { ok: status >= 200 && status < 300, status, json: async () => ({}) };
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

  it("quota：滑下家 + 2min 冷却 + 冷却期跳过 + 到期半开探针", async () => {
    const clock = makeClock();
    let aQuota = true;
    const { fn, calls } = makeFetch({
      [URL_A]: () => (aQuota ? http(429) : ok("A")),
      [URL_B]: () => ok("B"),
    });
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn, now: clock.now });

    expect(await pool.call<string>("eth_blockNumber", [])).toBe("B");
    expect(pool.snapshot()[0]).toMatchObject({ host: "a.example", failures: 1, cooldownSec: 120 });

    clock.tick(60_000);
    expect(await pool.call<string>("eth_blockNumber", [])).toBe("B");
    expect(calls.filter((c) => c.url === URL_A)).toHaveLength(1);

    clock.tick(60_000); // 探针，仍 429 → ×4 = 8min
    expect(await pool.call<string>("eth_blockNumber", [])).toBe("B");
    expect(pool.snapshot()[0].cooldownSec).toBe(480);

    clock.tick(480_000);
    aQuota = false;
    expect(await pool.call<string>("eth_blockNumber", [])).toBe("A");
    expect(pool.snapshot()[0]).toMatchObject({ failures: 0, cooldownSec: 0 });
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
    const pool = new FetchRpcPool([URL_A, URL_B], { fetchImpl: fn });
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
