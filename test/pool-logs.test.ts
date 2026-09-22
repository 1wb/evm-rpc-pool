import { describe, expect, it } from "vitest";
import { RpcPool, type RpcPoolEntry } from "../src/pool";

const URL_A = "https://a.example/rpc";
const URL_B = "https://b.example/rpc";

type Logs = Array<{ block: bigint }>;

function makePool(entries: RpcPoolEntry<string>[]) {
  return new RpcPool<string>(entries);
}

describe("RpcPool.callLogs", () => {
  it("caps.topicLogs=false 的端点跳过 getLogs（call 不受影响）", async () => {
    const seen: string[] = [];
    const pool = makePool([
      { url: URL_A, client: "A", caps: { topicLogs: false } },
      { url: URL_B, client: "B" },
    ]);
    const logs = await pool.callLogs<Logs>({ fromBlock: 0n, toBlock: 99n }, async (c, r) => {
      seen.push(`${c}:${r.fromBlock}-${r.toBlock}`);
      return [{ block: r.toBlock }];
    });
    expect(logs).toEqual([{ block: 99n }]);
    expect(seen).toEqual(["B:0-99"]);
    await pool.call(async (c) => c); // call 不受 caps 限制
    expect(pool.snapshot()[0].failures).toBe(0);
  });

  it("按错误文案提取上限一次切到位并记忆；之后预分块不再试错", async () => {
    let calls = 0;
    const pool = makePool([{ url: URL_A, client: "A" }]);
    const fn = async (_c: string, r: { fromBlock: bigint; toBlock: bigint }) => {
      calls++;
      if (r.toBlock - r.fromBlock + 1n > 3000n) throw new Error("Query returned more than 3000 blocks range");
      return [{ block: r.toBlock }, { block: r.fromBlock }];
    };
    const logs = await pool.callLogs<Logs>({ fromBlock: 0n, toBlock: 9999n }, fn);
    expect(logs).toHaveLength(8); // 4 片 × 2 条
    expect(pool.snapshot()[0].maxLogRange).toBe(3000n);

    const before = calls;
    await pool.callLogs<Logs>({ fromBlock: 0n, toBlock: 9999n }, fn);
    expect(calls - before).toBe(4); // 直接按已学范围预分块，无失败重试
  });

  it("读不出上限对半二分，拆到 MIN_CHUNK 仍失败滑下家（下家收到整段）", async () => {
    const spans: bigint[] = [];
    let bArgs: Array<[bigint, bigint]> = [];
    const pool = makePool([
      { url: URL_A, client: "A" },
      { url: URL_B, client: "B" },
    ]);
    const logs = await pool.callLogs<Logs>({ fromBlock: 0n, toBlock: 9999n }, async (c, r) => {
      if (c === "A") {
        spans.push(r.toBlock - r.fromBlock + 1n);
        throw new Error("block range too wide"); // 无数字上限，任何范围都超限
      }
      bArgs.push([r.fromBlock, r.toBlock]);
      return [{ block: r.toBlock }];
    });
    expect(logs).toEqual([{ block: 9999n }]);
    expect(spans.map(String)).toEqual(["10000", "5000", "2500", "2000"]);
    expect(bArgs).toEqual([[0n, 9999n]]);
  });

  it("quota 不触发拆分，冷却后滑下家", async () => {
    const pool = makePool([
      { url: URL_A, client: "A" },
      { url: URL_B, client: "B" },
    ]);
    const fn = async (c: string) => {
      if (c === "A") throw new Error("HTTP 429"); // 文案经 classifyRpcError → quota
      return [{ block: 1n }];
    };
    const logs = await pool.callLogs<Logs>({ fromBlock: 0n, toBlock: 9999n }, fn);
    expect(logs).toEqual([{ block: 1n }]);
    expect(pool.snapshot()[0].cooldownSec).toBeGreaterThan(0);
  });

  it("from > to 直接返回空且不发调用", async () => {
    const pool = makePool([{ url: URL_A, client: "A" }]);
    let calls = 0;
    const logs = await pool.callLogs<Logs>({ fromBlock: 10n, toBlock: 9n }, async () => { calls++; return []; });
    expect(logs).toEqual([]);
    expect(calls).toBe(0);
  });
});

describe("v0.2.0 数值 caps 与分桶学习", () => {
  const URL_A = "https://a.example/rpc";
  const URL_B = "https://b.example/rpc";

  it("caps 数值初始值按 lane 独立预分块，学习只收紧各自通道", async () => {
    const pool = new RpcPool<string>([
      { url: URL_A, client: "A", caps: { topicLogRange: 3000, addressLogRange: 8000 } },
    ]);
    const topicSpans: bigint[] = [];
    const addrSpans: bigint[] = [];
    await pool.callLogs<unknown[]>({ fromBlock: 0n, toBlock: 9999n },
      async (_c, r) => { topicSpans.push(r.toBlock - r.fromBlock + 1n); return []; },
      { lane: "topic" });
    await pool.callLogs<unknown[]>({ fromBlock: 0n, toBlock: 9999n },
      async (_c, r) => { addrSpans.push(r.toBlock - r.fromBlock + 1n); return []; },
      { lane: "address" });
    expect(topicSpans[0]).toBe(3000n);
    expect(addrSpans[0]).toBe(8000n);
    // topic 通道学习 2000 不影响 address 通道的 8000。
    // 该次调用最终失败：拆到 MIN_CHUNK(2000) 仍 >= 2000n 超限、无下家可滑——但学习值已在拆分前落桶。
    await expect(
      pool.callLogs<unknown[]>({ fromBlock: 0n, toBlock: 9999n },
        async (_c, r) => { topicSpans.push(r.toBlock - r.fromBlock + 1n);
          if (r.toBlock - r.fromBlock + 1n >= 2000n) throw new Error("block range too wide");
          return []; },
        { lane: "topic" }),
    ).rejects.toThrow("block range too wide");
    expect(pool.snapshot()[0].maxTopicRange).toBe(2000n);
    expect(pool.snapshot()[0].maxAddressRange).toBe(8000n);
  });

  it("topicLogRange<=0 视同不支持：topic 查询跳过该端点，address 查询可用", async () => {
    const seen: string[] = [];
    const pool = new RpcPool<string>([
      { url: URL_A, client: "A", caps: { topicLogRange: 0, addressLogRange: 1000 } },
      { url: URL_B, client: "B" },
    ]);
    await pool.callLogs<unknown[]>({ fromBlock: 0n, toBlock: 99n },
      async (c) => { seen.push(c); return []; }, { lane: "topic" });
    expect(seen).toEqual(["B"]);
    await pool.callLogs<unknown[]>({ fromBlock: 0n, toBlock: 99n },
      async (c) => { seen.push(c); return []; }, { lane: "address" });
    expect(seen).toEqual(["B", "A"]); // address lane 下 A 仍是首选
  });

  it("缺省 lane = topic；maxLogRange 别名 = maxTopicRange", async () => {
    const pool = new RpcPool<string>([{ url: URL_A, client: "A", caps: { topicLogRange: 2500 } }]);
    await pool.callLogs<unknown[]>({ fromBlock: 0n, toBlock: 9999n },
      async (_c, r) => (r.toBlock - r.fromBlock + 1n > 2500n ? Promise.reject(new Error("Query returned more than 2500 blocks range")) : []));
    const s = pool.snapshot()[0];
    expect(s.maxLogRange).toBe(2500n);
    expect(s.maxTopicRange).toBe(2500n);
    expect(s.maxAddressRange).toBeNull();
  });
});
