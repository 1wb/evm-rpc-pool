import { RpcPool, type EntryCaps, type RpcPoolOptions } from "./pool.js";
import {
  ARCHIVE_TEXT,
  QUOTA_TEXT,
  RpcKindError,
  isContractRevertMessage,
  isRangeLimitMessage,
  rangeLimitFromMessage,
} from "./classify.js";
import type { BlockRange } from "./range.js";
import { hostOf } from "./url.js";

export interface RpcResponse {
  ok: boolean;
  status: number;
  json(): Promise<unknown>;
}

export type FetchLike = (
  url: string,
  init: { method: string; headers: Record<string, string>; body: string; signal?: AbortSignal },
) => Promise<RpcResponse>;

export interface FetchRpcPoolOptions extends RpcPoolOptions {
  fetchImpl?: FetchLike;
  timeoutMs?: number;
  /** host → 能力表（如 { "base-rpc.publicnode.com": { topicLogs: false } }） */
  hostCaps?: Record<string, EntryCaps>;
}

const QUOTA_STATUS = new Set([429, 402, 403]);
const DEFAULT_TIMEOUT_MS = 20_000;

function timeoutSignal(ms: number): AbortSignal | undefined {
  const t = (AbortSignal as { timeout?: (ms: number) => AbortSignal }).timeout;
  return t ? t.call(AbortSignal, ms) : undefined;
}

interface FetchClient {
  url: string;
  fetch: FetchLike;
  timeoutMs: number;
}

/** 把 fetch / HTTP 状态 / JSON-RPC error 三层结果映射为预分型错误；rejected 只可能出自这里。 */
async function attemptJsonRpc(c: FetchClient, method: string, params: unknown): Promise<unknown> {
  let res: RpcResponse;
  try {
    res = await c.fetch(c.url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
      signal: timeoutSignal(c.timeoutMs),
    });
  } catch (err) {
    throw new RpcKindError("transient", String(err).slice(0, 120));
  }
  if (QUOTA_STATUS.has(res.status)) throw new RpcKindError("quota", `HTTP ${res.status}`);
  if (!res.ok) throw new RpcKindError("transient", `HTTP ${res.status}`);
  let j: { result?: unknown; error?: unknown };
  try {
    j = (await res.json()) as typeof j;
  } catch {
    throw new RpcKindError("transient", "响应非 JSON");
  }
  if (j && j.error !== undefined && j.error !== null) {
    const msg =
      typeof j.error === "object" && j.error !== null && "message" in j.error
        ? String((j.error as { message?: unknown }).message ?? j.error)
        : String(j.error);
    const short = msg.slice(0, 120);
    if (QUOTA_TEXT.test(msg)) throw new RpcKindError("quota", short);
    if (ARCHIVE_TEXT.test(msg)) throw new RpcKindError("archive", short);
    if (isRangeLimitMessage(msg)) throw new RpcKindError("range", short, rangeLimitFromMessage(msg));
    if (isContractRevertMessage(msg)) throw new RpcKindError("reverted", short);
    throw new RpcKindError("rejected", short);
  }
  if (j && j.result !== undefined && j.result !== null) return j.result;
  throw new RpcKindError("transient", "null result");
}

export class FetchRpcPool {
  private readonly pool: RpcPool<FetchClient>;

  constructor(urls: string[], opts: FetchRpcPoolOptions = {}) {
    if (!urls.length) throw new Error("FetchRpcPool: 端点列表为空");
    const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    // fetch 须绑定 globalThis：CF 新运行时对裸引用的 host 函数报 Illegal invocation
    const fallback: FetchLike = fetch.bind(globalThis);
    const entries = urls.map((url) => ({
      url,
      client: { url, fetch: opts.fetchImpl ?? fallback, timeoutMs } satisfies FetchClient,
      caps: opts.hostCaps?.[hostOf(url)],
    }));
    this.pool = new RpcPool<FetchClient>(entries, opts);
  }

  call<T>(method: string, params: unknown): Promise<T> {
    return this.pool.call((c) => attemptJsonRpc(c, method, params) as Promise<T>);
  }

  /** fromBlock/toBlock 接受 number 或 bigint（number 侧为历史消费方便利），内部统一 bigint。 */
  getLogs<T>(opts: {
    address: string;
    topics: string[];
    fromBlock: number | bigint;
    toBlock: number | bigint;
  }): Promise<T[]> {
    const range: BlockRange = { fromBlock: BigInt(opts.fromBlock), toBlock: BigInt(opts.toBlock) };
    // 按 topics 形状自动派生 lane：存在非 null topic 走 topic 通道（null 占位不算），否则 address 通道
    const lane: "topic" | "address" = opts.topics.some((t) => t != null) ? "topic" : "address";
    return this.pool.callLogs<T>(range, async (c, r) => {
      const result = (await attemptJsonRpc(c, "eth_getLogs", [
        {
          address: opts.address,
          topics: opts.topics,
          fromBlock: "0x" + r.fromBlock.toString(16),
          toBlock: "0x" + r.toBlock.toString(16),
        },
      ])) as T[];
      return Array.isArray(result) ? result : [];
    }, { lane });
  }

  snapshot() {
    return this.pool.snapshot();
  }
}
