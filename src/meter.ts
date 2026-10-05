import { parseRetryAfter } from "./classify.js";
import { isLocalThrottle, KeyRateLimiter } from "./limiter.js";

/** 单 endpointId×方法 的计数格。localThrottled 未发请求，不计入 requests。 */
export interface MethodUsage {
  requests: number;
  ok: number;
  r429: number;
  r402: number;
  rpcError: number;
  otherErrors: number;
  localThrottled: number;
}

export interface EndpointUsage {
  /** WSS 推送（每条新头/日志通知）计数 */
  push: number;
  methods: Record<string, MethodUsage>;
}

/** endpointId（具体 URL）→ 用量；快照为自上次 confirmDelta() 的窗口增量。 */
export type MeterSnapshot = Record<string, EndpointUsage>;

type BodyVerdict = "ok" | "rpcError" | "malformed";

const ZERO: MethodUsage = { requests: 0, ok: 0, r429: 0, r402: 0, rpcError: 0, otherErrors: 0, localThrottled: 0 };

function cellKey(url: string, method: string): string {
  return `${url}\u0000${method}`;
}

function usageDelta(cur: MethodUsage, base: MethodUsage): MethodUsage {
  return {
    requests: cur.requests - base.requests,
    ok: cur.ok - base.ok,
    r429: cur.r429 - base.r429,
    r402: cur.r402 - base.r402,
    rpcError: cur.rpcError - base.rpcError,
    otherErrors: cur.otherErrors - base.otherErrors,
    localThrottled: cur.localThrottled - base.localThrottled,
  };
}

function usageIsZero(u: MethodUsage): boolean {
  return u.requests === 0 && u.ok === 0 && u.r429 === 0 && u.r402 === 0 && u.rpcError === 0 && u.otherErrors === 0 && u.localThrottled === 0;
}

/**
 * 按供应商、key（匿名编号）、RPC 方法计量实际消耗与失败原因。
 * 内部累计永不清零；snapshotDelta 给出窗口增量，confirmDelta 推进水位（发送成功才确认）。
 */
export class PoolMeter {
  private readonly cells = new Map<string, MethodUsage>();
  private committed = new Map<string, MethodUsage>();
  private readonly pushes = new Map<string, number>();
  private pushesCommitted = new Map<string, number>();

  /** 启动预登记（仅为别处对账枚举端点留底；记录本身不要求先登记）。 */
  registerEndpoints(urls: readonly string[]): void {
    for (const u of urls) if (!this.pushes.has(u)) this.pushes.set(u, 0);
  }

  /** 直接计量一次调用/请求（probe 等自控全流程的调用方用）。 */
  record(url: string, r: { method: string; status?: number; rpcError?: boolean; localThrottled?: boolean }): void {
    const cell = this.bump(url, r.method);
    if (r.localThrottled) {
      cell.localThrottled += 1;
      return;
    }
    cell.requests += 1;
    if (r.status === 429) cell.r429 += 1;
    else if (r.status === 402 || r.status === 403) cell.r402 += 1;
    else if (r.rpcError) cell.rpcError += 1;
    else if (r.status !== undefined && r.status >= 200 && r.status < 300) cell.ok += 1;
    else cell.otherErrors += 1;
  }

  /** meteredFetch 用：HTTP 结果落格；2xx 的 ok/rpcError 由 body 异步判定后经 noteBodyVerdict 补记。 */
  noteHttpResult(url: string, method: string, status: number): void {
    if (status >= 200 && status < 300) {
      this.bump(url, method).requests += 1;
      return;
    }
    this.record(url, { method, status });
  }

  noteBodyVerdict(url: string, method: string, verdict: BodyVerdict): void {
    const cell = this.bump(url, method);
    if (verdict === "ok") cell.ok += 1;
    else if (verdict === "rpcError") cell.rpcError += 1;
    else cell.otherErrors += 1;
  }

  /** WSS 推送计量：每条新头/日志通知 +1。 */
  recordPush(url: string): void {
    this.pushes.set(url, (this.pushes.get(url) ?? 0) + 1);
  }

  /** 自上次 confirmDelta() 的窗口增量；无事件的端点不出现在结果里。 */
  snapshotDelta(): MeterSnapshot {
    const out: MeterSnapshot = {};
    const take = (url: string): EndpointUsage => {
      if (!out[url]) out[url] = { push: 0, methods: {} };
      return out[url];
    };
    for (const [key, cur] of this.cells) {
      const idx = key.indexOf("\u0000");
      const url = key.slice(0, idx);
      const method = key.slice(idx + 1);
      const base = this.committed.get(key) ?? ZERO;
      const d = usageDelta(cur, base);
      if (!usageIsZero(d)) take(url).methods[method] = d;
    }
    for (const [url, n] of this.pushes) {
      const d = n - (this.pushesCommitted.get(url) ?? 0);
      if (d > 0) take(url).push = d;
    }
    return out;
  }

  /** 水位确认：当前累计固化为基线（调用时机 = 摘要推送成功后）。 */
  confirmDelta(): void {
    this.committed = new Map([...this.cells].map(([k, v]) => [k, { ...v }]));
    this.pushesCommitted = new Map(this.pushes);
  }

  private bump(url: string, method: string): MethodUsage {
    const key = cellKey(url, method);
    let cell = this.cells.get(key);
    if (!cell) {
      cell = { ...ZERO };
      this.cells.set(key, cell);
    }
    return cell;
  }
}

/** 从 fetch init body 提取 RPC 方法名（解析失败记 unknown）。 */
function parseBodyMethod(body: unknown): string {
  if (typeof body !== "string" || !body) return "unknown";
  try {
    const j = JSON.parse(body) as { method?: unknown };
    return typeof j.method === "string" ? j.method : "unknown";
  } catch {
    return "unknown";
  }
}

// 库 tsconfig 不含 DOM lib：经 typeof fetch 派生入参类型，避免引用 RequestInfo 等全局名
type FetchInput = Parameters<typeof fetch>[0];
type FetchInit = Parameters<typeof fetch>[1];

function requestUrl(input: FetchInput): string {
  if (typeof input === "string") return input;
  if (input instanceof URL) return input.href;
  return (input as { url: string }).url;
}

/**
 * viem `http(url, { fetchFn })` 注入用：实际 HTTP 发送前的权威扣费点（等待型获取），
 * 每次真实请求计量（覆盖 callLogs 拆片与 viem 内部重试——后者本项目以 retryCount:0 关闭），
 * 429 读 Retry-After 调 limiter.setPenalty（边带同步 breaker——JSON-RPC error body 丢响应头路径的对策）。
 */
export function meteredFetch(meter: PoolMeter, limiter: KeyRateLimiter, base: typeof fetch = fetch.bind(globalThis)): typeof fetch {
  return async (input: FetchInput, init?: FetchInit): Promise<Response> => {
    const url = requestUrl(input);
    const method = parseBodyMethod(init?.body);
    try {
      await limiter.acquireAndWait(url);
    } catch (error) {
      if (isLocalThrottle(error)) meter.record(url, { method, localThrottled: true });
      throw error;
    }
    const res = await base(input, init);
    if (res.status === 429) {
      const ms = parseRetryAfter(res.headers?.get?.("retry-after") ?? null, Date.now());
      if (ms !== null) limiter.setPenalty(url, Date.now() + ms);
    }
    meter.noteHttpResult(url, method, res.status);
    if (res.status >= 200 && res.status < 300) {
      // clone 异步判定 JSON-RPC error body：不阻塞响应返回；失败按 malformed 计 otherErrors
      void res
        .clone()
        .json()
        .then((j: unknown) => {
          const err = j && typeof j === "object" ? (j as { error?: unknown }).error : undefined;
          meter.noteBodyVerdict(url, method, err !== null && err !== undefined ? "rpcError" : "ok");
        })
        .catch(() => meter.noteBodyVerdict(url, method, "malformed"));
    }
    return res;
  };
}
