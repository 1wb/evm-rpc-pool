import { hostOf } from "./url.js";

/** 本地令牌桶限速（非供应商故障）：等待至多 maxWaitMs，超时抛出。 */
export class LocalThrottleError extends Error {
  readonly retryAfterMs: number;
  constructor(retryAfterMs: number) {
    super(`本地限速：下一令牌约 ${Math.ceil(retryAfterMs / 1000)} 秒后可用`);
    this.name = "LocalThrottleError";
    this.retryAfterMs = Math.max(0, retryAfterMs);
  }
}

/**
 * 沿 cause 链识别本地节流错误。viem 会把 fetchFn 抛出的错误包装成
 * HttpRequestError（instanceof 失效），必须走 cause 链；name 兜底双副本场景。
 */
export function isLocalThrottle(error: unknown): boolean {
  let cur: unknown = error;
  for (let depth = 0; depth < 10; depth++) {
    if (cur instanceof LocalThrottleError) return true;
    if (cur instanceof Error) {
      if (cur.name === "LocalThrottleError") return true;
      cur = cur.cause;
      continue;
    }
    return false;
  }
  return false;
}

/** 沿 cause 链提取 LocalThrottleError.retryAfterMs；非本地节流返回 null。 */
export function localThrottleRetryAfterMs(error: unknown): number | null {
  let cur: unknown = error;
  for (let depth = 0; depth < 10; depth++) {
    if (cur instanceof LocalThrottleError) return cur.retryAfterMs;
    if (cur instanceof Error) {
      if (cur.name === "LocalThrottleError") {
        const v = (cur as { retryAfterMs?: unknown }).retryAfterMs;
        return typeof v === "number" && Number.isFinite(v) && v > 0 ? v : 0;
      }
      cur = cur.cause;
      continue;
    }
    return null;
  }
  return null;
}

interface BudgetBucket {
  budget: string;
  /** 组速率（req/min）；0 = 不限 */
  reqPerMin: number;
  capacity: number;
  tokens: number;
  lastMs: number;
  /** Retry-After 边带（绝对截止时间）；期间视为无令牌，过期自然失效 */
  penaltyUntil: number;
}

export interface LimitEndpointEntry {
  url: string;
  /** 共享限额组键（同凭据跨链跨协议共用一桶）；缺省 = url 自身独享 */
  budget?: string;
  /** 组速率 req/min；缺省 90，0 = 不限。组速率由调用方解析（组内取显式最小值） */
  reqPerMin?: number;
}

export interface KeyRateLimiterOptions {
  /** acquireAndWait 未显式传 maxWaitMs 时的缺省等待上限 */
  acquireWaitMs?: number;
  /** 桶容量，缺省 5 */
  capacity?: number;
  now?: () => number;
  /** 等待实现；注入假时钟测试时须同步推进 now */
  sleep?: (ms: number) => Promise<void>;
  /** Retry-After 边带回调（pool 接线：同步对应端点 breaker） */
  onPenalty?: (url: string, untilMs: number) => void;
}

/**
 * endpointId/budgetKey 双层令牌桶：url → bucket（budget ?? url）。
 * 未登记 URL 一律放行（漏登记不致不可用）；权威扣费点在 fetchFn 的 acquireAndWait。
 */
export class KeyRateLimiter {
  private readonly buckets = new Map<string, BudgetBucket>();
  private readonly urlRegistry = new Map<string, { alias: string; budget: string }>();
  private readonly hostSeq = new Map<string, number>();
  private readonly listeners: Array<(url: string, untilMs: number) => void> = [];
  private readonly acquireWaitMs: number;
  private readonly capacity: number;
  private readonly now: () => number;
  private readonly sleep: (ms: number) => Promise<void>;

  constructor(opts: KeyRateLimiterOptions = {}) {
    this.acquireWaitMs = opts.acquireWaitMs ?? 15_000;
    this.capacity = opts.capacity ?? 5;
    this.now = opts.now ?? Date.now;
    this.sleep = opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
    if (opts.onPenalty) this.listeners.push(opts.onPenalty);
  }

  /** 启动预登记（稳定 host#kN 别名，非异步首见）；同 URL 重复登记保持首见。 */
  registerEndpoints(entries: readonly LimitEndpointEntry[]): void {
    for (const e of entries) {
      if (this.urlRegistry.has(e.url)) continue;
      const host = hostOf(e.url);
      const seq = (this.hostSeq.get(host) ?? 0) + 1;
      this.hostSeq.set(host, seq);
      const budget = e.budget ?? e.url;
      this.urlRegistry.set(e.url, { alias: `${host}#k${seq}`, budget });
      const existing = this.buckets.get(budget);
      if (!existing) {
        this.buckets.set(budget, {
          budget,
          reqPerMin: e.reqPerMin ?? 90,
          capacity: this.capacity,
          tokens: this.capacity,
          lastMs: this.now(),
          penaltyUntil: 0,
        });
      } else if (e.reqPerMin !== undefined) {
        existing.reqPerMin = Math.min(existing.reqPerMin, e.reqPerMin);
      }
    }
  }

  /** 查询不消费：penalty 未过或无令牌即 false。 */
  hasToken(url: string): boolean {
    const bucket = this.bucketOf(url);
    if (!bucket || bucket.reqPerMin === 0) return true;
    const t = this.now();
    this.refill(bucket, t);
    return t >= bucket.penaltyUntil && bucket.tokens >= 1;
  }

  /** 无令牌等待至多 maxWaitMs（等待型获取：分片序列慢下来续跑，而非弃前缀）；超时抛 LocalThrottleError。 */
  async acquireAndWait(url: string, maxWaitMs?: number): Promise<void> {
    const bucket = this.bucketOf(url);
    if (!bucket || bucket.reqPerMin === 0) return;
    const deadline = this.now() + (maxWaitMs ?? this.acquireWaitMs);
    for (;;) {
      const t = this.now();
      this.refill(bucket, t);
      if (t >= bucket.penaltyUntil && bucket.tokens >= 1) {
        bucket.tokens -= 1;
        return;
      }
      const availAt = Math.max(bucket.penaltyUntil, this.nextTokenAt(bucket, t));
      if (availAt > deadline) {
        throw new LocalThrottleError(availAt - this.now());
      }
      await this.sleep(Math.min(availAt - t, 25)); // tick 粒度 25ms；注入 sleep 须推进 now
    }
  }

  /** Retry-After 边带：写入该 budget 桶（取更晚）并通知监听者（pool → breaker）。 */
  setPenalty(url: string, untilMs: number): void {
    const bucket = this.bucketOf(url);
    if (bucket) bucket.penaltyUntil = Math.max(bucket.penaltyUntil, untilMs);
    for (const fn of this.listeners) fn(url, untilMs);
  }

  addPenaltyListener(fn: (url: string, untilMs: number) => void): void {
    this.listeners.push(fn);
  }

  /** 该 url 下一令牌可用的绝对时刻（含 penalty）；未登记/不限 = 0（随时可用）。 */
  earliestTokenRecovery(url: string): number {
    const bucket = this.bucketOf(url);
    if (!bucket || bucket.reqPerMin === 0) return 0;
    const t = this.now();
    this.refill(bucket, t);
    return Math.max(this.nextTokenAt(bucket, t), bucket.penaltyUntil);
  }

  /** 脱敏别名 host#kN（按 host 计数，启动顺序稳定）。 */
  urlAlias(url: string): string {
    return this.urlRegistry.get(url)?.alias ?? hostOf(url);
  }

  private bucketOf(url: string): BudgetBucket | undefined {
    const reg = this.urlRegistry.get(url);
    return reg ? this.buckets.get(reg.budget) : undefined;
  }

  private refill(bucket: BudgetBucket, t: number): void {
    if (t <= bucket.lastMs) return;
    bucket.tokens = Math.min(bucket.capacity, bucket.tokens + ((t - bucket.lastMs) * bucket.reqPerMin) / 60_000);
    bucket.lastMs = t;
  }

  private nextTokenAt(bucket: BudgetBucket, t: number): number {
    if (bucket.tokens >= 1) return t;
    return t + Math.ceil(((1 - bucket.tokens) * 60_000) / bucket.reqPerMin);
  }
}
