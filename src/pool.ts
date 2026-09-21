import { EndpointBreaker, type BreakerTuning, type CoolingKind } from "./endpoint-breaker.js";
import { classifyRpcError, RpcKindError, type RpcErrorKind } from "./classify.js";
import { redactError, redactUrl } from "./redact.js";
import { hostOf } from "./url.js";

export interface EntryCaps {
  /** 是否支持按 topic 过滤的 getLogs（false = getLogs 候选中跳过该端点） */
  topicLogs?: boolean;
}

export interface RpcPoolEntry<C> {
  url: string;
  client: C;
  caps?: EntryCaps;
}

export interface RpcPoolOptions {
  now?: () => number;
  quota?: BreakerTuning;
  archive?: BreakerTuning;
  transient?: BreakerTuning;
}

/** 全部端点均在冷却（含探针在途）时抛出，带最早可重试时间与脱敏标签。 */
export class PoolCoolingError extends Error {
  readonly retryAfterMs: number;
  constructor(retryAfterMs: number, labels: readonly string[]) {
    super(`全部 RPC 端点均在冷却，最早 ${Math.ceil(retryAfterMs / 1000)} 秒后可重试：${labels.join("、")}`);
    this.name = "PoolCoolingError";
    this.retryAfterMs = retryAfterMs;
  }
}

/** 端点健康但本次调用失败（rejected/reverted/range 拆到下限），滑下家不冷却。 */
class SlideSignal extends Error {}

function kindOf(error: unknown): RpcErrorKind {
  if (error instanceof RpcKindError) return error.kind;
  return classifyRpcError(error);
}

type EntryState<C> = RpcPoolEntry<C> & { breaker: EndpointBreaker; maxLogRange: bigint | null };

export class RpcPool<C> {
  private readonly entries: Array<EntryState<C>>;
  private readonly now: () => number;

  constructor(entries: readonly RpcPoolEntry<C>[], opts: RpcPoolOptions = {}) {
    if (!entries.length) throw new Error("RpcPool: 端点列表为空");
    this.now = opts.now ?? Date.now;
    this.entries = entries.map((e) => ({
      ...e,
      breaker: new EndpointBreaker({
        now: opts.now,
        quota: opts.quota,
        archive: opts.archive,
        transient: opts.transient,
      }),
      maxLogRange: null,
    }));
  }

  /**
   * 粘性优先通用调用：恒按构造序过滤冷却端点，从首个开始试；
   * rejected/reverted 滑下家不冷却，quota/archive/transient 冷却后滑下家，ok 清零。
   * 回调返回 null/undefined 视同 transient（对合法返回 null 的方法请自行包 sentinel）。
   */
  async call<T>(fn: (client: C, url: string) => Promise<T>): Promise<T> {
    const reasons: string[] = [];
    let attempts = 0;
    for (const e of this.entries) {
      if (!e.breaker.begin()) continue;
      attempts += 1;
      try {
        const result = await fn(e.client, e.url);
        if (result === null || result === undefined) {
          reasons.push(this.fail(e, new Error("返回 null")));
          continue;
        }
        e.breaker.report("ok");
        return result;
      } catch (error) {
        reasons.push(this.fail(e, error));
      }
    }
    if (attempts === 0) throw this.coolingError();
    throw new Error(`RPC 全部 ${attempts} 端点失败: ${reasons.join(" | ").slice(0, 300)}`);
  }

  snapshot(): Array<{ host: string; failures: number; cooldownSec: number; maxLogRange: bigint | null }> {
    const now = this.now();
    return this.entries.map((e) => {
      const s = e.breaker.snapshot(now);
      return { host: hostOf(e.url), failures: s.failures, cooldownSec: s.cooldownSec, maxLogRange: e.maxLogRange };
    });
  }

  /** 按分型处置单端点失败，返回脱敏原因供聚合报错。 */
  private fail(e: EntryState<C>, error: unknown): string {
    const kind = kindOf(error);
    const detail = error instanceof SlideSignal ? error.message : redactError(error, [e.url]).slice(0, 120);
    if (kind === "rejected" || kind === "reverted" || kind === "range" || error instanceof SlideSignal) {
      e.breaker.slide();
    } else {
      e.breaker.report(kind as CoolingKind);
    }
    return `${hostOf(e.url)}: ${detail}`;
  }

  private coolingError(): PoolCoolingError {
    const now = this.now();
    const earliest = Math.min(...this.entries.map((e) => e.breaker.cooldownUntil));
    const labels = this.entries
      .filter((e) => e.breaker.cooldownUntil === earliest)
      .map((e) => redactUrl(e.url));
    return new PoolCoolingError(Math.max(0, earliest - now), labels);
  }
}
