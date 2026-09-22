import { EndpointBreaker, type BreakerTuning, type CoolingKind } from "./endpoint-breaker.js";
import { classifyRpcError, RpcKindError, type RpcErrorKind } from "./classify.js";
import { rangeLimitFromMessage } from "./classify.js";
import { MIN_CHUNK, splitBlockRange, type BlockRange } from "./range.js";
import { redactError, redactUrl } from "./redact.js";
import { hostOf } from "./url.js";

export interface EntryCaps {
  /** 是否支持按 topic 过滤的 getLogs（false = topic 候选中跳过该端点） */
  topicLogs?: boolean;
  /** topic 查询单次最大块跨度；<=0 视同不支持该 lane */
  topicLogRange?: number;
  /** address-only 查询单次最大块跨度；<=0 视同不支持该 lane */
  addressLogRange?: number;
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

/** getLogs 查询通道：topic = 带 topic 过滤；address = 仅按地址过滤。 */
export type LogLane = "topic" | "address";

/** caps 数值 → 初始学习值；缺省或 <=0 视同未声明（null = 无已学上限，不预分块）。 */
function capsInitial(n: number | undefined): bigint | null {
  return n != null && n > 0 ? BigInt(n) : null;
}

/** 数值 caps 的 lane 支持判定：缺省视为支持，<=0 视同不支持该 lane。 */
function laneSupported(n: number | undefined): boolean {
  return n === undefined || n > 0;
}

function kindOf(error: unknown): RpcErrorKind {
  if (error instanceof RpcKindError) return error.kind;
  return classifyRpcError(error);
}

type EntryState<C> = RpcPoolEntry<C> & {
  breaker: EndpointBreaker;
  maxTopicRange: bigint | null;
  maxAddressRange: bigint | null;
};

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
      maxTopicRange: capsInitial(e.caps?.topicLogRange),
      maxAddressRange: capsInitial(e.caps?.addressLogRange),
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

  snapshot(): Array<{
    host: string;
    failures: number;
    cooldownSec: number;
    maxTopicRange: bigint | null;
    maxAddressRange: bigint | null;
    /** 弃用别名 = maxTopicRange（topic 通道学习值），供 v0.1.x 消费方过渡 */
    maxLogRange: bigint | null;
  }> {
    const now = this.now();
    return this.entries.map((e) => {
      const s = e.breaker.snapshot(now);
      return {
        host: hostOf(e.url),
        failures: s.failures,
        cooldownSec: s.cooldownSec,
        maxTopicRange: e.maxTopicRange,
        maxAddressRange: e.maxAddressRange,
        maxLogRange: e.maxTopicRange,
      };
    });
  }

  /**
   * getLogs：与 call 同候选序，但先按 caps 与 lane 过滤（过滤后为空则用全体），
   * 再按端点在对应 lane 已学的范围预分块；范围超限在端点内自适应拆分（提取上限一次切到位/对半二分），
   * 拆到 MIN_CHUNK 仍失败才滑下家。块号 bigint，from > to 返回 []。
   * lane 缺省 "topic"；学习值按 lane 分桶（maxTopicRange / maxAddressRange），只收紧取历史最小。
   */
  async callLogs<T>(
    range: BlockRange,
    fn: (client: C, range: BlockRange) => Promise<readonly T[]>,
    opts: { lane?: LogLane } = {},
  ): Promise<T[]> {
    const lane: LogLane = opts.lane ?? "topic";
    if (range.fromBlock > range.toBlock) return [];
    const capable = this.entries.filter((e) => {
      if (lane === "topic") return e.caps?.topicLogs !== false && laneSupported(e.caps?.topicLogRange);
      return laneSupported(e.caps?.addressLogRange);
    });
    const candidates = capable.length ? capable : this.entries;
    const reasons: string[] = [];
    let attempts = 0;
    for (const e of candidates) {
      if (!e.breaker.begin()) continue;
      attempts += 1;
      try {
        const span = range.toBlock - range.fromBlock + 1n;
        const learned = lane === "topic" ? e.maxTopicRange : e.maxAddressRange;
        const initial =
          learned !== null && learned < span
            ? splitBlockRange(range.fromBlock, range.toBlock, learned).map((p) => ({ fromBlock: p.from, toBlock: p.to }))
            : [range];
        const out: T[] = [];
        for (const part of initial) {
          out.push(...(await this.logsRange(e, fn, part, lane)));
        }
        e.breaker.report("ok");
        return out;
      } catch (error) {
        reasons.push(this.fail(e, error));
      }
    }
    if (attempts === 0) throw this.coolingError();
    throw new Error(`RPC getLogs 全部 ${attempts} 端点失败: ${reasons.join(" | ").slice(0, 300)}`);
  }

  /** 端点内取一段范围；range 超限则自适应拆分递归，rejected/reverted/range到底 转滑动信号，其余按分型上抛。 */
  private async logsRange<T>(
    e: EntryState<C>,
    fn: (client: C, range: BlockRange) => Promise<readonly T[]>,
    range: BlockRange,
    lane: LogLane,
  ): Promise<T[]> {
    let result: readonly T[];
    try {
      result = await fn(e.client, range);
    } catch (error) {
      const span = range.toBlock - range.fromBlock + 1n;
      const kind = kindOf(error);
      if (kind === "range" && span > MIN_CHUNK) {
        const parsed =
          error instanceof RpcKindError ? error.rangeLimit : rangeLimitFromMessage(error instanceof Error ? error.message : String(error));
        // 能读出上限一次切到位，读不出对半二分；下限 MIN_CHUNK 兜底，学习值按 lane 分桶、取历史最小
        const chunk = parsed !== null && parsed < span ? parsed : (span + 1n) / 2n;
        const eff = chunk > MIN_CHUNK ? chunk : MIN_CHUNK;
        if (lane === "topic") {
          e.maxTopicRange = e.maxTopicRange === null ? eff : (eff < e.maxTopicRange ? eff : e.maxTopicRange);
        } else {
          e.maxAddressRange = e.maxAddressRange === null ? eff : (eff < e.maxAddressRange ? eff : e.maxAddressRange);
        }
        const out: T[] = [];
        for (const part of splitBlockRange(range.fromBlock, range.toBlock, eff)) {
          out.push(...(await this.logsRange(e, fn, { fromBlock: part.from, toBlock: part.to }, lane)));
        }
        return out;
      }
      if (kind === "range" || kind === "rejected" || kind === "reverted") {
        throw new SlideSignal(redactError(error, [e.url]).slice(0, 120));
      }
      throw error; // quota/transient/archive → callLogs 外层按分型冷却
    }
    return [...result];
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
