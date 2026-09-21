import { EndpointBreaker } from "./endpoint-breaker.js";
import { classifyRpcError, RpcKindError } from "./classify.js";
import { rangeLimitFromMessage } from "./classify.js";
import { MIN_CHUNK, splitBlockRange } from "./range.js";
import { redactError, redactUrl } from "./redact.js";
import { hostOf } from "./url.js";
/** 全部端点均在冷却（含探针在途）时抛出，带最早可重试时间与脱敏标签。 */
export class PoolCoolingError extends Error {
    retryAfterMs;
    constructor(retryAfterMs, labels) {
        super(`全部 RPC 端点均在冷却，最早 ${Math.ceil(retryAfterMs / 1000)} 秒后可重试：${labels.join("、")}`);
        this.name = "PoolCoolingError";
        this.retryAfterMs = retryAfterMs;
    }
}
/** 端点健康但本次调用失败（rejected/reverted/range 拆到下限），滑下家不冷却。 */
class SlideSignal extends Error {
}
function kindOf(error) {
    if (error instanceof RpcKindError)
        return error.kind;
    return classifyRpcError(error);
}
export class RpcPool {
    entries;
    now;
    constructor(entries, opts = {}) {
        if (!entries.length)
            throw new Error("RpcPool: 端点列表为空");
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
    async call(fn) {
        const reasons = [];
        let attempts = 0;
        for (const e of this.entries) {
            if (!e.breaker.begin())
                continue;
            attempts += 1;
            try {
                const result = await fn(e.client, e.url);
                if (result === null || result === undefined) {
                    reasons.push(this.fail(e, new Error("返回 null")));
                    continue;
                }
                e.breaker.report("ok");
                return result;
            }
            catch (error) {
                reasons.push(this.fail(e, error));
            }
        }
        if (attempts === 0)
            throw this.coolingError();
        throw new Error(`RPC 全部 ${attempts} 端点失败: ${reasons.join(" | ").slice(0, 300)}`);
    }
    snapshot() {
        const now = this.now();
        return this.entries.map((e) => {
            const s = e.breaker.snapshot(now);
            return { host: hostOf(e.url), failures: s.failures, cooldownSec: s.cooldownSec, maxLogRange: e.maxLogRange };
        });
    }
    /**
     * getLogs：与 call 同候选序，但先按 caps 过滤（全部无 caps 则用全体），
     * 再按端点已学范围预分块；范围超限在端点内自适应拆分（提取上限一次切到位/对半二分），
     * 拆到 MIN_CHUNK 仍失败才滑下家。块号 bigint，from > to 返回 []。
     */
    async callLogs(range, fn) {
        if (range.fromBlock > range.toBlock)
            return [];
        const capable = this.entries.filter((e) => e.caps?.topicLogs !== false);
        const candidates = capable.length ? capable : this.entries;
        const reasons = [];
        let attempts = 0;
        for (const e of candidates) {
            if (!e.breaker.begin())
                continue;
            attempts += 1;
            try {
                const span = range.toBlock - range.fromBlock + 1n;
                const initial = e.maxLogRange !== null && e.maxLogRange < span
                    ? splitBlockRange(range.fromBlock, range.toBlock, e.maxLogRange).map((p) => ({ fromBlock: p.from, toBlock: p.to }))
                    : [range];
                const out = [];
                for (const part of initial) {
                    out.push(...(await this.logsRange(e, fn, part)));
                }
                e.breaker.report("ok");
                return out;
            }
            catch (error) {
                reasons.push(this.fail(e, error));
            }
        }
        if (attempts === 0)
            throw this.coolingError();
        throw new Error(`RPC getLogs 全部 ${attempts} 端点失败: ${reasons.join(" | ").slice(0, 300)}`);
    }
    /** 端点内取一段范围；range 超限则自适应拆分递归，rejected/reverted/range到底 转滑动信号，其余按分型上抛。 */
    async logsRange(e, fn, range) {
        let result;
        try {
            result = await fn(e.client, range);
        }
        catch (error) {
            const span = range.toBlock - range.fromBlock + 1n;
            const kind = kindOf(error);
            if (kind === "range" && span > MIN_CHUNK) {
                const parsed = error instanceof RpcKindError ? error.rangeLimit : rangeLimitFromMessage(error instanceof Error ? error.message : String(error));
                // 能读出上限一次切到位，读不出对半二分；下限 MIN_CHUNK 兜底，学习值取历史最小
                const chunk = parsed !== null && parsed < span ? parsed : (span + 1n) / 2n;
                const eff = chunk > MIN_CHUNK ? chunk : MIN_CHUNK;
                e.maxLogRange = e.maxLogRange === null ? eff : (eff < e.maxLogRange ? eff : e.maxLogRange);
                const out = [];
                for (const part of splitBlockRange(range.fromBlock, range.toBlock, eff)) {
                    out.push(...(await this.logsRange(e, fn, { fromBlock: part.from, toBlock: part.to })));
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
    fail(e, error) {
        const kind = kindOf(error);
        const detail = error instanceof SlideSignal ? error.message : redactError(error, [e.url]).slice(0, 120);
        if (kind === "rejected" || kind === "reverted" || kind === "range" || error instanceof SlideSignal) {
            e.breaker.slide();
        }
        else {
            e.breaker.report(kind);
        }
        return `${hostOf(e.url)}: ${detail}`;
    }
    coolingError() {
        const now = this.now();
        const earliest = Math.min(...this.entries.map((e) => e.breaker.cooldownUntil));
        const labels = this.entries
            .filter((e) => e.breaker.cooldownUntil === earliest)
            .map((e) => redactUrl(e.url));
        return new PoolCoolingError(Math.max(0, earliest - now), labels);
    }
}
