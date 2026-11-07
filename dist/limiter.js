import { hostOf } from "./url.js";
/** 本地令牌桶限速（非供应商故障）：等待至多 maxWaitMs，超时抛出。 */
export class LocalThrottleError extends Error {
    retryAfterMs;
    constructor(retryAfterMs) {
        super(`本地限速：下一令牌约 ${Math.ceil(retryAfterMs / 1000)} 秒后可用`);
        this.name = "LocalThrottleError";
        this.retryAfterMs = Math.max(0, retryAfterMs);
    }
}
/**
 * 沿 cause 链识别本地节流错误。viem 会把 fetchFn 抛出的错误包装成
 * HttpRequestError（instanceof 失效），必须走 cause 链；name 兜底双副本场景。
 */
export function isLocalThrottle(error) {
    let cur = error;
    for (let depth = 0; depth < 10; depth++) {
        if (cur instanceof LocalThrottleError)
            return true;
        if (cur instanceof Error) {
            if (cur.name === "LocalThrottleError")
                return true;
            cur = cur.cause;
            continue;
        }
        return false;
    }
    return false;
}
/** 沿 cause 链提取 LocalThrottleError.retryAfterMs；非本地节流返回 null。 */
export function localThrottleRetryAfterMs(error) {
    let cur = error;
    for (let depth = 0; depth < 10; depth++) {
        if (cur instanceof LocalThrottleError)
            return cur.retryAfterMs;
        if (cur instanceof Error) {
            if (cur.name === "LocalThrottleError") {
                const v = cur.retryAfterMs;
                return typeof v === "number" && Number.isFinite(v) && v > 0 ? v : 0;
            }
            cur = cur.cause;
            continue;
        }
        return null;
    }
    return null;
}
/**
 * endpointId/budgetKey 双层令牌桶：url → bucket（budget ?? url）。
 * 未登记 URL 一律放行（漏登记不致不可用）；权威扣费点在 fetchFn 的 acquireAndWait。
 */
export class KeyRateLimiter {
    buckets = new Map();
    urlRegistry = new Map();
    hostSeq = new Map();
    listeners = [];
    acquireWaitMs;
    capacity;
    now;
    sleep;
    constructor(opts = {}) {
        this.acquireWaitMs = opts.acquireWaitMs ?? 15_000;
        this.capacity = opts.capacity ?? 5;
        this.now = opts.now ?? Date.now;
        this.sleep = opts.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
        if (opts.onPenalty)
            this.listeners.push(opts.onPenalty);
    }
    /** 启动预登记（稳定 host#kN 别名，非异步首见）；同 URL 重复登记保持首见。 */
    registerEndpoints(entries) {
        for (const e of entries) {
            if (this.urlRegistry.has(e.url))
                continue;
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
            }
            else if (e.reqPerMin !== undefined) {
                existing.reqPerMin = Math.min(existing.reqPerMin, e.reqPerMin);
            }
        }
    }
    /** 查询不消费：penalty 未过或无令牌即 false。 */
    hasToken(url) {
        const bucket = this.bucketOf(url);
        if (!bucket || bucket.reqPerMin === 0)
            return true;
        const t = this.now();
        this.refill(bucket, t);
        return t >= bucket.penaltyUntil && bucket.tokens >= 1;
    }
    /** 无令牌等待至多 maxWaitMs（等待型获取：分片序列慢下来续跑，而非弃前缀）；超时抛 LocalThrottleError。 */
    async acquireAndWait(url, maxWaitMs) {
        const bucket = this.bucketOf(url);
        if (!bucket || bucket.reqPerMin === 0)
            return;
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
    setPenalty(url, untilMs) {
        const bucket = this.bucketOf(url);
        if (bucket)
            bucket.penaltyUntil = Math.max(bucket.penaltyUntil, untilMs);
        for (const fn of this.listeners)
            fn(url, untilMs);
    }
    addPenaltyListener(fn) {
        this.listeners.push(fn);
    }
    /** 该 url 下一令牌可用的绝对时刻（含 penalty）；未登记/不限 = 0（随时可用）。 */
    earliestTokenRecovery(url) {
        const bucket = this.bucketOf(url);
        if (!bucket || bucket.reqPerMin === 0)
            return 0;
        const t = this.now();
        this.refill(bucket, t);
        return Math.max(this.nextTokenAt(bucket, t), bucket.penaltyUntil);
    }
    /** 脱敏别名 host#kN（按 host 计数，启动顺序稳定）。 */
    urlAlias(url) {
        return this.urlRegistry.get(url)?.alias ?? hostOf(url);
    }
    /** 该 URL 是否已登记（漏登记的 URL 一律放行，此处供消费端核对台账覆盖面）。 */
    isRegistered(url) {
        return this.urlRegistry.has(url);
    }
    /** Retry-After 边带绝对截止时刻；未登记/未设罚 = 0。 */
    penaltyUntil(url) {
        return this.bucketOf(url)?.penaltyUntil ?? 0;
    }
    /** 按登记顺序返回已登记端点 { url, alias, budget, reqPerMin }（reqPerMin 取桶当前生效值，
     *  准确反映同组取 min 后的真实速率；供服务端内部关联 PoolMeter / BreakerRegistry 使用）。 */
    endpoints() {
        const out = [];
        for (const [url, reg] of this.urlRegistry) {
            out.push({
                url,
                alias: reg.alias,
                budget: reg.budget,
                reqPerMin: this.buckets.get(reg.budget)?.reqPerMin ?? 0,
            });
        }
        return out;
    }
    /** 完全脱敏观测快照：零原始 URL、零凭据子串；budget === url 的独享端点回退为 alias（host#kN）。 */
    snapshot() {
        const now = this.now();
        return this.endpoints().map((e) => ({
            alias: e.alias,
            budget: e.budget === e.url ? e.alias : e.budget,
            reqPerMin: e.reqPerMin,
            hasToken: this.hasToken(e.url),
            penaltyUntilSec: Math.max(0, Math.ceil((this.penaltyUntil(e.url) - now) / 1000)),
            tokenRecoverySec: Math.max(0, Math.ceil((this.earliestTokenRecovery(e.url) - now) / 1000)),
        }));
    }
    bucketOf(url) {
        const reg = this.urlRegistry.get(url);
        return reg ? this.buckets.get(reg.budget) : undefined;
    }
    refill(bucket, t) {
        if (t <= bucket.lastMs)
            return;
        bucket.tokens = Math.min(bucket.capacity, bucket.tokens + ((t - bucket.lastMs) * bucket.reqPerMin) / 60_000);
        bucket.lastMs = t;
    }
    nextTokenAt(bucket, t) {
        if (bucket.tokens >= 1)
            return t;
        return t + Math.ceil(((1 - bucket.tokens) * 60_000) / bucket.reqPerMin);
    }
}
