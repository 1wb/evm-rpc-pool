/** 本地令牌桶限速（非供应商故障）：等待至多 maxWaitMs，超时抛出。 */
export declare class LocalThrottleError extends Error {
    readonly retryAfterMs: number;
    constructor(retryAfterMs: number);
}
/**
 * 沿 cause 链识别本地节流错误。viem 会把 fetchFn 抛出的错误包装成
 * HttpRequestError（instanceof 失效），必须走 cause 链；name 兜底双副本场景。
 */
export declare function isLocalThrottle(error: unknown): boolean;
/** 沿 cause 链提取 LocalThrottleError.retryAfterMs；非本地节流返回 null。 */
export declare function localThrottleRetryAfterMs(error: unknown): number | null;
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
export declare class KeyRateLimiter {
    private readonly buckets;
    private readonly urlRegistry;
    private readonly hostSeq;
    private readonly listeners;
    private readonly acquireWaitMs;
    private readonly capacity;
    private readonly now;
    private readonly sleep;
    constructor(opts?: KeyRateLimiterOptions);
    /** 启动预登记（稳定 host#kN 别名，非异步首见）；同 URL 重复登记保持首见。 */
    registerEndpoints(entries: readonly LimitEndpointEntry[]): void;
    /** 查询不消费：penalty 未过或无令牌即 false。 */
    hasToken(url: string): boolean;
    /** 无令牌等待至多 maxWaitMs（等待型获取：分片序列慢下来续跑，而非弃前缀）；超时抛 LocalThrottleError。 */
    acquireAndWait(url: string, maxWaitMs?: number): Promise<void>;
    /** Retry-After 边带：写入该 budget 桶（取更晚）并通知监听者（pool → breaker）。 */
    setPenalty(url: string, untilMs: number): void;
    addPenaltyListener(fn: (url: string, untilMs: number) => void): void;
    /** 该 url 下一令牌可用的绝对时刻（含 penalty）；未登记/不限 = 0（随时可用）。 */
    earliestTokenRecovery(url: string): number;
    /** 脱敏别名 host#kN（按 host 计数，启动顺序稳定）。 */
    urlAlias(url: string): string;
    private bucketOf;
    private refill;
    private nextTokenAt;
}
