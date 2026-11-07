/** 全部候选被本地令牌桶限住（非供应商故障）时抛出；调用方静默延期，不计失败不告警。
 *  ETA = 逐候选 max(冷却到期, 令牌恢复, 已观察节流截止) 后取 min。 */
export declare class PoolThrottledError extends Error {
    readonly retryAfterMs: number;
    constructor(retryAfterMs: number);
}
/**
 * 沿 cause 链识别节流错误（本地令牌桶 LocalThrottleError 或池聚合 PoolThrottledError）。
 * viem 会把 fetchFn 抛出的错误包装成 HttpRequestError（instanceof 失效），必须走 cause 链；
 * name 兜底双副本/跨 realm 场景。最多向下探索 10 层：循环 cause 链在深度上限内安全终止，
 * 非 Error 原始值直接返回 false。
 */
export declare function isThrottleError(error: unknown): boolean;
/** 沿 cause 链提取节流错误 retryAfterMs：同构遍历，命中时取有限正数（<=0/缺失归一化为 0）；
 *  未命中任何节流错误返回 null。深度上限与循环 cause 链安全性同 isThrottleError。 */
export declare function throttleRetryAfterMs(error: unknown): number | null;
