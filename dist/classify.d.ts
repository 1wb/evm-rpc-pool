export type RpcErrorKind = "quota" | "rate" | "archive" | "transient" | "reverted" | "rejected" | "range";
/** 预分型错误：fetch 工厂在能确知分型（HTTP 状态、200+JSON-RPC error）时抛出，内核直接采信。 */
export declare class RpcKindError extends Error {
    readonly kind: RpcErrorKind;
    readonly rangeLimit: bigint | null;
    /** Retry-After 毫秒（调用方从响应头解析后携带）；冷却直取不进阶梯 */
    readonly retryAfterMs: number | null;
    constructor(kind: RpcErrorKind, message: string, rangeLimit?: bigint | null, 
    /** Retry-After 毫秒（调用方从响应头解析后携带）；冷却直取不进阶梯 */
    retryAfterMs?: number | null);
}
/**
 * 解析 Retry-After 头：秒数与 HTTP-date 双格式，返回距 nowMs 的毫秒。
 * 无效/负数/非有限值回退 null（调用方走分型阶梯）。
 */
export declare function parseRetryAfter(value: string | null, nowMs: number): number | null;
export declare const RATE_TEXT: RegExp;
export declare const QUOTA_TEXT: RegExp;
export declare const ARCHIVE_TEXT: RegExp;
/** 从错误文案提取 getLogs 单次范围上限；读不出返回 null（调用方对半二分）。 */
export declare function rangeLimitFromMessage(msg: string): bigint | null;
export declare function isRangeLimitMessage(msg: string): boolean;
export declare function isContractRevertMessage(msg: string): boolean;
/**
 * 通用文本分型，序：range → reverted → quota → archive → transient。
 * "rejected"（端点健康但本次无法满足）只在 fetch 工厂能区分「200 带 JSON-RPC error」时产生，
 * 文本层面不可判，故本函数不返回它。
 */
export declare function classifyRpcError(error: unknown): Exclude<RpcErrorKind, "rejected">;
