import { KeyRateLimiter } from "./limiter.js";
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
/**
 * 按供应商、key（匿名编号）、RPC 方法计量实际消耗与失败原因。
 * 内部累计永不清零；snapshotDelta 给出窗口增量，confirmDelta 推进水位（发送成功才确认）。
 */
export declare class PoolMeter {
    private readonly cells;
    private committed;
    private readonly pushes;
    private pushesCommitted;
    /** 启动预登记（仅为别处对账枚举端点留底；记录本身不要求先登记）。 */
    registerEndpoints(urls: readonly string[]): void;
    /** 直接计量一次调用/请求（probe 等自控全流程的调用方用）。 */
    record(url: string, r: {
        method: string;
        status?: number;
        rpcError?: boolean;
        localThrottled?: boolean;
    }): void;
    /** meteredFetch 用：HTTP 结果落格；2xx 的 ok/rpcError 由 body 异步判定后经 noteBodyVerdict 补记。 */
    noteHttpResult(url: string, method: string, status: number): void;
    noteBodyVerdict(url: string, method: string, verdict: BodyVerdict): void;
    /** WSS 推送计量：每条新头/日志通知 +1。 */
    recordPush(url: string): void;
    /** 自上次 confirmDelta() 的窗口增量；无事件的端点不出现在结果里。 */
    snapshotDelta(): MeterSnapshot;
    /** 水位确认：当前累计固化为基线（调用时机 = 摘要推送成功后）。 */
    confirmDelta(): void;
    private bump;
}
/**
 * viem `http(url, { fetchFn })` 注入用：实际 HTTP 发送前的权威扣费点（等待型获取），
 * 每次真实请求计量（覆盖 callLogs 拆片与 viem 内部重试——后者本项目以 retryCount:0 关闭），
 * 429 读 Retry-After 调 limiter.setPenalty（边带同步 breaker——JSON-RPC error body 丢响应头路径的对策）。
 */
export declare function meteredFetch(meter: PoolMeter, limiter: KeyRateLimiter, base?: typeof fetch): typeof fetch;
export {};
