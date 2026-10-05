export interface BreakerTuning {
    baseMs: number;
    factor: number;
    capMs: number;
}
export type CoolingKind = "quota" | "rate" | "transient" | "archive";
export declare const DEFAULT_TUNING: Record<CoolingKind, BreakerTuning>;
export interface EndpointBreakerOptions {
    now?: () => number;
    quota?: BreakerTuning;
    rate?: BreakerTuning;
    archive?: BreakerTuning;
    transient?: BreakerTuning;
}
/**
 * 单端点状态机：分型指数退避冷却 + 半开单探针。
 * rejected/range/reverted 不进入本类（端点健康，仅滑下家）——见 RpcPool。
 */
export declare class EndpointBreaker {
    private failures;
    private _cooldownUntil;
    /** Retry-After 边带（绝对截止时间）；冷却有效性取 max(阶梯, 边带)，过期自然失效 */
    private _penaltyUntil;
    /** 半开探针在途：冷却到期放行的第 1 发尚未回报，期间其余请求跳过 */
    private probing;
    private readonly now;
    private readonly tuning;
    constructor(opts?: EndpointBreakerOptions);
    /** 冷却已过且无探针在途才放行；冷却到期的本发放行为探针（同步置位，单线程无竞态）。 */
    begin(): boolean;
    report(kind: "ok" | CoolingKind, opts?: {
        retryAfterMs?: number;
    }): void;
    /** Retry-After 边带（绝对截止时间）；重复设置取更晚，过期自然失效。 */
    setPenalty(untilMs: number): void;
    /** rejected/range/reverted：端点健康，仅终止在途探针，不计数不冷却。 */
    slide(): void;
    /** 有效冷却截止 = max(分型阶梯, Retry-After 边带)——单一绝对截止规则。 */
    get cooldownUntil(): number;
    private effectiveCooldownUntil;
    snapshot(now: number): {
        failures: number;
        cooldownSec: number;
    };
}
