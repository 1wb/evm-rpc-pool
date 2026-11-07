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
/**
 * 按完整 URL 懒创建/共享 EndpointBreaker 的注册表：多池经 `breakerFor: (url) => registry.get(url)`
 * 注入即共享冷却与半开探针状态。只读观测（snapshot）不脏建实例；setPenalty 未建实例也会初始化并写入。
 * 注意：同一 EndpointBreaker 实例不得同时用于外层池与内层 fetchImpl 包装（半开单探针互斥锁会重入自锁）。
 */
export declare class BreakerRegistry {
    private readonly map;
    private readonly opts;
    private readonly now;
    constructor(opts?: EndpointBreakerOptions);
    /** 按完整 URL 懒获取/创建对应的 EndpointBreaker 实例。 */
    get(url: string): EndpointBreaker;
    has(url: string): boolean;
    /** 对该 URL 写入 Retry-After 绝对截止时间边带（未建实例也会初始化并写入）。 */
    setPenalty(url: string, untilMs: number): void;
    /** 只读快照：未触碰过的健康 URL 直接返回 { failures: 0, cooldownSec: 0 } 且无需分配实例。 */
    snapshot(url: string, now?: number): {
        failures: number;
        cooldownSec: number;
    };
    /** 清空全部已建断路器（供测试重入重置）。 */
    clear(): void;
}
