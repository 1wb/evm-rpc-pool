export const DEFAULT_TUNING = {
    quota: { baseMs: 2 * 60_000, factor: 4, capMs: 6 * 3_600_000 },
    rate: { baseMs: 30_000, factor: 2, capMs: 10 * 60_000 },
    archive: { baseMs: 30 * 60_000, factor: 2, capMs: 6 * 3_600_000 },
    transient: { baseMs: 30_000, factor: 2, capMs: 10 * 60_000 },
};
/**
 * 单端点状态机：分型指数退避冷却 + 半开单探针。
 * rejected/range/reverted 不进入本类（端点健康，仅滑下家）——见 RpcPool。
 */
export class EndpointBreaker {
    failures = 0;
    // 命名 _cooldownUntil：与公开 getter cooldownUntil 撞名（TS2300），brief 原文不可编译
    _cooldownUntil = 0;
    /** Retry-After 边带（绝对截止时间）；冷却有效性取 max(阶梯, 边带)，过期自然失效 */
    _penaltyUntil = 0;
    /** 半开探针在途：冷却到期放行的第 1 发尚未回报，期间其余请求跳过 */
    probing = false;
    now;
    tuning;
    constructor(opts = {}) {
        this.now = opts.now ?? Date.now;
        this.tuning = {
            quota: opts.quota ?? DEFAULT_TUNING.quota,
            rate: opts.rate ?? DEFAULT_TUNING.rate,
            archive: opts.archive ?? DEFAULT_TUNING.archive,
            transient: opts.transient ?? DEFAULT_TUNING.transient,
        };
    }
    /** 冷却已过且无探针在途才放行；冷却到期的本发放行为探针（同步置位，单线程无竞态）。 */
    begin() {
        if (this.probing)
            return false;
        if (this.now() < this.effectiveCooldownUntil())
            return false;
        if (this.effectiveCooldownUntil() > 0)
            this.probing = true;
        return true;
    }
    report(kind, opts) {
        this.probing = false;
        if (kind === "ok") {
            this.failures = 0;
            this._cooldownUntil = 0;
            this._penaltyUntil = 0;
            return;
        }
        this.failures += 1;
        // 未知 kind（透传消费方带来的新分型）回退 transient 档，不崩
        const { baseMs, factor, capMs } = this.tuning[kind] ?? this.tuning.transient;
        this._cooldownUntil = this.now() + Math.min(baseMs * factor ** (this.failures - 1), capMs);
        // Retry-After 直取：绝对截止时间边带（≥5s），与阶梯取 max 后生效
        if (opts?.retryAfterMs !== undefined && opts.retryAfterMs > 0) {
            this.setPenalty(this.now() + Math.max(opts.retryAfterMs, 5_000));
        }
    }
    /** Retry-After 边带（绝对截止时间）；重复设置取更晚，过期自然失效。 */
    setPenalty(untilMs) {
        this._penaltyUntil = Math.max(this._penaltyUntil, untilMs);
    }
    /** rejected/range/reverted：端点健康，仅终止在途探针，不计数不冷却。 */
    slide() {
        this.probing = false;
    }
    /** 有效冷却截止 = max(分型阶梯, Retry-After 边带)——单一绝对截止规则。 */
    get cooldownUntil() {
        return this.effectiveCooldownUntil();
    }
    effectiveCooldownUntil() {
        return Math.max(this._cooldownUntil, this._penaltyUntil);
    }
    snapshot(now) {
        return {
            failures: this.failures,
            cooldownSec: Math.max(0, Math.ceil((this.effectiveCooldownUntil() - now) / 1000)),
        };
    }
}
