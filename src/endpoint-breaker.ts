export interface BreakerTuning {
  baseMs: number;
  factor: number;
  capMs: number;
}

export type CoolingKind = "quota" | "transient" | "archive";

export const DEFAULT_TUNING: Record<CoolingKind, BreakerTuning> = {
  quota: { baseMs: 2 * 60_000, factor: 4, capMs: 6 * 3_600_000 },
  archive: { baseMs: 30 * 60_000, factor: 2, capMs: 6 * 3_600_000 },
  transient: { baseMs: 30_000, factor: 2, capMs: 10 * 60_000 },
};

export interface EndpointBreakerOptions {
  now?: () => number;
  quota?: BreakerTuning;
  archive?: BreakerTuning;
  transient?: BreakerTuning;
}

/**
 * 单端点状态机：分型指数退避冷却 + 半开单探针。
 * rejected/range/reverted 不进入本类（端点健康，仅滑下家）——见 RpcPool。
 */
export class EndpointBreaker {
  private failures = 0;
  // 命名 _cooldownUntil：与公开 getter cooldownUntil 撞名（TS2300），brief 原文不可编译
  private _cooldownUntil = 0;
  /** 半开探针在途：冷却到期放行的第 1 发尚未回报，期间其余请求跳过 */
  private probing = false;
  private readonly now: () => number;
  private readonly tuning: Record<CoolingKind, BreakerTuning>;

  constructor(opts: EndpointBreakerOptions = {}) {
    this.now = opts.now ?? Date.now;
    this.tuning = {
      quota: opts.quota ?? DEFAULT_TUNING.quota,
      archive: opts.archive ?? DEFAULT_TUNING.archive,
      transient: opts.transient ?? DEFAULT_TUNING.transient,
    };
  }

  /** 冷却已过且无探针在途才放行；冷却到期的本发放行为探针（同步置位，单线程无竞态）。 */
  begin(): boolean {
    if (this.probing) return false;
    if (this.now() < this._cooldownUntil) return false;
    if (this._cooldownUntil > 0) this.probing = true;
    return true;
  }

  report(kind: "ok" | CoolingKind): void {
    this.probing = false;
    if (kind === "ok") {
      this.failures = 0;
      this._cooldownUntil = 0;
      return;
    }
    this.failures += 1;
    const { baseMs, factor, capMs } = this.tuning[kind];
    this._cooldownUntil = this.now() + Math.min(baseMs * factor ** (this.failures - 1), capMs);
  }

  /** rejected/range/reverted：端点健康，仅终止在途探针，不计数不冷却。 */
  slide(): void {
    this.probing = false;
  }

  get cooldownUntil(): number {
    return this._cooldownUntil;
  }

  snapshot(now: number): { failures: number; cooldownSec: number } {
    return {
      failures: this.failures,
      cooldownSec: Math.max(0, Math.ceil((this._cooldownUntil - now) / 1000)),
    };
  }
}
