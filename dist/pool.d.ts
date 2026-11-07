import { EndpointBreaker, type BreakerTuning } from "./endpoint-breaker.js";
import { KeyRateLimiter } from "./limiter.js";
import { type BlockRange } from "./range.js";
import { PoolThrottledError } from "./throttle.js";
/** 从独立共享模块 re-export（v0.5.0 起定义移至 src/throttle.ts），保持 v0.1.x 导入路径兼容。 */
export { PoolThrottledError };
export interface EntryCaps {
    /** 是否支持按 topic 过滤的 getLogs（false = topic 候选中跳过该端点） */
    topicLogs?: boolean;
    /** topic 查询单次最大块跨度；<=0 视同不支持该 lane */
    topicLogRange?: number;
    /** address-only 查询单次最大块跨度；<=0 视同不支持该 lane */
    addressLogRange?: number;
}
export interface RpcPoolEntry<C> {
    url: string;
    client: C;
    caps?: EntryCaps;
}
/** 候选排序策略：priority = 粘性优先（默认, v0.2.0 语义）；rotation = 活端点轮转。 */
export type PoolSelection = "priority" | "rotation";
export interface RpcPoolOptions {
    now?: () => number;
    quota?: BreakerTuning;
    /** rate（429 短时限流）分型冷却调参；缺省用 DEFAULT_TUNING.rate（30s ×2 短梯） */
    rate?: BreakerTuning;
    archive?: BreakerTuning;
    transient?: BreakerTuning;
    /** getLogs 自适应拆分的最小块跨度下限（默认 MIN_CHUNK = 10n；必须 >= 1） */
    minChunk?: number | bigint;
    /** 候选排序：'priority'（默认）恒按构造序过滤冷却从首个试起；'rotation' 从轮转指针起
     *  对活端点循环排列（冷却端点当轮不占候选位；全冷却时对全体循环兜底保进度）。 */
    selection?: PoolSelection;
    /** 共享令牌桶（同凭据跨池共用）；注入后候选层 hasToken 预判 + 全受限聚合成 PoolThrottledError。
     *  权威扣费仍在其下 fetchFn 的 acquireAndWait——本层预判只是快速跳过。 */
    rateLimiter?: KeyRateLimiter;
    /** 可选外部断路器工厂（如 `(url) => registry.get(url)`）；缺省每池每条目独立创建私有 EndpointBreaker。
     *  注意：切勿将同一 EndpointBreaker 实例同时用于本池与内层 fetchImpl 包装，否则半开单探针互斥锁会重入自锁。 */
    breakerFor?: (url: string) => EndpointBreaker;
}
/** 全部端点均在冷却（含探针在途）时抛出，带最早可重试时间与脱敏标签。 */
export declare class PoolCoolingError extends Error {
    readonly retryAfterMs: number;
    constructor(retryAfterMs: number, labels: readonly string[]);
}
/** getLogs 查询通道：topic = 带 topic 过滤；address = 仅按地址过滤。 */
export type LogLane = "topic" | "address";
export declare class RpcPool<C> {
    private readonly entries;
    private readonly entryByUrl;
    private readonly now;
    private readonly selection;
    private readonly rateLimiter?;
    private readonly minChunk;
    private rotationIndex;
    constructor(entries: readonly RpcPoolEntry<C>[], opts?: RpcPoolOptions);
    /** selection 候选排序。rotation: 只读 snapshot 预筛活端点（不触碰 begin() 的半开探针
     *  副作用），从轮转指针起对活集循环排列；指针每调用递增，冷却端点当轮不占候选位
     *  （活集大小变化由 mod 吸收）；全冷却时对全体循环兜底保进度。熔断 begin/report 按
     *  返回候选序在调用处执行，与 priority 完全共用。 */
    private ordered;
    /**
     * 粘性优先通用调用：恒按构造序过滤冷却端点，从首个开始试；
     * rejected/reverted 滑下家不冷却，quota/archive/transient 冷却后滑下家，ok 清零。
     * 注入 rateLimiter 时无令牌候选直接跳过；存在本地受限候选时（无论是否混入其他失败）
     * 统一抛 PoolThrottledError（静默延期语义，spec §5.3）。
     * 回调返回 undefined 恒视同 transient；返回 null 默认同 transient，
     * opts.allowNull === true 时视为成功（report("ok") 并原样返回 null，spec §5.4）。
     */
    call<T>(fn: (client: C, url: string) => Promise<T>, opts?: {
        allowNull?: boolean;
    }): Promise<T>;
    snapshot(): Array<{
        host: string;
        /** 匿名 key 编号（同 host 多凭据各自成行） */
        keyId: string;
        failures: number;
        cooldownSec: number;
        maxTopicRange: bigint | null;
        maxAddressRange: bigint | null;
        /** 弃用别名 = maxTopicRange（topic 通道学习值），供 v0.1.x 消费方过渡 */
        maxLogRange: bigint | null;
    }>;
    /**
     * getLogs：与 call 同候选序，但先按 caps 与 lane 过滤（过滤后为空则用全体），
     * 再按端点在对应 lane 已学的范围预分块；范围超限在端点内自适应拆分（提取上限一次切到位/对半二分），
     * 拆到下限（minChunk，默认 MIN_CHUNK = 10n）仍失败才滑下家。块号 bigint，from > to 返回 []。
     * lane 缺省 "topic"；学习值按 lane 分桶（maxTopicRange / maxAddressRange），只收紧取历史最小。
     * 混合失败裁决与 call 一致（spec §5.3）：存在本地受限候选即抛 PoolThrottledError。
     */
    callLogs<T>(range: BlockRange, fn: (client: C, range: BlockRange) => Promise<readonly T[]>, opts?: {
        lane?: LogLane;
    }): Promise<T[]>;
    /** 端点内取一段范围；range 超限则自适应拆分递归，rejected/reverted/range到底 转滑动信号，其余按分型上抛。 */
    private logsRange;
    /** 按分型处置单端点失败，返回脱敏原因供聚合报错。 */
    private fail;
    /** 全部候选本地受限：ETA = 逐候选 max(冷却到期, 令牌恢复, 已观察节流截止) 后取 min。
     *  取 min 是因为任一候选到点即可服务；双全局 min 会把不同候选的最优值错配出 0。 */
    private throttledError;
    private coolingError;
}
