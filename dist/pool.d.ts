import { type BreakerTuning } from "./endpoint-breaker.js";
import { type BlockRange } from "./range.js";
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
export interface RpcPoolOptions {
    now?: () => number;
    quota?: BreakerTuning;
    archive?: BreakerTuning;
    transient?: BreakerTuning;
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
    private readonly now;
    constructor(entries: readonly RpcPoolEntry<C>[], opts?: RpcPoolOptions);
    /**
     * 粘性优先通用调用：恒按构造序过滤冷却端点，从首个开始试；
     * rejected/reverted 滑下家不冷却，quota/archive/transient 冷却后滑下家，ok 清零。
     * 回调返回 null/undefined 视同 transient（对合法返回 null 的方法请自行包 sentinel）。
     */
    call<T>(fn: (client: C, url: string) => Promise<T>): Promise<T>;
    snapshot(): Array<{
        host: string;
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
     * 拆到 MIN_CHUNK 仍失败才滑下家。块号 bigint，from > to 返回 []。
     * lane 缺省 "topic"；学习值按 lane 分桶（maxTopicRange / maxAddressRange），只收紧取历史最小。
     */
    callLogs<T>(range: BlockRange, fn: (client: C, range: BlockRange) => Promise<readonly T[]>, opts?: {
        lane?: LogLane;
    }): Promise<T[]>;
    /** 端点内取一段范围；range 超限则自适应拆分递归，rejected/reverted/range到底 转滑动信号，其余按分型上抛。 */
    private logsRange;
    /** 按分型处置单端点失败，返回脱敏原因供聚合报错。 */
    private fail;
    private coolingError;
}
