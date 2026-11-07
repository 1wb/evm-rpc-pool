import { type EntryCaps, type LogLane, type RpcPoolOptions } from "./pool.js";
export interface RpcResponse {
    ok: boolean;
    status: number;
    json(): Promise<unknown>;
    /** Retry-After 提取用；假实现可缺省 */
    headers?: {
        get(name: string): string | null;
    };
}
export type FetchLike = (url: string, init: {
    method: string;
    headers: Record<string, string>;
    body: string;
    signal?: AbortSignal;
}) => Promise<RpcResponse>;
export interface FetchRpcPoolOptions extends RpcPoolOptions {
    fetchImpl?: FetchLike;
    timeoutMs?: number;
    /** host → 能力表（如 { "base-rpc.publicnode.com": { topicLogs: false } }） */
    hostCaps?: Record<string, EntryCaps>;
    /** 可空方法白名单（方法名集合）：命中方法返回合法 null 视为成功不冷却；
     *  传入时整体替换默认白名单，缺省用 DEFAULT_NULLABLE_METHODS（spec §5.4）。 */
    nullableMethods?: Iterable<string>;
}
/** 默认可空方法白名单：合法业务语义允许返回 null 的查询方法（spec §5.4）。
 *  注：eth_getBlockByNumber / eth_getBlockByHash 有意不在名单内——请求已知区块
 *  命中同步滞后节点返回 null 时须继续按 transient 滑向健康节点。 */
export declare const DEFAULT_NULLABLE_METHODS: ReadonlySet<string>;
/** getLogs topics 过滤项：单值、位置内嵌套数组 OR（如 [[t0, t1]]）或 null 通配位。 */
export type TopicFilter = ReadonlyArray<string | readonly string[] | null>;
export interface GetLogsOptions {
    /** 可选：缺省、空串 "" 或空数组 [] 时省略 JSON-RPC filter 中的 address 字段；支持单地址或地址数组 OR */
    address?: string | readonly string[];
    /** 可选：undefined 时省略 topics 字段，显式传 [] 时原样保留 topics: [] */
    topics?: TopicFilter;
    fromBlock: number | bigint;
    toBlock: number | bigint;
    /** 可选显式指定分桶通道；缺省按 topics 自动派生（存在有效过滤项走 "topic"，否则走 "address"） */
    lane?: LogLane;
}
export declare class FetchRpcPool {
    private readonly pool;
    private readonly nullableMethods;
    constructor(urls: string[], opts?: FetchRpcPoolOptions);
    call<T>(method: string, params: unknown, opts?: {
        allowNull?: boolean;
    }): Promise<T>;
    /** fromBlock/toBlock 接受 number 或 bigint（number 侧为历史消费方便利），内部统一 bigint。
     *  出站 filter 构造（spec §5.5）：address 缺省/空串/空数组时省略该键；topics 仅 undefined
     *  时省略（显式 [] 原样保留）；lane 显式优先，缺省按 topics 形状自动派生。 */
    getLogs<T>(opts: GetLogsOptions): Promise<T[]>;
    snapshot(): {
        host: string;
        keyId: string;
        failures: number;
        cooldownSec: number;
        maxTopicRange: bigint | null;
        maxAddressRange: bigint | null;
        maxLogRange: bigint | null;
    }[];
}
