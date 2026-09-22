import { type EntryCaps, type RpcPoolOptions } from "./pool.js";
export interface RpcResponse {
    ok: boolean;
    status: number;
    json(): Promise<unknown>;
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
}
export declare class FetchRpcPool {
    private readonly pool;
    constructor(urls: string[], opts?: FetchRpcPoolOptions);
    call<T>(method: string, params: unknown): Promise<T>;
    /** fromBlock/toBlock 接受 number 或 bigint（number 侧为历史消费方便利），内部统一 bigint。 */
    getLogs<T>(opts: {
        address: string;
        topics: string[];
        fromBlock: number | bigint;
        toBlock: number | bigint;
    }): Promise<T[]>;
    snapshot(): {
        host: string;
        failures: number;
        cooldownSec: number;
        maxTopicRange: bigint | null;
        maxAddressRange: bigint | null;
        maxLogRange: bigint | null;
    }[];
}
