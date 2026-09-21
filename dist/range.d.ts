export declare const MIN_CHUNK = 2000n;
export interface BlockRange {
    fromBlock: bigint;
    toBlock: bigint;
}
/** 把 [from, to] 切成不超过 chunk 块的若干片（公共 RPC 有跨度上限）；from > to 返回空数组。 */
export declare function splitBlockRange(from: bigint, to: bigint, chunk: bigint): Array<{
    from: bigint;
    to: bigint;
}>;
