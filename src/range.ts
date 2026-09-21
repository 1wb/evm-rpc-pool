export const MIN_CHUNK = 2_000n;

export interface BlockRange {
  fromBlock: bigint;
  toBlock: bigint;
}

/** 把 [from, to] 切成不超过 chunk 块的若干片（公共 RPC 有跨度上限）；from > to 返回空数组。 */
export function splitBlockRange(
  from: bigint,
  to: bigint,
  chunk: bigint,
): Array<{ from: bigint; to: bigint }> {
  if (chunk < 1n) throw new Error("chunk 必须 ≥ 1");
  const out: Array<{ from: bigint; to: bigint }> = [];
  for (let s = from; s <= to; s += chunk) {
    const e = s + chunk - 1n;
    out.push({ from: s, to: e > to ? to : e });
  }
  return out;
}
