import { describe, expect, it } from "vitest";
import { MIN_CHUNK, splitBlockRange } from "../src/range";

describe("splitBlockRange", () => {
  it("按 chunk 切片，尾片钳到 to", () => {
    expect(splitBlockRange(0n, 9999n, 3000n)).toEqual([
      { from: 0n, to: 2999n },
      { from: 3000n, to: 5999n },
      { from: 6000n, to: 8999n },
      { from: 9000n, to: 9999n },
    ]);
  });
  it("from > to 返回空", () => {
    expect(splitBlockRange(10n, 9n, 100n)).toEqual([]);
  });
  it("MIN_CHUNK 为 2000n，chunk < 1 抛错", () => {
    expect(MIN_CHUNK).toBe(2000n);
    expect(() => splitBlockRange(0n, 10n, 0n)).toThrow();
  });
});
