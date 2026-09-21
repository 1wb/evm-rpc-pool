import { describe, expect, it } from "vitest";
import {
  RpcKindError,
  classifyRpcError,
  isContractRevertMessage,
  isRangeLimitMessage,
  rangeLimitFromMessage,
} from "../src/classify";

describe("rangeLimitFromMessage", () => {
  it("提取各节点形态的范围上限", () => {
    expect(rangeLimitFromMessage("range must not exceed 5000")).toBe(5000n);
    expect(rangeLimitFromMessage("Up to 10000 blocks can be queried")).toBe(10000n);
    expect(rangeLimitFromMessage("query returned more than 10000 results")).toBe(10000n);
    expect(rangeLimitFromMessage("0 - 1500 blocks range allowed")).toBe(1500n);
    expect(rangeLimitFromMessage("execution reverted")).toBeNull();
  });
});

describe("isRangeLimitMessage", () => {
  it("识别范围类错误（含无数字文案）", () => {
    expect(isRangeLimitMessage("block range too wide")).toBe(true);
    expect(isRangeLimitMessage("eth_getLogs range limit exceeded")).toBe(true);
    expect(isRangeLimitMessage("rate limit exceeded")).toBe(false);
  });
});

describe("classifyRpcError", () => {
  it("quota：状态码数字与限速文案", () => {
    expect(classifyRpcError(new Error("HTTP 429"))).toBe("quota");
    expect(classifyRpcError(new Error("HTTP 402"))).toBe("quota");
    expect(classifyRpcError(new Error("too many requests"))).toBe("quota");
    expect(classifyRpcError(new Error("credits exhausted"))).toBe("quota");
    expect(classifyRpcError(new Error("monthly quota exceeded"))).toBe("quota");
  });
  it("archive：历史状态文案", () => {
    expect(classifyRpcError(new Error("archive node required"))).toBe("archive");
    expect(classifyRpcError(new Error("historical state unavailable"))).toBe("archive");
  });
  it("reverted：合约执行回退", () => {
    expect(isContractRevertMessage("execution reverted")).toBe(true);
    expect(classifyRpcError(new Error("execution reverted"))).toBe("reverted");
  });
  it("range 优先于其它判定", () => {
    expect(classifyRpcError(new Error("block range too wide"))).toBe("range");
  });
  it("其余归 transient", () => {
    expect(classifyRpcError(new Error("fetch failed"))).toBe("transient");
  });
  it("RpcKindError 原样携带 kind 与 rangeLimit", () => {
    const e = new RpcKindError("rejected", "method not found");
    expect(e.kind).toBe("rejected");
    expect(e.rangeLimit).toBeNull();
  });
});
