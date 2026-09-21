import { describe, expect, it } from "vitest";
import { redactError, redactUrl } from "../src/redact";

describe("redactUrl", () => {
  it("只留协议与 host", () => {
    expect(redactUrl("https://mainnet.infura.io/v3/SECRETKEY123")).toBe("https://mainnet.infura.io/…");
  });
  it("非法 URL 给占位", () => {
    expect(redactUrl("not-a-url")).toBe("<invalid-rpc-url>");
  });
});

describe("redactError", () => {
  it("消息中的完整 URL 替换为 host 形态", () => {
    const msg = redactError(new Error("POST https://mainnet.infura.io/v3/SECRETKEY123 failed"));
    expect(msg).not.toContain("SECRETKEY123");
    expect(msg).toContain("mainnet.infura.io");
  });
  it("已知 URL 的长 path/query 片段也替换", () => {
    const msg = redactError(new Error("key LONGSECRET99 invalid"), ["https://x.example/v2/LONGSECRET99"]);
    expect(msg).toContain("<redacted>");
  });
});
