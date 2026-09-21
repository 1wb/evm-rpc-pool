import { describe, expect, it } from "vitest";
import { hostOf, parseRpcUrls } from "../src/url";

describe("hostOf", () => {
  it("取 host，容忍非法 URL", () => {
    expect(hostOf("https://a.example/rpc")).toBe("a.example");
    expect(hostOf("http://localhost:8545")).toBe("localhost:8545");
    expect(hostOf("not-a-url")).toBe("not-a-url");
  });
});

describe("parseRpcUrls", () => {
  it("去空格、滤空段，顺序即优先级", () => {
    expect(parseRpcUrls(" a , , b ")).toEqual(["a", "b"]);
  });
  it("空列表报错，文案含变量名", () => {
    expect(() => parseRpcUrls(" , ")).toThrow("RPC_URLS 未配置");
    expect(() => parseRpcUrls(" , ", "BASE_RPC_URLS")).toThrow("BASE_RPC_URLS 未配置");
  });
});
