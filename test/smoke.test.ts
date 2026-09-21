import { describe, expect, it } from "vitest";
import { EVI_RPC_POOL } from "../src/index";

describe("工具链冒烟", () => {
  it("导入可用", () => {
    expect(EVI_RPC_POOL).toBe("evm-rpc-pool");
  });
});
