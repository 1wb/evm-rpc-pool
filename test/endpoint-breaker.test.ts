import { describe, expect, it } from "vitest";
import { EndpointBreaker } from "../src/endpoint-breaker";

function makeClock() {
  let t = 1_000_000;
  return { now: () => t, tick: (ms: number) => { t += ms; } };
}

describe("EndpointBreaker", () => {
  it("新端点直接放行", () => {
    expect(new EndpointBreaker().begin()).toBe(true);
  });

  it("quota：2min 起步、×4 递增、封顶 6h", () => {
    const clock = makeClock();
    const b = new EndpointBreaker({ now: clock.now });
    b.report("quota");
    expect(b.snapshot(clock.now()).cooldownSec).toBe(120);
    clock.tick(120_000);
    expect(b.begin()).toBe(true); // 到期放行探针
    b.report("quota");
    expect(b.snapshot(clock.now()).cooldownSec).toBe(480); // 120×4
    for (let i = 0; i < 8; i++) {
      clock.tick(6 * 3_600_000); // 每次冷却走完再来一发
      expect(b.begin()).toBe(true);
      b.report("quota");
    }
    expect(b.snapshot(clock.now()).cooldownSec).toBe(6 * 3_600); // 封顶 6h
  });

  it("transient：30s ×2 封顶 10min；archive：30min ×2 封顶 6h", () => {
    const clock = makeClock();
    const t = new EndpointBreaker({ now: clock.now });
    t.report("transient");
    expect(t.snapshot(clock.now()).cooldownSec).toBe(30);
    clock.tick(30_000);
    expect(t.begin()).toBe(true);
    t.report("transient");
    expect(t.snapshot(clock.now()).cooldownSec).toBe(60);
    clock.tick(60_000);
    for (let i = 0; i < 6; i++) {
      clock.tick(10 * 60_000); // 每次冷却走完再来一发（同 quota 模式；tick 在 report 后会使封顶在快照时恒已走完，600 断言不可达）
      expect(t.begin()).toBe(true);
      t.report("transient");
    }
    expect(t.snapshot(clock.now()).cooldownSec).toBe(600); // 10min 封顶

    const a = new EndpointBreaker({ now: clock.now });
    a.report("archive");
    expect(a.snapshot(clock.now()).cooldownSec).toBe(1800);
  });

  it("半开探针在途时其余请求跳过；ok 全清零", () => {
    const clock = makeClock();
    const b = new EndpointBreaker({ now: clock.now });
    b.report("quota");
    clock.tick(120_000);
    expect(b.begin()).toBe(true); // 探针放行
    expect(b.begin()).toBe(false); // 探针在途，其余跳过
    b.report("ok");
    expect(b.snapshot(clock.now())).toEqual({ failures: 0, cooldownSec: 0 });
    expect(b.begin()).toBe(true);
  });

  it("slide：只终止探针，不计数不冷却", () => {
    const clock = makeClock();
    const b = new EndpointBreaker({ now: clock.now });
    b.report("quota");
    clock.tick(120_000);
    expect(b.begin()).toBe(true);
    b.slide();
    expect(b.begin()).toBe(true); // 探针已终止，可再次放行
    expect(b.snapshot(clock.now()).failures).toBe(1); // 计数不变
  });

  it("自定义 tuning 生效", () => {
    const clock = makeClock();
    const b = new EndpointBreaker({ now: clock.now, quota: { baseMs: 1000, factor: 2, capMs: 4000 } });
    b.report("quota");
    expect(b.snapshot(clock.now()).cooldownSec).toBe(1);
  });
});
