# evm-rpc-pool

Zero-dependency EVM JSON-RPC endpoint pool: sticky-priority failover, error-classified
cooldowns, half-open probes, and adaptive `eth_getLogs` range splitting.
Runs on Node ≥18, Bun, and Cloudflare Workers.

零依赖的 EVM JSON-RPC 端点池：粘性优先故障转移、错误分型冷却、半开探针、getLogs 范围自适应。
源自三个生产项目的实战实现收敛。

## Install / 安装（git 依赖）

```bash
pnpm add github:1wb/evm-rpc-pool#v0.5.0
# 或 npm i github:1wb/evm-rpc-pool#v0.5.0
```

## Usage

### 开箱即用（纯 HTTP JSON-RPC）

```ts
import { FetchRpcPool } from "evm-rpc-pool";

const pool = new FetchRpcPool(urls, {
  // urls: 顺序即优先级，宽裕端点在前（冷却恢复后自动回首选）
  timeoutMs: 20_000,
  hostCaps: { "base-rpc.publicnode.com": { topicLogs: false } },
});

const head = await pool.call<string>("eth_blockNumber", []);
const logs = await pool.getLogs({ address, topics, fromBlock: 0, toBlock: 9999 });
```

### 自定义传输（viem 等）

```ts
import { RpcPool } from "evm-rpc-pool";

const pool = new RpcPool(urls.map((url) => ({ url, client: createViemClient(url) })));
const head = await pool.call((client) => client.getBlockNumber());
const logs = await pool.callLogs(range, (client, r) => client.getLogs({ ...r }));
```

### 低阶原语（自建接入层用）

```ts
import { EndpointBreaker, classifyRpcError } from "evm-rpc-pool";
```

### 限速与计量（v0.4.0：同凭据跨池令牌桶 + 用量台账）

```ts
import { KeyRateLimiter, meteredFetch, PoolMeter, RpcPool } from "evm-rpc-pool";
import { http, createPublicClient } from "viem";

const limiter = new KeyRateLimiter({ acquireWaitMs: 15_000 });
const meter = new PoolMeter();
// 启动预登记全部 URL（稳定 host#kN 别名）；budget 相同 = 共享限额组（同 key 跨链/跨协议共用一桶）
limiter.registerEndpoints([
  { url: "https://lb.drpc.live/bsc", budget: "drpc-key1", reqPerMin: 90 },
  { url: "https://lb.drpc.live/base", budget: "drpc-key1", reqPerMin: 90 },
]);
meter.registerEndpoints(["https://lb.drpc.live/bsc", "https://lb.drpc.live/base"]);

// fetchFn = 权威扣费点（每次真实 HTTP 发送前 acquireAndWait，覆盖拆片与内部重试）；
// retryCount: 0 —— 本地节流（LocalThrottleError）不许 viem 再重试
const fetchFn = meteredFetch(meter, limiter);
const client = createPublicClient({ transport: http(url, { fetchFn, retryCount: 0 }) });
const pool = new RpcPool(entries, { rateLimiter: limiter });

// 用量摘要（窗口增量，推送成功后 confirmDelta 推进水位）
const delta = meter.snapshotDelta();
// ... 渲染/发送 ...
meter.confirmDelta();
```

- `meteredFetch` 对 429 读 `Retry-After` 调 `limiter.setPenalty`，经监听同步各池 breaker
  （对策：429 + JSON-RPC error body 时 viem 包装会丢响应头）。
- 全部候选被本地限速时抛 `PoolThrottledError`（ETA = 逐候选 max(冷却, 令牌恢复) 后取 min）；
  语义是「本地等待」而非供应商故障——调用方宜静默延期，不计失败不告警。
- 未登记 URL 一律放行（漏登记不致不可用）；`req_per_min: 0` = 不限。

### 可空方法、getLogs 过滤器与节流工具（v0.5.0）

```ts
import { BreakerRegistry, FetchRpcPool, isThrottleError, throttleRetryAfterMs } from "evm-rpc-pool";

// 可空 JSON-RPC 方法：pending 交易/回执查询合法返回 null，不再冤枉端点
// DEFAULT_NULLABLE_METHODS（查交易/回执/叔块）默认放行；eth_getBlockByNumber 有意不在白名单
const receipt = await pool.call<unknown | null>("eth_getTransactionReceipt", [hash]);
const block = await pool.call("eth_getBlockByNumber", ["0x1", false], { allowNull: true }); // 单次显式放行
const pool2 = new FetchRpcPool(urls, { nullableMethods: ["eth_getBlockByNumber"] });        // 构造级覆盖

// getLogs（GetLogsOptions）：可选单/多地址（数组 OR）、位置内嵌套 topics OR、显式 lane
await pool.getLogs({ topics: [sig], fromBlock: 0, toBlock: 999 });              // 省略 address = 全链纯 topic 过滤
await pool.getLogs({ address: ["0xa", "0xb"], topics, fromBlock, toBlock, lane: "address" });

// 统一节流识别：沿 cause 链同时识别 LocalThrottleError 与 PoolThrottledError（viem 包装不失效）
if (isThrottleError(err)) console.log("throttled, eta =", throttleRetryAfterMs(err));
```

### 限速器自省与跨池共享熔断（v0.5.0）

```ts
import { BreakerRegistry, KeyRateLimiter, RpcPool } from "evm-rpc-pool";

// KeyRateLimiter 自省与脱敏快照（监控 /metrics 用）
limiter.isRegistered(url);          // 该 URL 是否已登记
limiter.penaltyUntil(url);          // Retry-After 边带绝对截止时刻（未设罚 = 0）
limiter.endpoints();                // [{ url, alias, budget, reqPerMin }]，reqPerMin 反映同组取 min 后真实速率
limiter.snapshot();                 // 完全脱敏：零原始 URL；独享端点 budget 回退为 alias（host#kN）

// BreakerRegistry：多池共享冷却与半开探针状态（按 URL 懒创建；只读 snapshot 不脏建实例）
const registry = new BreakerRegistry();
const poolA = new RpcPool(entriesA, { breakerFor: (u) => registry.get(u) });
const poolB = new RpcPool(entriesB, { breakerFor: (u) => registry.get(u) });
registry.setPenalty(url, Date.now() + 30_000); // 未建实例也会初始化并写入
registry.snapshot(url);            // { failures, cooldownSec }；未触碰 URL 返回 { failures: 0, cooldownSec: 0 }
registry.clear();                  // 测试重入重置
```

## Semantics / 语义

- 错误分型与默认冷却：`quota`（HTTP 402/403 或额度耗尽文案）2min×4ⁿ 封顶 6h；`rate`（HTTP 429 或限速文案）30s×2ⁿ 封顶 10min；`archive` 30min×2ⁿ 封顶 6h；`transient`（网络/超时/5xx/非 JSON）30s×2ⁿ 封顶 10min；`reverted`/`rejected` 端点健康仅滑下家；`range` 端点内自适应拆分（下限 `MIN_CHUNK = 10n`，可经 `minChunk` 选项上调，v0.5.0 起适配 10～1000 块上限的现代免费节点）。
- 429 响应带 `Retry-After`（秒数或 HTTP-date）时直取为冷却截止（≥5s，边带取 max）；
  冷却统一为绝对截止时间规则，重复设置取更晚，过期自然失效。
- 全部端点冷却时抛 `PoolCoolingError`（含 `retryAfterMs`）；全部候选本地限速时抛
  `PoolThrottledError`（含 ETA，静默延期语义）；`isThrottleError` / `throttleRetryAfterMs`
  沿 `cause` 链统一识别两类节流错误（v0.5.0 导出）。
- **已知语义（v0.5.0 更新）**：回调/响应返回 `null` 默认仍视同端点故障（transient 滑下家，
  防 `eth_getBlockByNumber` 命中落后节点）；但 `DEFAULT_NULLABLE_METHODS` 内的可空方法
  （查交易/回执/叔块）默认放行 `null`，可经构造 `nullableMethods` 或单次 `{ allowNull: true }` 覆盖。
- **已知语义**：`reverted` 判定依赖错误文案（匹配 `/execution reverted/i`），不解析 revert
  data 或自定义错误选择器；文案不含该短语的执行失败会被归入其他分型，需要精确判定请走
  自定义传输（`RpcPool`）并在回调内自行识别。
- 所有错误信息与快照对 URL 脱敏（只留协议与 host），key 不落日志。
- **v0.2.0 数值能力表与分桶学习**：`EntryCaps` 支持 `topicLogRange` / `addressLogRange`
  （单次最大块跨度，`<=0` 视同不支持该类查询）；学习值按 lane（topic / address）独立分桶；
  `getLogs` 按 topics 形状自动选择 lane，`RpcPool.callLogs` 可显式传 `{ lane }`。
  snapshot 的 `maxLogRange` 为 `maxTopicRange` 的弃用别名。

## License

MIT
