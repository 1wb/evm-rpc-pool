# evm-rpc-pool

Zero-dependency EVM JSON-RPC endpoint pool: sticky-priority failover, error-classified
cooldowns, half-open probes, and adaptive `eth_getLogs` range splitting.
Runs on Node ≥18, Bun, and Cloudflare Workers.

零依赖的 EVM JSON-RPC 端点池：粘性优先故障转移、错误分型冷却、半开探针、getLogs 范围自适应。
源自三个生产项目的实战实现收敛。

## Install / 安装（git 依赖）

```bash
pnpm add github:1wb/evm-rpc-pool#v0.4.0
# 或 npm i github:1wb/evm-rpc-pool#v0.4.0
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

## Semantics / 语义

- 错误分型与默认冷却：`quota`（HTTP 402/403 或额度耗尽文案）2min×4ⁿ 封顶 6h；`rate`（HTTP 429 或限速文案）30s×2ⁿ 封顶 10min；`archive` 30min×2ⁿ 封顶 6h；`transient`（网络/超时/5xx/非 JSON）30s×2ⁿ 封顶 10min；`reverted`/`rejected` 端点健康仅滑下家；`range` 端点内自适应拆分（下限 2000 块）。
- 429 响应带 `Retry-After`（秒数或 HTTP-date）时直取为冷却截止（≥5s，边带取 max）；
  冷却统一为绝对截止时间规则，重复设置取更晚，过期自然失效。
- 全部端点冷却时抛 `PoolCoolingError`（含 `retryAfterMs`）；全部候选本地限速时抛
  `PoolThrottledError`（含 ETA，静默延期语义）。
- **已知语义**：回调/响应返回 `null` 视同端点故障（transient）。对合法返回 `null` 的方法
  （如 `eth_getBlockByNumber` 查不存在块）请自行包 sentinel，否则会冤枉健康端点。
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
