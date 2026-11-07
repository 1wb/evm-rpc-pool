import { RpcPool } from "./pool.js";
import { ARCHIVE_TEXT, QUOTA_TEXT, RATE_TEXT, RpcKindError, isContractRevertMessage, isRangeLimitMessage, parseRetryAfter, rangeLimitFromMessage, } from "./classify.js";
import { isThrottleError } from "./throttle.js";
import { hostOf } from "./url.js";
/** 默认可空方法白名单：合法业务语义允许返回 null 的查询方法（spec §5.4）。
 *  注：eth_getBlockByNumber / eth_getBlockByHash 有意不在名单内——请求已知区块
 *  命中同步滞后节点返回 null 时须继续按 transient 滑向健康节点。 */
export const DEFAULT_NULLABLE_METHODS = new Set([
    "eth_getTransactionReceipt",
    "eth_getTransactionByHash",
    "eth_getTransactionByBlockHashAndIndex",
    "eth_getTransactionByBlockNumberAndIndex",
    "eth_getUncleByBlockHashAndIndex",
    "eth_getUncleByBlockNumberAndIndex",
]);
const RATE_STATUS = new Set([429]);
const QUOTA_STATUS = new Set([402, 403]);
const DEFAULT_TIMEOUT_MS = 20_000;
function timeoutSignal(ms) {
    const t = AbortSignal.timeout;
    return t ? t.call(AbortSignal, ms) : undefined;
}
/** 把 fetch / HTTP 状态 / JSON-RPC error 三层结果映射为预分型错误；rejected 只可能出自这里。
 *  allowNull：方法命中可空白名单（或单次显式放行）时，合法 `result: null` 视为成功原样返回；
 *  缺失 result 键的畸形响应（j.result === undefined）恒按 transient 滑下家（Review Focus #4）。 */
async function attemptJsonRpc(c, method, params, allowNull) {
    let res;
    try {
        res = await c.fetch(c.url, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
            signal: timeoutSignal(c.timeoutMs),
        });
    }
    catch (err) {
        // 本地节流（含包装形态）原样透传：pool 层按节流归并静默延期，不计失败不冷却
        if (isThrottleError(err))
            throw err;
        throw new RpcKindError("transient", String(err).slice(0, 120), null, null, { cause: err });
    }
    if (RATE_STATUS.has(res.status)) {
        const retryAfterMs = parseRetryAfter(res.headers?.get("retry-after") ?? null, Date.now());
        throw new RpcKindError("rate", `HTTP ${res.status}`, null, retryAfterMs);
    }
    if (QUOTA_STATUS.has(res.status))
        throw new RpcKindError("quota", `HTTP ${res.status}`);
    if (!res.ok)
        throw new RpcKindError("transient", `HTTP ${res.status}`);
    let j;
    try {
        j = (await res.json());
    }
    catch {
        throw new RpcKindError("transient", "响应非 JSON");
    }
    if (j && j.error !== undefined && j.error !== null) {
        const msg = typeof j.error === "object" && j.error !== null && "message" in j.error
            ? String(j.error.message ?? j.error)
            : String(j.error);
        const short = msg.slice(0, 120);
        // 判定序与 classifyRpcError 锁定一致：range → reverted → quota → rate → archive → rejected
        if (isRangeLimitMessage(msg))
            throw new RpcKindError("range", short, rangeLimitFromMessage(msg));
        if (isContractRevertMessage(msg))
            throw new RpcKindError("reverted", short);
        if (QUOTA_TEXT.test(msg))
            throw new RpcKindError("quota", short);
        if (RATE_TEXT.test(msg))
            throw new RpcKindError("rate", short);
        if (ARCHIVE_TEXT.test(msg))
            throw new RpcKindError("archive", short);
        throw new RpcKindError("rejected", short);
    }
    if (j && j.result !== undefined) {
        if (j.result !== null || allowNull)
            return j.result;
    }
    throw new RpcKindError("transient", "null result");
}
/** topics 形状 → lane 自动派生（spec §5.5）：存在至少一个有效过滤项（非空单值或非空嵌套数组）走 topic。 */
function deriveLane(topics) {
    return (topics ?? []).some((t) => typeof t === "string" || (Array.isArray(t) && t.length > 0)) ? "topic" : "address";
}
export class FetchRpcPool {
    pool;
    nullableMethods;
    constructor(urls, opts = {}) {
        if (!urls.length)
            throw new Error("FetchRpcPool: 端点列表为空");
        const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
        // fetch 须绑定 globalThis：CF 新运行时对裸引用的 host 函数报 Illegal invocation
        const fallback = fetch.bind(globalThis);
        const entries = urls.map((url) => ({
            url,
            client: { url, fetch: opts.fetchImpl ?? fallback, timeoutMs },
            caps: opts.hostCaps?.[hostOf(url)],
        }));
        this.nullableMethods = opts.nullableMethods !== undefined ? new Set(opts.nullableMethods) : DEFAULT_NULLABLE_METHODS;
        this.pool = new RpcPool(entries, opts);
    }
    call(method, params, opts) {
        const allowNull = opts?.allowNull ?? this.nullableMethods.has(method);
        return this.pool.call((c) => attemptJsonRpc(c, method, params, allowNull), { allowNull });
    }
    /** fromBlock/toBlock 接受 number 或 bigint（number 侧为历史消费方便利），内部统一 bigint。
     *  出站 filter 构造（spec §5.5）：address 缺省/空串/空数组时省略该键；topics 仅 undefined
     *  时省略（显式 [] 原样保留）；lane 显式优先，缺省按 topics 形状自动派生。 */
    getLogs(opts) {
        const range = { fromBlock: BigInt(opts.fromBlock), toBlock: BigInt(opts.toBlock) };
        const lane = opts.lane ?? deriveLane(opts.topics);
        return this.pool.callLogs(range, async (c, r) => {
            const filter = {
                fromBlock: "0x" + r.fromBlock.toString(16),
                toBlock: "0x" + r.toBlock.toString(16),
            };
            const hasAddress = typeof opts.address === "string" ? opts.address.length > 0 : Array.isArray(opts.address) && opts.address.length > 0;
            if (hasAddress)
                filter.address = opts.address;
            if (opts.topics !== undefined)
                filter.topics = opts.topics;
            const result = (await attemptJsonRpc(c, "eth_getLogs", [filter], false));
            return Array.isArray(result) ? result : [];
        }, { lane });
    }
    snapshot() {
        return this.pool.snapshot();
    }
}
