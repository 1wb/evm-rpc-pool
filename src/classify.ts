export type RpcErrorKind =
  | "quota"
  | "rate"
  | "archive"
  | "transient"
  | "reverted"
  | "rejected"
  | "range";

/** 预分型错误：fetch 工厂在能确知分型（HTTP 状态、200+JSON-RPC error）时抛出，内核直接采信。 */
export class RpcKindError extends Error {
  constructor(
    readonly kind: RpcErrorKind,
    message: string,
    readonly rangeLimit: bigint | null = null,
    /** Retry-After 毫秒（调用方从响应头解析后携带）；冷却直取不进阶梯 */
    readonly retryAfterMs: number | null = null,
  ) {
    super(message);
    this.name = "RpcKindError";
  }
}

/**
 * 解析 Retry-After 头：秒数与 HTTP-date 双格式，返回距 nowMs 的毫秒。
 * 无效/负数/非有限值回退 null（调用方走分型阶梯）。
 */
export function parseRetryAfter(value: string | null, nowMs: number): number | null {
  if (value === null || value.trim() === "") return null;
  const secs = Number(value);
  if (Number.isFinite(secs)) {
    return secs > 0 ? Math.round(secs * 1000) : null;
  }
  const at = Date.parse(value);
  return Number.isFinite(at) && at > nowMs ? at - nowMs : null;
}

// v0.4.0 分型拆分：rate = 短时限流（429/限速文案，30s×2 短梯）；quota = 硬额度（402/额度耗尽，2min×4 长梯）
export const RATE_TEXT = /\b429\b|too many requests|rate[ -]?(?:limit|exceeded)|request rate/i;
// quota 正则取两父实现的并集（flower: monthly；bn-alpha: upgrade here）
export const QUOTA_TEXT =
  /\b(?:402|403)\b|usage limit|quota|credits?\s*[\s\S]*(?:limit|exhaust)|current plan|monthly|upgrade here/i;
export const ARCHIVE_TEXT = /archive requests?|archive node|historical (?:data|state)|personal token/i;
const REVERT_TEXT = /execution reverted/i;

const RANGE_LIMIT_PATTERNS = [
  /(?:0\s*-\s*)?(\d+)\s*blocks?\s*range/i,
  /up\s+to\s+(?:a\s+)?(\d+)\s*blocks?/i,
  /range\s+(?:must\s+not\s+exceed|cannot\s+exceed|limited\s+to)\s*(\d+)/i,
  /more\s+than\s+(\d+)\s+(?:results|logs)/i,
];
const RANGE_TEXT = /eth_getLogs\s+(?:block\s+)?range[\s\S]*(?:limit|exceed|too (?:large|wide))/i;
const RANGE_TEXT_2 = /(?:log query|block)\s+range[\s\S]*(?:limit|exceed|too (?:large|wide))/i;

/** 从错误文案提取 getLogs 单次范围上限；读不出返回 null（调用方对半二分）。 */
export function rangeLimitFromMessage(msg: string): bigint | null {
  for (const p of RANGE_LIMIT_PATTERNS) {
    const m = msg.match(p);
    const n = m ? Number(m[1]) : 0;
    if (m && Number.isFinite(n) && n > 0) return BigInt(n);
  }
  return null;
}

export function isRangeLimitMessage(msg: string): boolean {
  return rangeLimitFromMessage(msg) !== null || RANGE_TEXT.test(msg) || RANGE_TEXT_2.test(msg);
}

export function isContractRevertMessage(msg: string): boolean {
  return REVERT_TEXT.test(msg);
}

/**
 * 通用文本分型，序：range → reverted → quota → archive → transient。
 * "rejected"（端点健康但本次无法满足）只在 fetch 工厂能区分「200 带 JSON-RPC error」时产生，
 * 文本层面不可判，故本函数不返回它。
 */
export function classifyRpcError(error: unknown): Exclude<RpcErrorKind, "rejected"> {
  const msg = error instanceof Error ? error.message : String(error);
  if (isRangeLimitMessage(msg)) return "range";
  if (REVERT_TEXT.test(msg)) return "reverted";
  if (RATE_TEXT.test(msg)) return "rate";
  if (QUOTA_TEXT.test(msg)) return "quota";
  if (ARCHIVE_TEXT.test(msg)) return "archive";
  return "transient";
}
