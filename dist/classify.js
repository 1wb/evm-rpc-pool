/** 预分型错误：fetch 工厂在能确知分型（HTTP 状态、200+JSON-RPC error）时抛出，内核直接采信。 */
export class RpcKindError extends Error {
    kind;
    rangeLimit;
    constructor(kind, message, rangeLimit = null) {
        super(message);
        this.kind = kind;
        this.rangeLimit = rangeLimit;
        this.name = "RpcKindError";
    }
}
// quota 正则取两父实现的并集（flower: monthly；bn-alpha: upgrade here）
export const QUOTA_TEXT = /\b(?:402|429)\b|too many requests|rate[ -]?(?:limit|exceeded)|request rate|usage limit|quota|credits?\s*[\s\S]*(?:limit|exhaust)|current plan|monthly|upgrade here/i;
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
export function rangeLimitFromMessage(msg) {
    for (const p of RANGE_LIMIT_PATTERNS) {
        const m = msg.match(p);
        const n = m ? Number(m[1]) : 0;
        if (m && Number.isFinite(n) && n > 0)
            return BigInt(n);
    }
    return null;
}
export function isRangeLimitMessage(msg) {
    return rangeLimitFromMessage(msg) !== null || RANGE_TEXT.test(msg) || RANGE_TEXT_2.test(msg);
}
export function isContractRevertMessage(msg) {
    return REVERT_TEXT.test(msg);
}
/**
 * 通用文本分型，序：range → reverted → quota → archive → transient。
 * "rejected"（端点健康但本次无法满足）只在 fetch 工厂能区分「200 带 JSON-RPC error」时产生，
 * 文本层面不可判，故本函数不返回它。
 */
export function classifyRpcError(error) {
    const msg = error instanceof Error ? error.message : String(error);
    if (isRangeLimitMessage(msg))
        return "range";
    if (REVERT_TEXT.test(msg))
        return "reverted";
    if (QUOTA_TEXT.test(msg))
        return "quota";
    if (ARCHIVE_TEXT.test(msg))
        return "archive";
    return "transient";
}
