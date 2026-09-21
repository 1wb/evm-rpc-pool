const URL_RE = /\b(?:https?|wss?):\/\/[^\s"'<>]+/gi;
/** RPC URL 常把 key 放 path/query；日志展示只留协议与 host。 */
export function redactUrl(value) {
    try {
        const url = new URL(value);
        if (!["http:", "https:", "ws:", "wss:"].includes(url.protocol)) {
            return "<invalid-rpc-url>";
        }
        return `${url.protocol}//${url.host}/…`;
    }
    catch {
        return "<invalid-rpc-url>";
    }
}
/** 错误消息中的已知端点 URL 与长认证片段脱敏后再落日志。 */
export function redactError(error, knownUrls = []) {
    let message = error instanceof Error ? error.message : String(error);
    for (const url of knownUrls) {
        if (url === "")
            continue;
        message = message.split(url).join(redactUrl(url));
        try {
            const parsed = new URL(url);
            const candidates = [
                parsed.username,
                parsed.password,
                ...parsed.pathname.split("/"),
                ...parsed.searchParams.values(),
            ];
            for (const candidate of candidates) {
                if (candidate.length < 8)
                    continue;
                message = message.split(candidate).join("<redacted>");
            }
        }
        catch {
            // 非法 URL 已由 redactUrl 处理
        }
    }
    return message.replace(URL_RE, (u) => redactUrl(u));
}
