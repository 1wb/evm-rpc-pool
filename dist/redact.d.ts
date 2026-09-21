/** RPC URL 常把 key 放 path/query；日志展示只留协议与 host。 */
export declare function redactUrl(value: string): string;
/** 错误消息中的已知端点 URL 与长认证片段脱敏后再落日志。 */
export declare function redactError(error: unknown, knownUrls?: readonly string[]): string;
