export declare function hostOf(url: string): string;
/** 逗号分隔 URL 列表 → 端点数组；顺序即优先级。空列表抛错，文案含 envName。 */
export declare function parseRpcUrls(raw: string, envName?: string): string[];
