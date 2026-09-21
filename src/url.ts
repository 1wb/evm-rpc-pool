export function hostOf(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return url;
  }
}

/** 逗号分隔 URL 列表 → 端点数组；顺序即优先级。空列表抛错，文案含 envName。 */
export function parseRpcUrls(raw: string, envName = "RPC_URLS"): string[] {
  const urls = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (!urls.length) throw new Error(`${envName} 未配置`);
  return urls;
}
