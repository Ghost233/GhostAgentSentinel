import { AnalysisError } from './index.mjs';
import { inferenceFailureInfo } from './failure-info.mjs';

export function inferenceEndpoint(config, path) {
  let url;
  try { url = new URL(config.baseUrl); } catch { throw new AnalysisError('configuration', 'configuration', '推理地址无效'); }
  if (!['http:', 'https:'].includes(url.protocol) || !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname) || url.username || url.password || url.search || url.hash || !['', '/'].includes(url.pathname)) {
    throw new AnalysisError('configuration', 'configuration', '推理服务必须使用无凭据的本机回环地址');
  }
  return new URL(path, url).href;
}

export async function requestInference(config, path, body, { signal, transportSignal, timeoutMs, stage }) {
  signal?.throwIfAborted();
  const endpoint = inferenceEndpoint(config, path);
  const deadline = new AbortController();
  const timeout = setTimeout(() => deadline.abort(), timeoutMs);
  const cancellation = transportSignal ?? signal;
  const combined = cancellation ? AbortSignal.any([cancellation, deadline.signal]) : deadline.signal;
  let operation = 'fetch';
  try {
    const response = await fetch(endpoint, {
      method: 'POST', redirect: 'error', signal: combined,
      headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
    });
    operation = 'response-body';
    if (response.headers.get('content-type')?.includes('text/event-stream')) {
      await response.body?.cancel();
      throw new AnalysisError('unexpected_stream', stage, '非流式分析收到 SSE，未作为完成结果');
    }
    const text = await response.text();
    signal?.throwIfAborted();
    if (deadline.signal.aborted) throw new AnalysisError('deadline', stage, '本轮推理超过客户端期限');
    let payload;
    try { payload = JSON.parse(text); } catch { throw new AnalysisError('invalid_json', stage, '推理响应不是完整 JSON'); }
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw new AnalysisError('invalid_response', stage, '推理响应结构无效');
    if (!response.ok || payload.error) throw new AnalysisError('http_error', stage, `推理服务拒绝或失败（HTTP ${response.status}）`, { status: response.status, remoteCode: payload.error?.code ?? payload.error?.type ?? null });
    if (payload.model !== config.model) throw new AnalysisError('invalid_model', stage, '推理响应模型与本轮选择不一致');
    return payload;
  } catch (error) {
    if (signal?.aborted) throw signal.reason;
    if (deadline.signal.aborted) throw new AnalysisError('deadline', stage, '本轮推理超过客户端期限');
    if (error instanceof AnalysisError) throw error;
    throw new AnalysisError('network', stage, '无法连接本机推理服务', inferenceFailureInfo(error, operation));
  } finally {
    clearTimeout(timeout);
  }
}
