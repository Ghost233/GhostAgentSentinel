import { createHash, randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { AnalysisError } from './index.mjs';
import { inferenceEndpoint } from './http.mjs';
import { inferenceFailureInfo } from './failure-info.mjs';

const PROTOCOL = 'gmd-prefill/1';
const LIMIT = 262_144;
const MODES = { chat_max_tokens_zero: ['/v1/chat/completions', 'llm'], systemone_state: ['/v1/systemone', 'decision'] };
const hash = value => createHash('sha256').update(value).digest('hex');
const integer = value => Number.isSafeInteger(value) && value >= 0;
const generationId = value => typeof value === 'string' && /^[A-Za-z0-9._~-]{1,128}$/.test(value);
const fail = (code, message, details) => new AnalysisError(code, 'llm', message, details);

export function gmdOperationNonce(tokenHex) {
  if (typeof tokenHex !== 'string' || !/^[a-f0-9]{64}$/.test(tokenHex)) throw fail('configuration', 'GMD 操作凭证必须为 32 字节的小写十六进制值');
  return createHash('sha256').update('gmd-prefill/1:operation-token', 'utf8').update(Buffer.from([0]))
    .update(Buffer.from(tokenHex, 'hex')).digest().subarray(0, 16).toString('hex');
}

async function requestJSON(config, path, { method = 'GET', body, headers, signal, timeoutMs = 5000, stage = 'llm' } = {}) {
  const deadline = new AbortController();
  const timer = setTimeout(() => deadline.abort(new AnalysisError('deadline', stage, 'GMD 请求超过客户端期限')), timeoutMs);
  try {
    const response = await fetch(inferenceEndpoint(config, path), { method, redirect: 'error', signal: signal ? AbortSignal.any([signal, deadline.signal]) : deadline.signal,
      headers: { ...headers, ...(body === undefined ? {} : { 'content-type': 'application/json', 'content-length': String(Buffer.byteLength(body)) }) }, ...(body === undefined ? {} : { body }) });
    if (response.headers.get('content-type')?.includes('text/event-stream')) {
      await response.body?.cancel();
      throw fail('unexpected_stream', 'GMD 非流式操作收到 SSE');
    }
    const chunks = []; let bytes = 0;
    for await (const chunk of response.body) {
      bytes += chunk.length;
      if (bytes > LIMIT) { await response.body?.cancel().catch(() => {}); throw fail('gmd_response_limit', 'GMD 操作响应超过字节预算'); }
      chunks.push(chunk);
    }
    let payload;
    try { payload = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { throw fail('invalid_json', 'GMD 操作响应不是完整 JSON'); }
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw fail('invalid_response', 'GMD 操作响应结构无效');
    return { status: response.status, payload };
  } catch (error) {
    signal?.throwIfAborted();
    if (deadline.signal.aborted) throw deadline.signal.reason;
    if (error instanceof AnalysisError) { if (error.stage === 'llm') error.stage = stage; throw error; }
    throw new AnalysisError('network', stage, 'GMD 操作连接中断，原生状态未确认', inferenceFailureInfo(error, 'fetch'));
  } finally { clearTimeout(timer); }
}

export async function discoverGMDPrefill(config, { mode = 'chat_max_tokens_zero', signal } = {}) {
  if (!MODES[mode]) throw fail('configuration', '未知的 GMD 预填模式');
  const unsupported = reason => ({ supported: false, reason });
  if (!config || typeof config.baseUrl !== 'string' || !config.baseUrl.trim() || typeof config.model !== 'string' || !config.model.trim()) return unsupported('unconfigured');
  let response;
  try { response = await requestJSON(config, '/api/v1/models', { signal }); }
  catch { signal?.throwIfAborted(); return unsupported('unavailable'); }
  if (response.status !== 200) return unsupported('undeclared');
  const directory = response.payload;
  if (directory.source !== 'ghost-model-deck' || !Array.isArray(directory.models)) return unsupported('undeclared');
  const candidates = directory.models.slice(0, 512).filter(row => row?.key === config.model && row.type === MODES[mode][1] && row.source === 'ghost-model-deck'
    && Array.isArray(row.loaded_instances) && row.loaded_instances.some(instance => instance?.id === config.model));
  if (candidates.length !== 1) return unsupported('unconfirmed');
  const capability = candidates[0].capabilities?.prefill;
  if (!capability) return unsupported('undeclared');
  if (capability.supported === false) return unsupported('not-supported');
  const policy = capability.operation_policy;
  if (capability.protocol !== PROTOCOL || capability.supported !== true || capability.mode !== mode
    || !generationId(capability.runtime_generation)
    || policy?.id_format !== 'gmd1' || !integer(policy.time_basis_ms) || !integer(policy.past_window_ms) || !policy.past_window_ms
    || !integer(policy.future_tolerance_ms)) return unsupported('unconfirmed');
  return { protocol: PROTOCOL, supported: true, mode, runtime_generation: capability.runtime_generation,
    operation_policy: { id_format: policy.id_format, time_basis_ms: policy.time_basis_ms, past_window_ms: policy.past_window_ms, future_tolerance_ms: policy.future_tolerance_ms }, observedAt: performance.now() };
}

export function createGMDOperation(config, { mode = 'chat_max_tokens_zero', capability, body }) {
  config = { baseUrl: config.baseUrl, model: config.model };
  capability = capability && { ...capability, operation_policy: { ...capability.operation_policy } };
  const stage = mode === 'systemone_state' ? 'jev' : 'llm';
  const fail = (code, message, details) => new AnalysisError(code, stage, message, details);
  if (capability?.supported !== true || capability.protocol !== PROTOCOL || capability.mode !== mode || !generationId(capability.runtime_generation) || !MODES[mode]) throw fail('prefill_unsupported', 'GMD 尚未明确支持所选预填模式');
  inferenceEndpoint(config, MODES[mode][0]);
  const current = performance.now(), elapsed = current - capability.observedAt;
  const issuedAt = capability.operation_policy?.time_basis_ms + Math.floor(elapsed);
  if (!integer(issuedAt) || !Number.isFinite(elapsed) || elapsed < 0 || elapsed > capability.operation_policy.past_window_ms) throw fail('gmd_clock_expired', 'GMD 时间快照已过期，请重新发现能力');
  if (!body || body.model !== config.model || body.stream !== false || typeof body.cache_salt !== 'string' || !body.cache_salt || body.cache_salt.length > 2000) throw fail('configuration', 'GMD 操作缺少固定模型、非流式参数或缓存隔离');
  const prefill = mode === 'chat_max_tokens_zero' ? body.max_tokens === 0 : body.prefill_only === true;
  const phase = prefill ? 'prefilling' : mode === 'chat_max_tokens_zero' ? 'generating' : 'judging';
  const questionsValid = body.questions && typeof body.questions === 'object' && !Array.isArray(body.questions);
  if (mode === 'chat_max_tokens_zero' && (!integer(body.max_tokens) || Object.hasOwn(body, 'max_completion_tokens') || !Array.isArray(body.messages) || !body.messages.length)
    || mode === 'systemone_state' && (!Object.hasOwn(body, 'state') || prefill && body.questions !== undefined && (!questionsValid || Object.keys(body.questions).length)
      || !prefill && (!questionsValid || !Object.keys(body.questions).length))) throw fail('configuration', 'GMD 预填参数组合无效');
  const raw = JSON.stringify(body);
  if (Buffer.byteLength(raw) > LIMIT) throw fail('configuration', 'GMD 操作材料超过字节预算');
  const token = randomBytes(32).toString('hex');
  const id = `gmd1.${hash(capability.runtime_generation)}.${issuedAt}.${gmdOperationNonce(token)}`;
  const headers = { 'idempotency-key': id, 'x-gmd-operation-token': token, 'x-gmd-runtime-generation': capability.runtime_generation };
  const saltHash = hash(body.cache_salt), path = MODES[mode][0], statusPath = '/api/v1/inference/requests/' + id;
  let last = { request_id: id, model: config.model, runtime_generation: capability.runtime_generation, cache_salt_sha256: saltHash, state: 'created', terminal: false, lease_released: false, children: [] };
  let executing = false, posted = false, cancelError, communicationFailure;
  const rememberFailure = error => {
    if (!communicationFailure && error instanceof AnalysisError) communicationFailure = { code: error.code, stage: error.stage,
      ...(error.details?.status ? { status: error.details.status } : {}), ...(error.details?.remoteCode ? { remoteCode: error.details.remoteCode } : {}) };
  };

  function validateView(value, model = value?.model, completedResponse = false) {
    if (value?.state === 'rejected' && !completedResponse) {
      if (value.request_id !== id || value.runtime_generation !== capability.runtime_generation || value.protocol !== undefined && value.protocol !== PROTOCOL) throw fail('gmd_binding_mismatch', '未提交证明不属于本次固定操作');
      if (value.terminal !== true || value.lease_released !== true || value.native_dispatch !== 'not_started'
        || !['bound', 'identity_only'].includes(value.proof_scope) || value.error?.stage !== 'admission'
        || typeof value.error?.code !== 'string' || !/^[a-z0-9_-]{1,80}$/.test(value.error.code)
        || value.children !== undefined && (!Array.isArray(value.children) || value.children.length)) throw fail('gmd_invalid_evidence', 'GMD 未提供完整的未提交证明');
      if (value.proof_scope === 'bound' ? model !== config.model || value.cache_salt_sha256 !== saltHash
        : Object.hasOwn(value, 'model') || Object.hasOwn(value, 'cache_salt_sha256')) throw fail('gmd_binding_mismatch', '未提交证明的绑定范围不一致');
      return { request_id: id, state: 'rejected', terminal: true, lease_released: true, runtime_generation: capability.runtime_generation,
        native_dispatch: 'not_started', proof_scope: value.proof_scope,
        ...(value.proof_scope === 'bound' ? { model, cache_salt_sha256: saltHash } : {}), error: { code: value.error.code, stage: 'admission' } };
    }
    if (value?.request_id !== id || model !== config.model || value.runtime_generation !== capability.runtime_generation || value.cache_salt_sha256 !== saltHash
      || value.protocol !== undefined && value.protocol !== PROTOCOL) throw fail('gmd_binding_mismatch', 'GMD 回执不属于本次固定操作');
    const pendingAssociation = value.children === undefined && value.terminal === false;
    if (typeof value.terminal !== 'boolean' || typeof value.lease_released !== 'boolean' || value.lease_released && !value.terminal || value.terminal && value.state === 'unknown'
      || !pendingAssociation && (!Array.isArray(value.children) || value.children.length > 256)) throw fail('gmd_invalid_evidence', 'GMD 操作缺少可信终态字段');
    const children = pendingAssociation ? undefined : value.children.map(child => {
      if (!child || typeof child !== 'object' || Array.isArray(child)) throw fail('gmd_invalid_evidence', 'GMD 原生回执结构无效');
      const result = {};
      for (const key of ['native_request_id', 'runtime_generation', 'prompt_tokens', 'completion_tokens', 'matched_prompt_tokens', 'deduplicated_tokens']) {
        if (child[key] === undefined) continue;
        if (!integer(child[key])) throw fail('gmd_invalid_evidence', 'GMD 原生计数字段无效');
        result[key] = child[key];
      }
      if (typeof child.lease_released !== 'boolean' || value.lease_released && !child.lease_released) throw fail('gmd_invalid_evidence', 'GMD 原生释放证据不完整');
      if (child.matched_prompt_tokens !== undefined && child.prompt_tokens !== undefined && child.matched_prompt_tokens > child.prompt_tokens) throw fail('gmd_invalid_evidence', 'GMD 前缀命中超过原生输入范围');
      if (typeof child.state !== 'string' || !/^[a-z][a-z0-9_-]{0,63}$/.test(child.state)) throw fail('gmd_invalid_evidence', 'GMD 原生结束原因未确认');
      result.state = child.state;
      result.lease_released = child.lease_released;
      if (child.question_id === null || typeof child.question_id === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(child.question_id)) result.question_id = child.question_id;
      return result;
    });
    const state = value.state ?? (completedResponse ? 'completed' : null);
    if (!['queued', 'running', 'prefilling', 'generating', 'judging', 'cancel_requested', 'completed', 'cancelled', 'failed', 'unknown'].includes(state)) throw fail('gmd_invalid_evidence', 'GMD 操作状态未确认');
    const error = typeof value.error?.code === 'string' && /^[a-z0-9_-]{1,80}$/.test(value.error.code)
      && typeof value.error.stage === 'string' && /^[a-z0-9_-]{1,64}$/.test(value.error.stage) ? { code: value.error.code, stage: value.error.stage } : null;
    return { request_id: id, model, runtime_generation: capability.runtime_generation, cache_salt_sha256: saltHash, state, terminal: value.terminal, lease_released: value.lease_released, ...(children ? { children } : {}), ...(error ? { error } : {}) };
  }
  function validateResult(payload) {
    const view = validateView(prefill ? payload.prefill : payload.cache, payload.model, true);
    if (view.state !== 'completed' || !view.terminal || !view.lease_released) throw fail('gmd_invalid_evidence', 'GMD 未确认操作完整结束与安全释放');
    if (prefill) {
      if (payload.prefill?.protocol !== PROTOCOL || payload.prefill.generated_tokens !== 0 || !view.children.length
        || view.children.some(child => !integer(child.native_request_id) || !child.runtime_generation || !child.prompt_tokens || child.completion_tokens !== 0 || child.state !== 'length')
        || mode === 'chat_max_tokens_zero' && (!Array.isArray(payload.choices) || payload.choices.length)
        || mode === 'systemone_state' && (payload.object !== 'systemone.prefill' || payload.answers !== undefined)) throw fail('gmd_invalid_evidence', '零输出响应没有真实原生预填证据');
    } else if (mode === 'chat_max_tokens_zero') {
      const choice = payload.choices?.[0];
      if (choice?.finish_reason !== 'stop' || typeof choice.message?.content !== 'string' || !choice.message.content.trim()) throw fail('incomplete_response', '正常生成未完整结束或内容为空');
      if (!view.children.some(child => integer(child.native_request_id) && child.runtime_generation > 0 && child.completion_tokens > 0)) throw fail('gmd_invalid_evidence', '正常文本生成缺少原生调用证据');
    } else if (!payload.answers || typeof payload.answers !== 'object' || Array.isArray(payload.answers)) throw fail('invalid_response', '正常 JEV 判断缺少答案对象');
    if (!(last.terminal && last.lease_released)) last = view;
    return payload;
  }
  const remoteError = (status, payload) => fail('gmd_http', `GMD 操作请求未成功（HTTP ${status}）`, { status,
    remoteCode: typeof payload?.error?.code === 'string' && /^[a-z0-9_-]{1,80}$/.test(payload.error.code) ? payload.error.code : null });
  const notAdmitted = status => fail('gmd_not_admitted', `GMD 已确认本次操作未进入原生执行（${last.error.code}）`, { ...(status ? { status } : {}), remoteCode: last.error.code });
  async function query({ signal } = {}) {
    if (last.terminal && last.lease_released) return structuredClone(last);
    const response = await requestJSON(config, statusPath, { headers, signal, stage });
    if (response.status !== 200) throw remoteError(response.status, response.payload);
    const next = validateView(response.payload);
    if (!(last.terminal && last.lease_released)) last = next;
    return structuredClone(last);
  }
  async function cancel({ signal } = {}) {
    if (last.terminal && last.lease_released) return structuredClone(last);
    cancelError ??= fail('cancelled', '本次 GMD 操作已封存，正在确认原生释放');
    if (!posted) return structuredClone(last);
    const response = await requestJSON(config, statusPath + '/cancel', { method: 'POST', body: '{}', headers, signal, stage });
    if (response.status !== 200 && response.status !== 202) throw remoteError(response.status, response.payload);
    const next = validateView(response.payload);
    if (!(last.terminal && last.lease_released)) last = next;
    return structuredClone(last);
  }

  return {
    id,
    snapshot() { return structuredClone({ ...last, ...(communicationFailure ? { communicationFailure } : {}) }); },
    query, cancel,
    async execute({ signal, transportSignal, lifetimeSignal, timeoutMs = 120_000, onState } = {}) {
      if (executing || posted) throw fail('gmd_operation_reentry', '已有操作只能查询或恢复，不能作为新请求执行');
      if (cancelError) throw cancelError;
      signal?.throwIfAborted(); transportSignal?.throwIfAborted(); lifetimeSignal?.throwIfAborted();
      executing = true;
      const initial = new AbortController(), control = new AbortController();
      const controlSignal = lifetimeSignal ? AbortSignal.any([control.signal, lifetimeSignal]) : control.signal;
      let cancelReason, cancellation, result, originalError, reported, callbackError;
      function report(state) {
        if (state === reported) return;
        reported = state;
        try { onState?.(state); } catch (error) { callbackError ??= error; }
      }
      function requestCancellation(reason) {
        if (cancelReason) return;
        cancelReason = reason ?? fail('cancelled', 'GMD 操作已取消');
        initial.abort(cancelReason);
        report('cancel_requested');
        cancellation = cancel({ signal: controlSignal }).catch(() => { if (!(last.terminal && last.lease_released)) report('unknown'); });
      }
      const onAbort = () => requestCancellation(signal?.aborted ? signal.reason : transportSignal?.reason);
      signal?.addEventListener('abort', onAbort, { once: true });
      transportSignal?.addEventListener('abort', onAbort, { once: true });
      const timer = setTimeout(() => requestCancellation(fail('deadline', 'GMD 操作超过分析期限，正在确认原生释放')), timeoutMs);
      const submit = async () => {
        const response = await requestJSON(config, path, { method: 'POST', body: raw, headers, signal: lifetimeSignal ? AbortSignal.any([initial.signal, lifetimeSignal]) : initial.signal, timeoutMs, stage });
        if (response.status === 202) { last = validateView(response.payload); return null; }
        if ([400, 409, 503].includes(response.status) && response.payload.state === 'rejected' && !(last.terminal && last.lease_released)) {
          last = validateView(response.payload);
          throw notAdmitted(response.status);
        }
        if (response.status !== 200) throw remoteError(response.status, response.payload);
        return validateResult(response.payload);
      };
      try {
        posted = true;
        report(phase);
        try { result = await submit(); }
        catch (error) {
          lifetimeSignal?.throwIfAborted();
          // A transport error may have lost a completed response. It does not
          // authorize another operation; first recover this operation's state.
          if (!cancelReason) {
            if (error.code !== 'gmd_not_admitted') rememberFailure(error);
            if (!['network', 'deadline', 'invalid_json'].includes(error.code) && !(error.code === 'gmd_http' && error.details?.status >= 500)) originalError = error;
          }
          if (!(last.terminal && last.lease_released)) report('unknown');
        }
        let backoff = 100;
        while (!(last.terminal && last.lease_released)) {
          lifetimeSignal?.throwIfAborted();
          try { await query({ signal: controlSignal }); if (last.state !== 'rejected') report(last.state === 'unknown' ? 'unknown' : cancelReason ? 'cancel_requested' : phase); }
          catch (error) {
            lifetimeSignal?.throwIfAborted();
            if (!(last.terminal && last.lease_released)) {
              if (!cancelReason) rememberFailure(error);
              last = { ...last, state: 'unknown', terminal: false, lease_released: false };
              report('unknown');
            }
          }
          if (last.terminal && last.lease_released) break;
          await delay(backoff, undefined, { signal: controlSignal });
          backoff = Math.min(5000, backoff * 2);
        }
        if (cancelReason || cancelError) throw cancelReason ?? cancelError;
        if (callbackError) throw callbackError;
        if (originalError) throw originalError;
        if (last.state === 'rejected') throw notAdmitted();
        if (last.state !== 'completed') throw fail('gmd_operation_failed', '原操作已结束，但没有可发布的成功结果' + (last.error?.code ? `（${last.error.code}）` : ''), {
          ...(communicationFailure?.status ? { status: communicationFailure.status } : {}), remoteCode: last.error?.code ?? communicationFailure?.remoteCode ?? null,
          ...(last.error?.stage ? { remoteStage: last.error.stage } : {}),
        });
        if (!result) {
          result = await submit(); // Same ID, credential and frozen bytes: replay only.
          if (!result) throw fail('gmd_invalid_evidence', '已确认终态的操作未能重放原结果');
        }
        return result;
      } catch (error) {
        lifetimeSignal?.throwIfAborted();
        throw error;
      } finally {
        clearTimeout(timer);
        signal?.removeEventListener('abort', onAbort);
        transportSignal?.removeEventListener('abort', onAbort);
        control.abort();
        if (cancellation) await cancellation;
        executing = false;
      }
    },
  };
}
