import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';
import { configureInstallationModels } from './helpers/installation-model-settings.mjs';
import { withModelSummary } from './helpers/model-summary.mjs';

const DAY = 86_400_000;
async function until(predicate, label) { const deadline = performance.now() + 3000; while (!await predicate()) { assert.ok(performance.now() < deadline, label); await delay(2); } }

async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-successful-summary-')), calls = [], held = [];
  let clock = Date.now(), mode = 'success', text = '已核对本轮观察材料。', server, client;
  const models = createServer(async (request, response) => {
    if (request.method === 'GET') return response.writeHead(404, { 'content-type': 'application/json' }).end('{}');
    let raw = ''; for await (const chunk of request) raw += chunk;
    const body = JSON.parse(raw); calls.push({ path: request.url, model: body.model });
    response.setHeader('content-type', 'application/json');
    if (request.url === '/v1/chat/completions') {
      const extraction = withModelSummary(body, { selectedOptionIds: [], gaps: [] }); extraction.summary.text = text;
      const reply = () => response.end(JSON.stringify({ model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(extraction) } }] }));
      if (mode === 'hold') held.push(reply); else reply();
    } else if (mode === 'fail-jev') response.writeHead(503).end('{"error":{"code":"controlled_failure"}}');
    else response.end(JSON.stringify({ model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: 'unknown' }])) }));
  });
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve));
  const start = async () => { server = await startServer({ dataDir, now: () => clock, cooldownMs: 10, leaseMs: DAY * 3 }); client = await connectCore({ dataDir }); };
  await start();
  t.after(async () => { for (const reply of held.splice(0)) reply(); await client?.close(); await server?.close(); models.closeAllConnections(); await new Promise(resolve => models.close(resolve)); await rm(dataDir, { recursive: true, force: true }); });
  const baseUrl = 'http://127.0.0.1:' + models.address().port;
  const config = { llm: { baseUrl, model: 'history-summary' }, jev: { baseUrl, model: 'history-judge' } };
  return { config, calls, held, now: () => clock, advance: ms => { clock += ms; }, mode: value => { mode = value; }, text: value => { text = value; },
    get client() { return client; }, async save(registrationId, value = config) { return configureInstallationModels(client, registrationId, value); },
    async task(sourceKey, id = 'main') { return (await client.snapshot()).tasks.find(task => task.sourceKey === sourceKey && task.id === id); },
    async restart() { await client.close(); await server.close(); await start(); },
  };
}

function nativeBatch(at) {
  const ref = eventId => ({ taskId: 'main', lifecycleId: 'run', eventId });
  const event = (id, kind, text, offset, data) => ({ id, taskId: 'main', taskLifecycleId: 'run', kind, text, capturedAt: at + offset, occurredAt: null, data: { sourceKind: 'chatgpt-native', runId: 'run', ...data } });
  return { tasks: [{ id: 'main', sessionId: 'main', lifecycleId: 'run', title: 'Owned main', status: 'running' }], events: [
    event('start', 'status', 'Started', -1000, { nativeType: 'task_started', sourceForm: 'event_msg', turnTrigger: 'composer', status: 'running' }),
    event('request', 'input', 'Inspect only the selected log', -500, { nativeType: 'item_completed/UserMessage', sourceForm: 'event_msg/item_completed', messageId: 'request-message', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false }),
    event('output', 'output', 'Inspecting the selected log', 0, { nativeType: 'item_completed/AgentMessage', sourceForm: 'event_msg/item_completed' }),
  ], hierarchyContext: { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'main' }], relations: [], activations: [{ taskId: 'main', lifecycleId: 'run', sessionId: 'main', runId: 'run', state: 'running', stateRefs: [ref('start')] }], instructionBindings: [{ taskId: 'main', lifecycleId: 'run', inputRef: ref('request'), phase: 'consumed', origin: 'user-entry', originRefs: [ref('start'), ref('request')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } } };
}

async function observed(f, installationId = 'history') {
  const setting = await f.save('chatgpt:' + installationId);
  const source = await f.client.registerSource({ registrationId: 'chatgpt:' + installationId, hostRunId: 'host', name: 'Owned main', policy: 'chatgpt-main/1' });
  await f.client.ingest(source, nativeBatch(f.now()));
  await until(async () => (await f.task(source)).analysis.summary?.generated, 'actual summary and judgment both complete');
  return { source, setting };
}

test('a successful generated summary remains reachable only as history after the same execution later fails', async t => {
  const f = await fixture(t), { source, setting } = await observed(f), first = (await f.task(source)).analysis;
  const expected = { generated: first.summary.generated, completedAt: first.completedAt, configScope: { registrationId: 'chatgpt:history', revision: setting.revision }, expiresAt: f.now() - 1000 + DAY };
  assert.deepEqual(first.lastSuccessfulSummary, expected);
  f.mode('fail-jev'); f.text('本轮总结尚未通过判断。'); f.advance(10);
  await f.client.ingest(source, { events: [{ ...nativeBatch(f.now()).events[2], id: 'later-output', text: 'Inspecting the next observed entry' }] });
  await until(async () => (await f.task(source)).analysis.state === 'failed', 'the later real judgment request fails');
  const failed = (await f.task(source)).analysis;
  assert.equal(failed.resultDisposition, 'invalid');
  assert.deepEqual(failed.lastSuccessfulSummary, expected);
  assert.deepEqual(f.calls.map(call => call.path), ['/v1/chat/completions', '/v1/systemone', '/v1/chat/completions', '/v1/systemone']);
  await f.restart();
  assert.deepEqual((await f.task(source)).analysis.lastSuccessfulSummary, expected, 'the single historical success is durable');
});

test('the historical summary expires with the earliest round material even when that material was not cited', async t => {
  const f = await fixture(t), at = f.now(), { source } = await observed(f);
  const first = (await f.task(source)).analysis.lastSuccessfulSummary;
  assert.deepEqual(first.generated.evidenceIds, ['output']);
  assert.equal(first.expiresAt, at - 1000 + DAY, 'the uncited start remains part of the successful round input');
  const monitoring = await f.client.installationMonitoring('chatgpt:history');
  await f.client.setInstallationMonitoring('chatgpt:history', true, monitoring.revision);
  f.advance(DAY - 1001);
  assert.deepEqual((await f.task(source)).analysis.lastSuccessfulSummary, first);
  assert.equal((await f.client.diagnostics()).retention.nextExpiryAt, first.expiresAt);
  f.advance(1);
  assert.equal((await f.task(source)).analysis.lastSuccessfulSummary, undefined);
  await f.restart();
  assert.equal((await f.task(source)).analysis.lastSuccessfulSummary, undefined, 'expired historical text is removed durably');
});

test('reuse and unconfigured local checks preserve the old history until a new real successful summary replaces it', async t => {
  const f = await fixture(t), { source } = await observed(f), first = (await f.task(source)).analysis.lastSuccessfulSummary;
  f.advance(10);
  const reused = (await f.task(source)).analysis;
  assert.equal(reused.resultDisposition, 'reused');
  assert.deepEqual(reused.lastSuccessfulSummary, first);
  assert.equal(f.calls.length, 2);
  const empty = await f.save('chatgpt:history', {});
  await until(async () => (await f.task(source)).analysis.coverage?.dependencies?.length === 2, 'local fact checks finish with both models unconfigured');
  const local = (await f.task(source)).analysis;
  assert.equal(local.summary.generated, undefined);
  assert.deepEqual(local.lastSuccessfulSummary, first);
  assert.notEqual(local.currentConfigScope.revision, first.configScope.revision);
  assert.equal(local.currentConfigScope.revision, empty.revision);
  assert.equal(f.calls.length, 2, 'unconfigured local checks do not call either model');
  f.text('已核对新的本轮总结。');
  const next = await f.save('chatgpt:history'); f.advance(10);
  await until(async () => (await f.task(source)).analysis.lastSuccessfulSummary?.generated.text === '已核对新的本轮总结。', 'a new actual two-model success replaces the single historical success');
  const replaced = (await f.task(source)).analysis.lastSuccessfulSummary;
  assert.equal(replaced.completedAt, f.now());
  assert.equal(replaced.configScope.revision, next.revision);
  assert.equal(replaced.expiresAt, first.expiresAt, 'new completion cannot extend the retained input material deadline');
  assert.deepEqual(f.calls.map(call => call.path), ['/v1/chat/completions', '/v1/systemone', '/v1/chat/completions', '/v1/systemone']);
});

test('cancelling a later actual model request cannot replace or retime the successful historical summary', async t => {
  const f = await fixture(t), { source } = await observed(f), first = (await f.task(source)).analysis.lastSuccessfulSummary;
  f.mode('hold'); f.text('这份尚未完成判断的总结不可发布。'); f.advance(10);
  await f.client.ingest(source, { events: [{ ...nativeBatch(f.now()).events[2], id: 'held-output', text: 'Inspecting another entry' }] });
  await until(() => f.held.length === 1, 'the later real summary request is held');
  await f.save('chatgpt:history', {});
  assert.deepEqual((await f.task(source)).analysis.lastSuccessfulSummary, first);
  f.mode('success'); f.held.shift()();
  await until(async () => (await f.client.diagnostics()).runtime.unsettledAnalysisCalls === 0, 'the cancelled real request settles');
  assert.deepEqual((await f.task(source)).analysis.lastSuccessfulSummary, first);
  assert.equal(f.calls.filter(call => call.path === '/v1/systemone').length, 1, 'the sealed later round never proceeds to judgment');
});

test('uninstall removes only the owned execution historical summary while the other installation remains readable', async t => {
  const f = await fixture(t), owned = await observed(f), other = await observed(f, 'other-history');
  const kept = (await f.task(other.source)).analysis.lastSuccessfulSummary;
  const coreId = (await f.client.health()).instanceId;
  await f.client.unregisterSource(owned.source);
  assert.equal((await f.client.purgeChatGPTInstallation({ installationId: 'history' })).status, 'complete');
  assert.equal(await f.task(owned.source), undefined);
  assert.deepEqual((await f.task(other.source)).analysis.lastSuccessfulSummary, kept);
  assert.equal((await f.client.health()).instanceId, coreId);
});

test('verified inherited material sets the historical summary deadline even while all own execution material remains retained', async t => {
  const f = await fixture(t), at = f.now();
  await f.save('dsh:history');
  const source = await f.client.registerSource({ registrationId: 'dsh:history', hostRunId: 'host', name: 'Owned inherited scope' });
  const ref = (taskId, eventId) => ({ taskId, lifecycleId: 'run', eventId });
  const event = (taskId, id, kind, text, capturedAt, data = {}) => ({ taskId, id, taskLifecycleId: 'run', kind, text, capturedAt, occurredAt: null, data });
  await f.client.ingest(source, { tasks: [
    { id: 'parent', sessionId: 'parent', lifecycleId: 'run', title: 'Parent', status: 'idle' },
    { id: 'main', sessionId: 'main', lifecycleId: 'run', title: 'Child', status: 'running' },
  ], events: [
    event('parent', 'parent-state', 'status', '', at - 2000, { nativeType: 'agent/status', status: 'idle' }),
    event('parent', 'catalog', 'status', '', at - 2000, { nativeType: 'subagent/catalog', childSessionId: 'main' }),
    event('parent', 'claim', 'input', 'Inspect only; preserve the original data.', at - 2000, { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: 'parent-message' }),
    event('parent', 'parent-input', 'input', 'Inspect only; preserve the original data.', at - 2000, { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', inputOrigin: 'unknown', inherited: false, messageId: 'parent-message' }),
    event('main', 'state', 'status', '', at, { nativeType: 'agent/status', status: 'running' }),
    event('main', 'child-input', 'input', 'Inspect the selected log.', at, { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', inputOrigin: 'delegated', inherited: false, messageId: 'child-message' }),
    event('main', 'child-output', 'output', 'Inspecting the selected log.', at),
  ], hierarchyContext: { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'parent' }, { sessionId: 'main' }], activations: [
    { taskId: 'parent', lifecycleId: 'run', sessionId: 'parent', runId: null, state: 'ended', stateRefs: [ref('parent', 'parent-state')] },
    { taskId: 'main', lifecycleId: 'run', sessionId: 'main', runId: null, state: 'running', stateRefs: [ref('main', 'state')] },
  ], relations: [{ parentSessionId: 'parent', childSessionId: 'main', kind: 'delegation', evidenceRefs: [ref('parent', 'catalog')] }], instructionBindings: [
    { taskId: 'parent', lifecycleId: 'run', inputRef: ref('parent', 'parent-input'), phase: 'consumed', origin: 'user-entry', originRefs: [ref('parent', 'claim')], ancestorLimitRefs: [] },
    { taskId: 'main', lifecycleId: 'run', inputRef: ref('main', 'child-input'), phase: 'consumed', origin: 'delegated', originRefs: [ref('parent', 'catalog')], ancestorLimitRefs: [ref('parent', 'parent-input')] },
  ], coverage: { complete: true, branchGaps: [] } } });
  await until(async () => (await f.task(source)).analysis.lastSuccessfulSummary, 'the real two-model child round completes');
  const first = (await f.task(source)).analysis.lastSuccessfulSummary;
  assert.equal(first.expiresAt, at - 2000 + DAY);
  const monitoring = await f.client.installationMonitoring('dsh:history');
  await f.client.setInstallationMonitoring('dsh:history', true, monitoring.revision);
  f.advance(DAY - 2000);
  const expired = await f.task(source);
  assert.equal(expired.analysis.lastSuccessfulSummary, undefined);
  assert.ok(expired.events.some(event => event.id === 'child-output'), 'own evidence outlives the inherited history deadline');
});
