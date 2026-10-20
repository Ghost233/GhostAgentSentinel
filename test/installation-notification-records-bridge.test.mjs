import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs';
import { createLazyCore } from '../chatgpt/lazy-core.mjs';
import { supportedProfile } from '../chatgpt/storage-compatibility.mjs';
import { createAdapter } from '../dsh/host.mjs';
import { req } from './dsh-native-runtime.mjs';
import { withModelSummary } from './helpers/model-summary.mjs';

async function until(predicate) { const end = Date.now() + 3000; while (!await predicate()) { assert.ok(Date.now() < end, 'expected public notification bridge state'); await delay(3); } }

test('actual MCP and Native Remote read their own produced notification records without a current chat', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-records-bridge-')), token = 'a'.repeat(64);
  let clock = Date.now(), codex, dsh, enabled = true;
  const models = createServer(async (request, response) => {
    let raw = ''; for await (const chunk of request) raw += chunk;
    const body = JSON.parse(raw);
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify(request.url === '/v1/chat/completions' ? { model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds: [], gaps: [] })) } }] }
      : { model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: id === 'request_drift' ? 'attention' : 'unknown' }])) }));
  });
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve));
  const server = await startServer({ dataDir, now: () => clock, leaseMs: 1e9 }), core = await connectCore({ dataDir }), lazy = createLazyCore({ dataDir }), ctx = new (req('@deepseek-ai/cordis').Context)();
  t.after(async () => { await codex?.close(); await dsh?.close(); await ctx.fiber.dispose(); await lazy.close(); await core.close(); await server.close(); models.closeAllConnections(); await new Promise(resolve => models.close(resolve)); await rm(dataDir, { recursive: true, force: true }); });
  const nativeSource = await core.registerSource({ registrationId: 'chatgpt:records', hostRunId: 'host', name: 'Codex', policy: 'chatgpt-main/1' });
  await core.recordCollectionOutcome({ sourceKey: nativeSource, mainId: null, lifecycleId: null, roundId: 'source-failure', channel: 'directory', outcome: 'failed', settledAt: clock, error: { code: 'EIO', stage: 'collection' } });
  codex = await startChatGPTAdapter({ core: lazy, native: {
    async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: 1, profile: supportedProfile }; },
    async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' }; },
    async readThread() { throw new Error('installation history must not read a chat'); },
  }, installationId: 'records', sessionsRoot: dataDir, dataDir: join(dataDir, 'codex'), collectMaterials: true, token, pollMs: 0 });
  const mcp = async (method, params = {}) => (await (await fetch(codex.url + '/mcp', { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })).json()).result;
  const tool = (await mcp('tools/list')).tools.find(tool => tool.name === 'sentinel_installation_notification_records');
  assert.ok(tool, 'the app-only installation records tool exists');
  assert.deepEqual(tool._meta.ui.visibility, ['app']); assert.equal(tool.annotations.readOnlyHint, true);
  const codexPage = (await mcp('tools/call', { name: tool.name, arguments: {} })).structuredContent;
  assert.equal(codexPage.records.length, 1); assert.equal(codexPage.records[0].kind, 'monitoring-failure');
  assert.equal(codexPage.records[0].chatTitle, null); assert.equal(codexPage.records[0].mainId, null);
  await ctx.plugin(req('@deepseek-ai/dsh-llm').default);
  for (const name of ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop']) await ctx.plugin(req(`@deepseek-ai/${name}`).default, { agents: [] });
  for (const name of ['dsh-typert-registry', 'dsh-api-remote-access', 'dsh-api-gateway']) await ctx.plugin(req(`@deepseek-ai/${name}`).default);
  dsh = createAdapter(ctx, { dataDir, registrationId: 'dsh:records' }, { flushMs: 10 });
  await until(() => dsh.connected);
  const sourceKey = (await dsh.snapshot()).adapter.sourceKey, baseUrl = `http://127.0.0.1:${models.address().port}`;
  await core.configureInstallationAnalysis('dsh:records', { llm: { baseUrl, model: 'summary' }, jev: { baseUrl, model: 'judge' } }, (await core.installationAnalysisConfig('dsh:records')).revision);
  const ref = eventId => ({ taskId: 'agent', lifecycleId: 'life', eventId });
  const event = (id, kind, text, data = {}) => ({ id, taskId: 'agent', taskLifecycleId: 'life', kind, text, data, capturedAt: clock, occurredAt: null });
  await core.ingest(sourceKey, { tasks: [{ id: 'agent', sessionId: 'dsh-chat', lifecycleId: 'life', title: 'Own DSH chat', status: 'running' }], events: [
    event('state', 'status', 'Running', { nativeType: 'agent/status', status: 'running' }),
    event('claim', 'input', 'Inspect only', { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: 'request' }),
    event('input', 'input', 'Inspect only', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', inputOrigin: 'user-entry', inherited: false, messageId: 'request' }),
    event('output', 'output', 'Changing other data'),
  ], hierarchyContext: { schemaVersion: 1, revision: 1000, sessions: [{ sessionId: 'dsh-chat' }], activations: [{ taskId: 'agent', lifecycleId: 'life', sessionId: 'dsh-chat', runId: null, state: 'running', stateRefs: [ref('state')] }], relations: [],
    instructionBindings: [{ taskId: 'agent', lifecycleId: 'life', inputRef: ref('input'), phase: 'consumed', origin: 'user-entry', originRefs: [ref('claim')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } } });
  await until(async () => (await core.snapshot()).tasks.find(task => task.id === 'agent').alerts.some(alert => alert.status === 'attention'));
  clock += 3000;
  const peer = new (req('@deepseek-ai/dsh-client-connection').Peer)(ctx, { principal: { kind: 'owner' }, interactions: 'none' });
  const rpc = (method, args = {}) => ctx.typertGateway.dispatchRpc(`sentinel/${method}`, { args }, undefined, peer);
  const [notice] = (await rpc('claimNotifications', { notificationClaimSelection: { submissionProtocol: 1 } })).value;
  assert.equal((await rpc('beginNotificationSubmission', { id: notice.id, attemptToken: notice.attemptToken })).value.maySubmit, true);
  await rpc('ackNotification', { id: notice.id, result: { status: 'submitted', attemptToken: notice.attemptToken } });
  const result = await rpc('installationNotificationRecords', { notificationRecordsSelection: {} });
  assert.equal(result.ok, true, JSON.stringify(result.error));
  assert.equal(result.value.records.length, 1);
  const record = result.value.records[0];
  assert.equal(record.registrationId, 'dsh:records'); assert.equal(record.mainId, 'dsh-chat'); assert.equal(record.chatTitle, 'Own DSH chat');
  assert.equal(record.kind, 'task-alert'); assert.equal(record.delivery, 'system-accepted'); assert.equal(record.native, null); assert.equal(record.humanSeen, 'unknown');
  assert.ok(!JSON.stringify(result.value).includes(nativeSource));
  const originalFetch = globalThis.fetch; let recordsRequests = 0;
  globalThis.fetch = async (input, options) => {
    if (typeof input === 'string' && input.startsWith(server.url + '/v1/notifications/installations/records')) recordsRequests++;
    const response = await originalFetch(input, options);
    if (input === server.url + '/v1/health') { const health = await response.json(); health.features = health.features.filter(feature => feature !== 'installation-notification-records/1'); return new Response(JSON.stringify(health), { status: response.status, headers: response.headers }); }
    return response;
  };
  try {
    assert.equal((await mcp('tools/call', { name: tool.name, arguments: {} })).isError, true);
    assert.equal((await rpc('installationNotificationRecords', { notificationRecordsSelection: {} })).ok, false);
    assert.equal(recordsRequests, 0, 'healthy old Core without this real capability is explicitly unsupported');
  } finally { globalThis.fetch = originalFetch; }
  let held;
  globalThis.fetch = async (input, options) => {
    const response = await originalFetch(input, options);
    if (typeof input === 'string' && input.startsWith(server.url + '/v1/notifications/installations/records')) await new Promise(resolve => { held = resolve; });
    return response;
  };
  try {
    const reading = mcp('tools/call', { name: tool.name, arguments: {} });
    await until(() => held);
    enabled = false; held();
    assert.equal((await reading).isError, true, 'a completed old read cannot disclose history after qualification changes');
  } finally { held?.(); enabled = true; globalThis.fetch = originalFetch; }
});
