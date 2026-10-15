import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createCore } from '../src/core/index.mjs';
import { createAnalyzer, CHECK_IDS, AnalysisError } from '../src/analysis/index.mjs';

const result = () => ({ summary: { currentTask: 'Observe', currentState: 'Unknown', recentProgress: '', attention: '' }, checks: CHECK_IDS.map(id => ({ id, status: 'unknown', judgmentState: 'insufficient', evidenceIds: [], reason: 'insufficient' })), coverage: { dependencies: [] } });
async function fixture(t, analyzer, cooldownMs = 0) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-settled-failure-'));
  const core = await createCore({ dataDir, analyzer, cooldownMs });
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }); });
  const sourceKey = await core.registerSource({ registrationId: 'settled-test', hostRunId: 'host', name: 'ChatGPT', policy: 'chatgpt-main/1' });
  const scope = { sourceKey, mainId: 'main', lifecycleId: 'run' }, at = Date.now(), ref = eventId => ({ taskId: 'main', lifecycleId: 'run', eventId });
  await core.ingest(sourceKey, { tasks: [{ id: 'main', lifecycleId: 'run', sessionId: 'main', title: 'Main', status: 'running' }], events: [
    { id: 'start', taskId: 'main', taskLifecycleId: 'run', kind: 'status', capturedAt: at, occurredAt: at, text: 'Started', data: { nativeType: 'task_started', sourceKind: 'chatgpt-native', sourceForm: 'event_msg', runId: 'run', turnTrigger: 'composer', status: 'running' } },
    { id: 'request', taskId: 'main', taskLifecycleId: 'run', kind: 'input', capturedAt: at, occurredAt: at, text: 'Observe this material', data: { nativeType: 'item_completed/UserMessage', sourceKind: 'chatgpt-native', sourceForm: 'event_msg/item_completed', runId: 'run', messageId: 'message', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false } },
  ], hierarchyContext: { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'main' }], relations: [], activations: [{ taskId: 'main', lifecycleId: 'run', sessionId: 'main', runId: 'run', state: 'running', stateRefs: [ref('start')] }], instructionBindings: [{ taskId: 'main', lifecycleId: 'run', inputRef: ref('request'), phase: 'consumed', origin: 'user-entry', originRefs: [ref('start'), ref('request')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } } });
  return { core, scope };
}
async function until(check) { for (let count = 0; count < 100; count++) { const value = await check(); if (value) return value; await delay(1); } assert.fail('controlled settlement did not arrive'); }

test('an explicitly failed completed check becomes a receipt without converting valid unknown checks', async t => {
  const f = await fixture(t, { async analyze() { const value = result(); value.checks[0].judgmentState = 'failed'; value.checks[0].error = { code: 'invalid_decision', stage: 'jev' }; return value; } }, 60_000);
  await until(async () => (await f.core.snapshot()).tasks[0].analysis.completedAt);
  const page = await f.core.failureReceipts(f.scope);
  assert.equal(page.receipts.length, 1);
  assert.equal(page.receipts[0].error.code, 'invalid_decision');
  assert.equal(page.receipts[0].error.stage, 'jev');
  const task = (await f.core.snapshot()).tasks[0];
  assert.equal(task.monitoringHealth.state, 'failed');
  assert.equal(task.analysis.checks.filter(check => check.judgmentState === 'failed').length, 1);
});

test('valid completed unknown recovers an earlier failure, while uncalled dependencies and cancellation do not fail', async t => {
  let calls = 0;
  const f = await fixture(t, { async analyze() { if (++calls === 1) throw new AnalysisError('deadline', 'llm', 'controlled deadline'); return result(); } });
  await until(async () => { const task = (await f.core.snapshot()).tasks[0]; return calls >= 2 && task.analysis.completedAt && !task.analysis.error && task.analysis.state !== 'analyzing'; });
  const page = await f.core.failureReceipts(f.scope);
  assert.equal(page.receipts.length, 1);
  assert.equal(page.receipts[0].error.code, 'deadline');
  assert.equal(page.receipts[0].state, 'recovered');
  const unconfigured = await fixture(t, createAnalyzer({}));
  await until(async () => (await unconfigured.core.snapshot()).tasks[0].analysis.completedAt);
  assert.deepEqual((await unconfigured.core.failureReceipts(unconfigured.scope)).receipts, []);
  let finish;
  const pending = await fixture(t, { analyze() { return new Promise(resolve => { finish = resolve; }); } });
  await until(() => finish);
  assert.deepEqual((await pending.core.failureReceipts(pending.scope)).receipts, []);
  const control = await pending.core.conversationMonitoring({ registrationId: 'settled-test', taskId: 'main' });
  await pending.core.setConversationMonitoring({ ...control, paused: true });
  finish(result()); await delay(1);
  assert.deepEqual((await pending.core.failureReceipts(pending.scope)).receipts, []);
});
