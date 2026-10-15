import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createCore } from '../src/core/index.mjs';

test('ChatGPT seals time-only alerts at the core boundary while preserving an independently evidenced alert', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-chatgpt-wait-policy-'));
  let time = 1_800_000_000_000;
  const core = await createCore({ dataDir, now: () => time, leaseMs: 1e9, analyzer: { async analyze(input) {
    return { summary: { currentTask: input.task.currentRequest, currentState: 'Observed', recentProgress: '', attention: 'Unconfirmed wait; independent scope issue' },
      coverage: input.coverage, checks: ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait'].map(id => ({
        id, status: ['long_wait', 'request_drift'].includes(id) ? 'attention' : 'unknown',
        reason: id === 'long_wait' ? 'Timer alone says stalled' : 'Independent scope issue', evidenceIds: ['request', 'tool'],
      })) };
  } } });
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }); });
  const source = await core.registerSource({ registrationId: 'native-wait', hostRunId: 'host', name: 'Main', policy: 'chatgpt-main/1' });
  const ref = eventId => ({ taskId: 'main', lifecycleId: 'turn', eventId });
  const event = (id, kind, text, data) => ({ id, kind, text, data, taskId: 'main', taskLifecycleId: 'turn', occurredAt: time, capturedAt: time });
  await core.ingest(source, { tasks: [{ id: 'main', sessionId: 'main', lifecycleId: 'turn', title: 'Main', status: 'running' }], events: [
    event('start', 'status', 'Started', { nativeType: 'task_started', sourceKind: 'chatgpt-native', sourceForm: 'event_msg', runId: 'turn', turnTrigger: 'edit_user_message', status: 'running' }),
    event('request', 'input', 'Read the current file only', { nativeType: 'item_completed/UserMessage', sourceKind: 'chatgpt-native', sourceForm: 'event_msg/item_completed', runId: 'turn', messageId: 'message', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false }),
    event('tool', 'tool-start', 'Write an unrelated file', { toolName: 'write_file', callId: 'call' }),
  ], hierarchyContext: { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'main' }],
    activations: [{ taskId: 'main', lifecycleId: 'turn', sessionId: 'main', runId: 'turn', state: 'running', stateRefs: [ref('start')] }], relations: [],
    instructionBindings: [{ taskId: 'main', lifecycleId: 'turn', inputRef: ref('request'), origin: 'user-entry', phase: 'consumed', originRefs: [ref('start'), ref('request')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } } });
  let observed;
  for (let i = 0; i < 100; i++) { observed = (await core.snapshot()).tasks[0]; if (observed.analysis.completedAt !== null) break; await delay(5); }
  assert.notEqual(observed.analysis.completedAt, null);
  assert.equal(observed.analysis.checks.find(check => check.id === 'long_wait').status, 'unknown');
  assert.equal(observed.alerts.some(alert => alert.checkId === 'long_wait' && alert.status === 'attention'), false);
  assert.equal(observed.alerts.find(alert => alert.checkId === 'request_drift').status, 'attention');
  assert.equal(observed.gs.light, 'red');
  time += 4000;
  const notifications = await core.claimNotifications(source);
  assert.equal(notifications.length, 1);
  assert.deepEqual(notifications[0].members.flatMap(member => member.checkIds), ['request_drift']);
});
