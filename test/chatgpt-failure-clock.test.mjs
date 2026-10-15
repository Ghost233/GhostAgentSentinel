import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createCore } from '../src/core/index.mjs';

const ref = (taskId, eventId) => ({ taskId, lifecycleId: 'life', eventId });
for (const populated of [false, true, 'success']) for (const [kind, expected] of [
  ['future-entry', [false, false, true, true]],
  ['expiry-exit', [true, true, false, false]],
  ['rollback', [true, false, false, false]],
]) test(`legacy projection keeps ${kind} eligibility with ${populated === 'success' ? 'successful ChatGPT task' : populated ? 'retained' : 'empty'} failure indexes`, async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'failure-projection-clock-'));
  let clock = 1_000_000, armed = false, calls = 0;
  const now = () => { if (!armed) return clock; calls++; return kind === 'rollback' ? calls < 6 ? 1_000_002 : 999_998 : clock++; };
  const core = await createCore({ dataDir, now, leaseMs: 172_800_000 });
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }); });
  const chatgpt = await core.registerSource({ registrationId: 'chatgpt-clock', hostRunId: 'host', name: 'ChatGPT', policy: 'chatgpt-main/1' });
  if (populated === 'success') {
    await core.ingest(chatgpt, { tasks: [{ id: 'chatgpt-main', lifecycleId: 'run', title: 'ChatGPT', status: 'running' }] });
    await core.recordCollectionOutcome({ sourceKey: chatgpt, mainId: 'chatgpt-main', lifecycleId: 'run', roundId: 'capture-success', outcome: 'succeeded', settledAt: clock });
  } else if (populated) await core.recordCollectionOutcome({ sourceKey: chatgpt, mainId: null, lifecycleId: null, roundId: 'capture', outcome: 'failed', settledAt: clock, error: { code: 'EACCES', stage: 'collection' } });
  const source = await core.registerSource({ registrationId: kind, hostRunId: 'host', name: 'DSH' }), ids = ['one', 'two', 'three', 'four'];
  await core.ingest(source, { tasks: ids.map(id => ({ id, lifecycleId: 'life', sessionId: id, status: 'running', title: id })), events: ids.flatMap(taskId => [
    { id: 'state', taskId, taskLifecycleId: 'life', kind: 'status', text: '', capturedAt: clock, data: { nativeType: 'agent/status', status: 'running' } },
    { id: 'claim', taskId, taskLifecycleId: 'life', kind: 'input', text: 'Read only', capturedAt: clock, data: { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: taskId } },
    { id: 'request', taskId, taskLifecycleId: 'life', kind: 'input', text: 'Read only', capturedAt: clock, data: { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', messageId: taskId, inherited: false } },
  ]), hierarchyContext: { schemaVersion: 1, revision: 1, sessions: ids.map(sessionId => ({ sessionId })), activations: ids.map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: 'running', stateRefs: [ref(taskId, 'state')] })), relations: [], instructionBindings: ids.map(taskId => ({ taskId, lifecycleId: 'life', inputRef: ref(taskId, 'request'), phase: 'consumed', origin: 'user-entry', originRefs: [ref(taskId, 'claim')], ancestorLimitRefs: [] })), coverage: { complete: true, branchGaps: [] } } });
  clock = kind === 'expiry-exit' ? 1_000_000 + 86_400_000 - 8 : 1_000_000 - 8;
  armed = true;
  const snapshot = await core.snapshot();
  armed = false;
  assert.deepEqual(snapshot.tasks.filter(task => task.sourceKey === source).map(task => task.events.find(event => event.id === 'request').authorizationEligible), expected);
});
