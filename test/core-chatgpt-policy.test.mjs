import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';

test('ChatGPT policy requires native request proof while legacy inputs retain their behavior through HTTP', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-chatgpt-policy-'));
  const server = await startServer({ dataDir });
  const client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  const descriptor = { registrationId: 'chatgpt-policy', hostRunId: 'host', name: 'ChatGPT main', policy: 'chatgpt-main/1' };
  const sourceKey = await client.registerSource(descriptor);
  const peer = await client.registerSource({ registrationId: 'legacy', hostRunId: 'host', name: 'Legacy' });
  const task = { id: 'main', sessionId: 'main', lifecycleId: 'turn', title: 'Main', status: 'running' };
  const event = { id: 'role-only', taskId: 'main', taskLifecycleId: 'turn', kind: 'input', capturedAt: Date.now(), occurredAt: null,
    text: 'A role label alone cannot grant permission', data: { sourceKind: 'user', inputOrigin: 'user-entry' } };
  await client.ingest(sourceKey, { tasks: [task], events: [event] });
  await client.ingest(peer, { tasks: [task], events: [{ ...event, id: 'legacy-request' }] });
  const snapshot = await client.snapshot();
  const observed = snapshot.tasks.find(value => value.sourceKey === sourceKey);
  assert.equal(observed.currentRequest, '');
  assert.equal(observed.analysis.completedAt, null);
  assert.ok(observed.coverage.gaps.includes('Current user request is unverified'));
  assert.equal(snapshot.tasks.find(value => value.sourceKey === peer).currentRequest, event.text);
  assert.equal(snapshot.sources.find(value => value.key === sourceKey).descriptor.policy, 'chatgpt-main/1');
  assert.ok((await client.health()).features.includes('chatgpt-main/1'));
  await assert.rejects(client.registerSource({ ...descriptor, policy: 'chatgpt-main/999' }), error => error.status === 400);
  assert.equal((await client.snapshot()).sources.find(value => value.key === sourceKey).descriptor.policy, 'chatgpt-main/1');
});

test('adopting ChatGPT policy does not reuse a request admitted under legacy rules', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-chatgpt-policy-switch-'));
  const server = await startServer({ dataDir });
  const client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  const descriptor = { registrationId: 'adopting-policy', hostRunId: 'host', name: 'Current main' };
  const sourceKey = await client.registerSource(descriptor);
  await client.ingest(sourceKey, { tasks: [{ id: 'main', sessionId: 'main', lifecycleId: 'turn', title: 'Main', status: 'running' }],
    events: [{ id: 'old-role', taskId: 'main', taskLifecycleId: 'turn', kind: 'input', capturedAt: Date.now(), occurredAt: null, text: 'Previously admitted by legacy rules' }] });
  assert.equal((await client.snapshot()).tasks[0].currentRequest, 'Previously admitted by legacy rules');
  assert.equal(await client.registerSource({ ...descriptor, policy: 'chatgpt-main/1' }), sourceKey);
  const current = (await client.snapshot()).tasks[0];
  assert.equal(current.currentRequest, '');
  assert.equal(current.analysis.summary, null);
  assert.equal(current.analysis.completedAt, null);
  assert.deepEqual(await client.claimNotifications(sourceKey), []);
});

test('a typed native user message binds its own main execution and exposes authorization provenance through HTTP', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-chatgpt-native-request-'));
  const server = await startServer({ dataDir });
  const client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  const sourceKey = await client.registerSource({ registrationId: 'native-request', hostRunId: 'host', name: 'Main', policy: 'chatgpt-main/1' });
  const time = Date.now(), ref = eventId => ({ taskId: 'main', lifecycleId: 'turn', eventId });
  const native = (id, kind, data, text) => ({ id, taskId: 'main', taskLifecycleId: 'turn', kind, capturedAt: time, occurredAt: null, text, data });
  const start = native('start', 'status', { nativeType: 'task_started', sourceKind: 'chatgpt-native', sourceForm: 'event_msg', runId: 'turn', turnTrigger: 'edit_user_message', status: 'running' }, 'Execution started');
  const input = native('user', 'input', { nativeType: 'item_completed/UserMessage', sourceKind: 'chatgpt-native', sourceForm: 'event_msg/item_completed', runId: 'turn', messageId: 'native-message', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false }, 'Only inspect the existing material');
  const context = { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'main' }],
    activations: [{ taskId: 'main', lifecycleId: 'turn', sessionId: 'main', runId: 'turn', state: 'running', stateRefs: [ref('start')] }], relations: [],
    instructionBindings: [{ taskId: 'main', lifecycleId: 'turn', inputRef: ref('user'), origin: 'user-entry', phase: 'consumed', originRefs: [ref('start'), ref('user')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } };
  const prelude = native('prelude', 'status', { nativeType: 'response_item/message', inputOrigin: 'unknown', truncated: true }, 'Earlier unverified model context');
  await client.ingest(sourceKey, { tasks: [{ id: 'main', sessionId: 'main', lifecycleId: 'turn', title: 'Main', status: 'running' }], events: [start, prelude, input], hierarchyContext: context });
  const observed = (await client.snapshot()).tasks[0];
  assert.equal(observed.currentRequest, input.text);
  assert.equal(observed.events.find(event => event.id === 'user').authorizationEligible, true);
  assert.equal(observed.instructionContext.binding.origin, 'user-entry');
  assert.equal(observed.coverage.complete, true, 'a pre-request excerpt does not become current evidence merely because capture timestamps tie');
  const before = await client.snapshot();
  await assert.rejects(client.ingest(sourceKey, { events: [{ ...start, id: 'goal-start', data: { ...start.data, turnTrigger: 'goal' } }],
    hierarchyContext: { ...context, revision: 2, instructionBindings: [{ ...context.instructionBindings[0], originRefs: [ref('goal-start'), ref('user')] }] } }), error => error.code === 'hierarchy_context_invalid');
  assert.deepEqual((await client.snapshot()).tasks[0].events, before.tasks[0].events);
  await client.ingest(sourceKey, { events: [native('later-output', 'output', { truncated: true }, 'Incomplete current output')] });
  assert.ok((await client.snapshot()).tasks[0].coverage.gaps.includes('Some observed excerpts are truncated'));
});
