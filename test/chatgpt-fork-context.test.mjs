import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

const at = 1_800_000_000_000
const ref = eventId => ({ taskId: 'fork', lifecycleId: 'current', eventId })
const event = (id, kind, text, data, capturedAt = at - 10) => ({ id, taskId: 'fork', taskLifecycleId: 'current', kind, occurredAt: at - 1000, capturedAt, text, data: { sourceKind: 'chatgpt-native', runId: 'current', ...data } })
export const forkContextBatch = () => ({
  tasks: [{ id: 'fork', lifecycleId: 'current', sessionId: 'fork', title: 'Fork', status: 'running' }],
  events: [
    event('start', 'status', 'task_started', { nativeType: 'task_started', sourceForm: 'event_msg', turnTrigger: 'composer', status: 'running' }),
    event('current-user', 'input', 'Inspect the new request', { nativeType: 'item_completed/UserMessage', sourceForm: 'event_msg/item_completed', messageId: 'current-message', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false }),
    event('boundary', 'status', 'Inherited history boundary', { nativeType: 'session_meta', sourceForm: 'session_meta/history_base', evidenceKind: 'fork-boundary', forkThreadId: 'fork', sourceThreadId: 'original', endOrdinalExclusive: 20, endByteOffset: 1000, history: true, inherited: true }),
    event('old-start', 'status', 'task_started', { nativeType: 'task_started', sourceForm: 'event_msg', turnTrigger: 'composer', sourceThreadId: 'original', forkThreadId: 'fork', originRunId: 'old-run', recordOrdinal: 5, history: true, inherited: true }),
    event('old-user', 'input', 'Never write outside the requested directory', { nativeType: 'item_completed/UserMessage', sourceForm: 'event_msg/item_completed', messageId: 'old-message', inputOrigin: 'user-entry', inputPhase: 'consumed', sourceThreadId: 'original', forkThreadId: 'fork', originRunId: 'old-run', recordOrdinal: 6, history: true, inherited: true }),
    event('old-result', 'tool-result', 'Old tests passed', { nativeType: 'item_completed/CommandExecution', sourceForm: 'event_msg/item_completed', sourceThreadId: 'original', forkThreadId: 'fork', originRunId: 'old-run', recordOrdinal: 7, exitCode: 0, history: true, inherited: true }, at - 1),
    event('current-output', 'output', 'Inspecting the new request', { nativeType: 'item_completed/AgentMessage', sourceForm: 'event_msg/item_completed' }),
  ],
  hierarchyContext: { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'fork' }], activations: [{ taskId: 'fork', lifecycleId: 'current', sessionId: 'fork', runId: 'current', state: 'running', stateRefs: [ref('start')] }], relations: [], instructionBindings: [{ taskId: 'fork', lifecycleId: 'current', inputRef: ref('current-user'), phase: 'consumed', origin: 'user-entry', originRefs: ['start', 'current-user', 'boundary', 'old-start', 'old-user', 'old-result'].map(ref), ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } },
})
async function fixture(t) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-fork-context-'))
  const server = await startServer({ dataDir: dir, now: () => at }), core = await connectCore({ dataDir: dir })
  const source = await core.registerSource({ registrationId: 'fork-context', hostRunId: 'host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  t.after(async () => { await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  return { core, source }
}

test('public HTTP keeps verified inherited user context apart from the fork current request and old tool activity', async t => {
  const { core, source } = await fixture(t)
  await core.ingest(source, forkContextBatch())
  const task = (await core.snapshot()).tasks[0]
  assert.equal(task.currentRequest, 'Inspect the new request')
  assert.deepEqual(task.instructionContext.userLimitRefs, [ref('old-user')])
  assert.equal(task.events.find(item => item.id === 'old-user').authorizationEligible, false)
  assert.equal(task.events.find(item => item.id === 'current-user').authorizationEligible, true)
  assert.equal(task.lastActivity, at - 10, 'capturing an old tool result does not count as new fork activity')
  assert.equal(task.status, 'running')
})

test('fork context cannot be established by an agent promise, mismatched source, missing boundary, later ordinal or unproved user trigger', async t => {
  for (const [name, mutate] of Object.entries({
    'agent-promise': value => { value.events[4].data.nativeType = 'item_completed/AgentMessage' },
    'wrong-source': value => { value.events[4].data.sourceThreadId = 'unrelated' },
    'missing-boundary': value => { value.hierarchyContext.instructionBindings[0].originRefs = value.hierarchyContext.instructionBindings[0].originRefs.filter(item => item.eventId !== 'boundary') },
    'outside-cutoff': value => { value.events[4].data.recordOrdinal = 20 },
    'goal-replay': value => { value.events[3].data.turnTrigger = 'goal' },
  })) await t.test(name, async t => {
    const { core, source } = await fixture(t), value = forkContextBatch(); mutate(value)
    await assert.rejects(core.ingest(source, value), error => error.code === 'hierarchy_context_invalid')
    assert.deepEqual((await core.snapshot()).tasks, [])
  })
})

test('a later fork execution reuses retained context refs from its earlier observation container without copying their capture time', async t => {
  const { core, source } = await fixture(t)
  const initial = forkContextBatch(); await core.ingest(source, initial)
  const nextRef = eventId => ({ taskId: 'fork', lifecycleId: 'next', eventId })
  const nextStart = { ...event('next-start', 'status', 'task_started', { nativeType: 'task_started', sourceForm: 'event_msg', turnTrigger: 'composer', status: 'running' }, at - 5), taskLifecycleId: 'next' }; nextStart.data.runId = 'next'
  const nextUser = { ...event('next-user', 'input', 'Inspect a later request', { nativeType: 'item_completed/UserMessage', sourceForm: 'event_msg/item_completed', messageId: 'next-message', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false }, at - 4), taskLifecycleId: 'next' }; nextUser.data.runId = 'next'
  const previousEnd = event('previous-end', 'status', 'task_complete', { nativeType: 'task_complete', sourceForm: 'event_msg', status: 'idle' }, at - 6)
  await core.ingest(source, { tasks: [{ id: 'fork', lifecycleId: 'current', sessionId: 'fork', title: 'Fork', status: 'idle' }, { id: 'fork', lifecycleId: 'next', sessionId: 'fork', title: 'Fork', status: 'running' }], events: [previousEnd, nextStart, nextUser], hierarchyContext: { schemaVersion: 1, revision: 2, sessions: [{ sessionId: 'fork' }], activations: [{ taskId: 'fork', lifecycleId: 'next', sessionId: 'fork', runId: 'next', state: 'running', stateRefs: [nextRef('next-start')] }], relations: [], instructionBindings: [{ taskId: 'fork', lifecycleId: 'next', inputRef: nextRef('next-user'), phase: 'consumed', origin: 'user-entry', originRefs: [nextRef('next-start'), nextRef('next-user'), ref('boundary'), ref('old-start'), ref('old-user')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } } })
  const tasks = (await core.snapshot()).tasks, next = tasks.find(item => item.lifecycleId === 'next'), previous = tasks.find(item => item.lifecycleId === 'current')
  assert.equal(next.currentRequest, 'Inspect a later request')
  assert.deepEqual(next.instructionContext.userLimitRefs, [ref('old-user')])
  assert.equal(previous.events.find(item => item.id === 'old-user').capturedAt, at - 10)
  assert.equal(next.events.some(item => item.id === 'old-user'), false)
})
