import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createCore } from '../src/core/index.mjs'

const at = 1_800_000_000_000
async function fixture(t, options = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-material-proof-'))
  const core = await createCore({ dataDir, now: () => at, leaseMs: 1_000_000, ...options })
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }) })
  return core
}
const ref = eventId => ({ taskId: 'main', lifecycleId: 'turn', eventId })
const batch = () => ({
  tasks: [{ id: 'main', lifecycleId: 'turn', sessionId: 'main', title: 'Main', status: 'running' }],
  events: [
    { id: 'start', taskId: 'main', taskLifecycleId: 'turn', kind: 'status', occurredAt: at - 2, capturedAt: at - 2, text: 'task_started', data: { nativeType: 'task_started', runId: 'turn', sourceKind: 'chatgpt-native', sourceForm: 'event_msg', turnTrigger: 'edit_user_message', status: 'running' } },
    { id: 'producer', taskId: 'main', taskLifecycleId: 'turn', kind: 'input', occurredAt: at - 1, capturedAt: at - 1, text: 'Read only the log', data: { nativeType: 'item_completed/UserMessage', sourceKind: 'chatgpt-native', sourceForm: 'event_msg/item_completed', runId: 'turn', messageId: 'native-message', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false } },
  ],
  hierarchyContext: { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'main' }], activations: [{ taskId: 'main', lifecycleId: 'turn', sessionId: 'main', runId: 'turn', state: 'running', stateRefs: [ref('start')] }], relations: [], instructionBindings: [{ taskId: 'main', lifecycleId: 'turn', inputRef: ref('producer'), phase: 'consumed', origin: 'user-entry', originRefs: [ref('start'), ref('producer')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } },
})

test('the public core accepts a source-bound typed native user producer without fabricating DSH receipts', async t => {
  const core = await fixture(t)
  const key = await core.registerSource({ registrationId: 'native-proof', hostRunId: 'host', name: 'Native', policy: 'chatgpt-main/1' })
  await core.ingest(key, batch())
  const task = (await core.snapshot()).tasks[0]
  assert.equal(task.currentRequest, 'Read only the log')
  assert.equal(task.events.find(event => event.id === 'producer').authorizationEligible, true)
  assert.ok(task.events.every(event => event.data.nativeType !== 'agent/inbox/claimed'))
})

test('the public ChatGPT status calls silence unconfirmed and never treats an asynchronous question as waiting for you', async t => {
  let time = at
  const core = await fixture(t, { now: () => time })
  const key = await core.registerSource({ registrationId: 'native-wait', hostRunId: 'host', name: 'Native', policy: 'chatgpt-main/1' })
  await core.ingest(key, batch())
  let task = (await core.snapshot()).tasks[0]
  assert.equal(task.gs.light, 'gray')
  assert.equal(task.gs.label, '等待原因未确认')
  assert.equal(task.gs.waiting.durationMs, 1)
  await core.ingest(key, { events: [{ id: 'question', taskId: 'main', taskLifecycleId: 'turn', kind: 'status', occurredAt: time, capturedAt: time, text: 'Asynchronous question opened', data: { nativeType: 'user-questions/request-start', waitId: 'async-question' } }] })
  time += 400_000
  task = (await core.snapshot()).tasks[0]
  assert.equal(task.gs.light, 'gray')
  assert.equal(task.gs.label, '等待原因未确认')
  assert.equal(task.gs.waiting.kind, 'unconfirmed')
  assert.ok(task.gs.waiting.durationMs >= 400_000)
  assert.deepEqual(await core.claimNotifications(key), [])
})

test('native user proof rejects role relabeling, goal replay, missing references, foreign runs and legacy source admission', async t => {
  const mutations = {
    role: value => { value.events[1].data.nativeType = 'response_item/message' },
    goal: value => { value.events[0].data.turnTrigger = 'goal' },
    'missing-self': value => { value.hierarchyContext.instructionBindings[0].originRefs = [ref('start')] },
    'missing-start': value => { value.hierarchyContext.instructionBindings[0].originRefs = [ref('producer')] },
    'foreign-run': value => { value.events[1].data.runId = 'foreign-run' },
    inherited: value => { value.events[1].data.inherited = true },
    history: value => { value.events[1].data.history = true },
    legacy: () => {},
  }
  for (const [name, mutate] of Object.entries(mutations)) await t.test(name, async t => {
    const core = await fixture(t)
    const key = await core.registerSource({ registrationId: name, hostRunId: 'host', name: 'Native', ...(name === 'legacy' ? {} : { policy: 'chatgpt-main/1' }) })
    const value = batch(); mutate(value)
    await assert.rejects(core.ingest(key, value), error => error.code === 'hierarchy_context_invalid')
    assert.deepEqual((await core.snapshot()).tasks, [], 'invalid proof must not partially admit the request')
  })
})

test('a native execution without a confirmed request still reports its observed waiting duration', async t => {
  let time = at
  const core = await fixture(t, { now: () => time })
  const key = await core.registerSource({ registrationId: 'goal-duration', hostRunId: 'host', name: 'Native', policy: 'chatgpt-main/1' })
  const value = batch(); value.events = [value.events[0]]; value.events[0].data.turnTrigger = 'goal'; value.hierarchyContext.instructionBindings = []
  await core.ingest(key, value)
  time += 5000
  const task = (await core.snapshot()).tasks[0]
  assert.equal(task.currentRequest, '')
  assert.equal(task.gs.label, '等待原因未确认')
  assert.equal(task.gs.waiting.durationMs, 5002)
})
