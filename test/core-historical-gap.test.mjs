import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

test('HTTP ingestion retains inherited gaps as historical evidence while only current gaps make current coverage incomplete', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-historical-gap-')), server = await startServer({ dataDir }), client = await connectCore({ dataDir })
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  for (const inherited of [true, false]) {
    const source = await client.registerSource({ registrationId: 'gap-' + inherited, hostRunId: 'host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
    const capturedAt = Date.now(), ref = eventId => ({ taskId: 'main', lifecycleId: 'turn', eventId })
    const event = (id, kind, text, data) => ({ id, kind, text, data, taskId: 'main', taskLifecycleId: 'turn', capturedAt, occurredAt: null })
    const events = [event('start', 'status', 'Started', { nativeType: 'task_started', sourceKind: 'chatgpt-native', sourceForm: 'event_msg', runId: 'turn', turnTrigger: 'composer', status: 'running' }), event('user', 'input', 'Read this execution only', { nativeType: 'item_completed/UserMessage', sourceKind: 'chatgpt-native', sourceForm: 'event_msg/item_completed', runId: 'turn', messageId: 'user', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false }), event('gap', 'gap', 'Opaque tool material omitted', inherited ? { history: true, inherited: true } : {})]
    const context = { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'main' }], activations: [{ taskId: 'main', lifecycleId: 'turn', sessionId: 'main', runId: 'turn', state: 'running', stateRefs: [ref('start')] }], relations: [], instructionBindings: [{ taskId: 'main', lifecycleId: 'turn', inputRef: ref('user'), origin: 'user-entry', phase: 'consumed', originRefs: [ref('user'), ref('start')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } }
    await client.ingest(source, { tasks: [{ id: 'main', sessionId: 'main', lifecycleId: 'turn', title: 'Main', status: 'running' }], events, hierarchyContext: context })
    const task = (await client.snapshot()).tasks.find(value => value.sourceKey === source)
    assert.ok(task.events.some(value => value.id === 'gap'), 'the original gap evidence remains visible')
    assert.equal(task.coverage.complete, inherited, JSON.stringify(task.coverage))
    assert.equal(task.coverage.gaps.includes('Opaque tool material omitted'), !inherited)
  }
})

test('late inherited tool excerpts do not invalidate current ChatGPT capture coverage', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-late-history-')), server = await startServer({ dataDir }), client = await connectCore({ dataDir })
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  for (const field of ['truncated', 'redacted']) await t.test(field, async () => {
    const source = await client.registerSource({ registrationId: 'late-history-' + field, hostRunId: 'host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
    const capturedAt = Date.now() - 2000, ref = eventId => ({ taskId: 'main', lifecycleId: 'turn', eventId })
    const event = (id, kind, text, data, at = capturedAt) => ({ id, kind, text, data, taskId: 'main', taskLifecycleId: 'turn', capturedAt: at, occurredAt: null })
    const start = event('start', 'status', 'Started', { nativeType: 'task_started', sourceKind: 'chatgpt-native', sourceForm: 'event_msg', runId: 'turn', turnTrigger: 'composer', status: 'running' })
    const input = event('user', 'input', 'Read the current execution', { nativeType: 'item_completed/UserMessage', sourceKind: 'chatgpt-native', sourceForm: 'event_msg/item_completed', runId: 'turn', messageId: 'user', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false })
    const context = { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'main' }], activations: [{ taskId: 'main', lifecycleId: 'turn', sessionId: 'main', runId: 'turn', state: 'running', stateRefs: [ref('start')] }], relations: [], instructionBindings: [{ taskId: 'main', lifecycleId: 'turn', inputRef: ref('user'), origin: 'user-entry', phase: 'consumed', originRefs: [ref('user'), ref('start')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } }
    await client.ingest(source, { tasks: [{ id: 'main', sessionId: 'main', lifecycleId: 'turn', title: 'Main', status: 'running' }], events: [start, input], hierarchyContext: context })
    const observed = async () => (await client.snapshot()).tasks.find(value => value.sourceKey === source)
    assert.equal((await observed()).coverage.complete, true)
    await client.ingest(source, { events: [event('old-tool', 'tool-result', 'Earlier tool excerpt', { history: true, inherited: true, [field]: true, callId: 'old-call' }, capturedAt + 500)] })
    const afterHistory = await observed()
    assert.ok(afterHistory.events.some(value => value.id === 'old-tool' && value.data.history && value.data[field]))
    assert.equal(afterHistory.currentRequest, input.text)
    assert.equal(afterHistory.coverage.complete, true, 'a later first-capture time does not turn old tool limitations into a current capture gap')
    await client.ingest(source, { events: [event('current-tool', 'tool-result', 'Current incomplete excerpt', { [field]: true, callId: 'current-call' }, capturedAt + 1000)] })
    assert.equal((await observed()).coverage.complete, false, 'a current excerpt limitation must still block current coverage')
  })
})
