import test from 'node:test'
import assert from 'node:assert/strict'
import { createObservation, hostIdentity } from '../dsh/observation.mjs'

const DAY = 24 * 60 * 60 * 1000
const agent = id => ({ id, status: 'running', session: { id, header: {} } })
const ack = collector => { const batch = collector.batch(); collector.acknowledge(batch.events.map(event => event.id), batch.tasks) }

test('an idle baseline is delivered once before its metadata acknowledgement and is then pruned', () => {
  const collector = createObservation({ identity: hostIdentity({}) })
  const idle = agent('idle-baseline'); idle.status = 'idle'
  collector.baseline([idle])
  const batch = collector.batch()
  assert.equal(batch.tasks.length, 1)
  assert.equal(batch.tasks[0].status, 'idle')
  assert.equal(collector.batch().tasks.length, 1)
  collector.acknowledge([], batch.tasks)
  assert.equal(collector.batch().tasks.length, 0)
  collector.close()
})

test('an old metadata acknowledgement cannot drop a new idle title, and a later native title recreates one pending row', () => {
  const collector = createObservation({ identity: hostIdentity({}) })
  const idle = agent('idle-titles'); idle.status = 'idle'; idle.session.header.title = 'initial'
  idle.session.snapshotEvents = () => { throw new Error('history must not be read') }
  collector.baseline([idle])
  const first = collector.batch()
  collector.observe('session/event', { session: idle.session, event: { type: 'session/title', seq: 0, data: { title: 'second' } } })
  collector.acknowledge([], first.tasks)
  const second = collector.batch()
  assert.equal(second.tasks.length, 1); assert.equal(second.tasks[0].title, 'second')
  assert.equal(second.tasks[0].lifecycleId, first.tasks[0].lifecycleId)
  collector.acknowledge([], second.tasks)
  assert.equal(collector.batch().tasks.length, 0)
  collector.observe('session/event', { session: idle.session, event: { type: 'session/title', seq: 1, data: { title: 'third' } } })
  const third = collector.batch()
  assert.equal(third.tasks.length, 1); assert.equal(third.tasks[0].title, 'third')
  assert.equal(third.tasks[0].lifecycleId, first.tasks[0].lifecycleId)
  collector.acknowledge([], third.tasks); assert.equal(collector.batch().tasks.length, 0)
  collector.close()
})

test('100 ended task and tool lifecycles release their owned indexes after acknowledgement', () => {
  let time = 1000
  const collector = createObservation({ identity: hostIdentity({}), now: () => time })
  for (let i = 0; i < 100; i++) {
    const actor = agent(`task-${i}`)
    collector.observe('agent/created', { agent: actor })
    const exec = { agent: actor, token: Symbol(), callId: `call-${i}`, name: 'read', arguments: {} }
    collector.observe('tools/execute', { exec })
    collector.observe('tools/result', { exec, result: { isError: false, content: [{ type: 'text', text: 'fixed' }] } })
    actor.status = 'disposed'; collector.observe('agent/disposed', { agent: actor })
    ack(collector)
  }
  time += DAY + 1
  assert.equal(collector.batch().tasks.length, 0)
  assert.deepEqual(collector.diagnostics(), {
    bufferedEvents: 0, taskRows: 0, toolDedupeEntries: 0, redactedCalls: 0,
    pendingTools: 0, gaps: 0, closed: false,
  })
  collector.close()
})

test('long-running call indexes expire but a first current native result is fresh evidence and its later replay cannot renew it', () => {
  let time = 1000
  const actor = agent('long-running'), identity = hostIdentity({})
  const collector = createObservation({ identity, now: () => time })
  collector.baseline([actor])
  const lifecycleId = collector.batch().tasks[0].lifecycleId
  const exec = { agent: actor, token: Symbol(), callId: 'expired-call', name: 'bash', arguments: { command: 'exit 7' } }
  collector.observe('tools/execute', { exec }); ack(collector)
  assert.equal(collector.diagnostics().pendingTools, 1)
  time += DAY + 1
  assert.equal(collector.diagnostics().pendingTools, 0)
  assert.equal(collector.diagnostics().toolDedupeEntries, 0)
  const result = { isError: false, content: [{ type: 'text', text: 'first-current-result' }],
    value: { kind: 'foreground', cwd: '/private/tmp', exitCode: 7, signal: null, timedOut: false, aborted: false,
      timeoutMs: 2000, stdout: { text: 'first-current-result', truncated: false }, stderr: { text: '', truncated: false } } }
  collector.observe('tools/result', { exec, result })
  const fresh = collector.batch().events.find(event => event.kind === 'tool-result')
  assert.ok(fresh); assert.equal(fresh.capturedAt, time)
  assert.equal(fresh.data.exitCode, 7); assert.equal(fresh.data.resultSource, 'native-shell-canonical')
  assert.equal(fresh.data.refinesEventId, undefined)
  ack(collector); time += DAY + 1
  collector.observe('tools/result', { exec, result })
  assert.equal(collector.batch().events.length, 0)
  collector.observe('agent/assistant-stream', { agent: actor, frame: { type: 'chunk', revision: 1, chunk: { text: 'new activity' } } })
  assert.equal(collector.batch().tasks[0].lifecycleId, lifecycleId)
  assert.equal(collector.batch().events[0].text, 'new activity')
  collector.close()
  const replacement = createObservation({ identity, now: () => time })
  replacement.baseline([actor]); assert.equal(replacement.batch().tasks[0].lifecycleId, lifecycleId)
  replacement.close()
})

test('all tool indexes and task rows obey the 500-entry boundary and replay preserves event identity', () => {
  let time = 1000
  const collector = createObservation({ identity: hostIdentity({}), now: () => time })
  const actor = agent('one'), calls = []
  collector.baseline([actor])
  for (let i = 0; i < 600; i++) {
    const exec = { agent: actor, token: Symbol(), callId: `call-${i}`, name: 'read', arguments: { command: 'Authorization: Bearer fixture-secret' } }
    calls.push(exec); collector.observe('tools/execute', { exec })
  }
  assert.equal(collector.diagnostics().bufferedEvents, 500)
  assert.equal(collector.diagnostics().toolDedupeEntries, 500)
  assert.equal(collector.diagnostics().redactedCalls, 500)
  assert.equal(collector.diagnostics().pendingTools, 500)
  collector.observe('tools/execute', { exec: calls[0] })
  const original = collector.batch().events.find(event => event.data.callId === 'call-0')
  assert.ok(original)
  assert.equal(original.capturedAt, 1000)
  assert.ok(!JSON.stringify(collector.batch()).includes('fixture-secret'))
  time += 100
  collector.observe('tools/result', { exec: calls[0], result: { isError: false, content: [{ type: 'text', text: 'secret-derived-result' }] } })
  const result = collector.batch().events.find(event => event.kind === 'tool-result' && event.data.callId === 'call-0')
  assert.equal(result.text, ''); assert.equal(result.data.redacted, true)
  collector.observe('tools/result', { exec: calls[0], result: { isError: false, content: [{ type: 'text', text: 'different-replay' }] } })
  assert.equal(collector.batch().events.filter(event => event.id === result.id).length, 1)
  for (let i = 0; i < 600; i++) collector.observe('agent/created', { agent: agent(`extra-${i}`) })
  assert.equal(collector.diagnostics().taskRows, 500)
  assert.ok(collector.batch().gaps.some(gap => gap.includes('任务索引')))
  collector.close(); collector.close()
  assert.equal(collector.diagnostics().toolDedupeEntries, 0)
  assert.equal(collector.diagnostics().pendingTools, 0)
})

test('an old disposed native lifecycle cannot recreate a row or pollute a new Agent with the same local ID', () => {
  const collector = createObservation({ identity: hostIdentity({}) })
  const old = agent('same-id')
  collector.observe('agent/created', { agent: old })
  const oldId = collector.batch().tasks[0].lifecycleId
  old.status = 'disposed'; collector.observe('agent/disposed', { agent: old }); ack(collector)
  const current = agent('same-id'); collector.observe('agent/created', { agent: current })
  const currentId = collector.batch().tasks[0].lifecycleId
  assert.notEqual(currentId, oldId)
  const exec = { agent: old, token: Symbol(), callId: 'late-old', name: 'read', arguments: {} }
  collector.observe('tools/result', { exec, result: { content: [{ type: 'text', text: 'wrong lifecycle' }] } })
  assert.equal(collector.batch().tasks.length, 1)
  assert.ok(collector.batch().events.every(event => event.taskLifecycleId === currentId))
  collector.close()
})

test('24-hour expiry removes inherited input and run references while preserving a running native identity', () => {
  let time = 1_800_000_000_000
  const identity = hostIdentity({}), collector = createObservation({ identity, hierarchy: true, now: () => time, readCatalog: () => [] })
  const parent = agent('expiry-parent'), child = agent('expiry-child')
  child.session.header.parentSession = parent.session.id
  collector.baseline([parent, child])
  collector.observe('session/event', { session: parent.session, event: { seq: 0, time, type: 'subagent/catalog', data: { version: 0, childId: child.session.id, mode: 'one-shot' } } })
  collector.observe('subagent/start', { parent, child, activation: { id: child.id, runId: 'expiry-run' } })
  const message = { id: 'expiry-user', source: { kind: 'user' }, content: [{ type: 'text', text: 'Inspect only and preserve files.' }] }
  collector.observe('agent/inbox/claimed', { agent: parent, messages: [message] })
  collector.observe('session/event', { session: parent.session, event: { seq: 1, time, type: 'user/message', data: message } })
  const before = collector.batch(), lifecycleId = before.tasks.find(task => task.id === parent.id).lifecycleId
  assert.ok(collector.diagnostics().hierarchyInputs > 0)
  assert.equal(collector.diagnostics().hierarchyRuns, 1)
  collector.acknowledge(before.events.map(event => event.id), before.tasks)
  time += DAY + 1
  const expired = collector.batch(), counts = collector.diagnostics()
  assert.equal(counts.hierarchyInputs, 0)
  assert.equal(counts.hierarchyRuns, 0)
  assert.equal(counts.hierarchyRelations, 0)
  assert.equal(expired.hierarchyContext.instructionBindings.length, 0)
  assert.equal(expired.hierarchyContext.activations.find(node => node.sessionId === child.id).runId, null)
  assert.equal(expired.tasks.find(task => task.id === parent.id).lifecycleId, lifecycleId)
  assert.equal(parent.status, 'running')
  collector.close()
  assert.equal(identity.hierarchyContent, null)
})

test('native read failure gaps omit a complete credential field before excerpting or transport retention', () => {
  const collector = createObservation({ identity: hostIdentity({}), hierarchy: true })
  const secret = 'SENTINEL-FIXTURE-GAP-SECRET'
  collector.recordGap(`${'x'.repeat(300)} Authorization: Bearer ${secret}`)
  collector.observe('session/observed', { sessionId: 'failed-cold', header: { title: 'Failed cold' }, events: [], cursor: -1, readComplete: false,
    gaps: [`native read failed: Authorization: Bearer ${secret}`] })
  const batch = collector.batch()
  assert.ok(!JSON.stringify(batch).includes(secret))
  assert.ok(batch.gaps.some(gap => gap.includes('认证材料已省略')))
  assert.ok(batch.hierarchyContext.coverage.branchGaps.some(gap => gap.reason.includes('认证材料已省略')))
  collector.close()
})
