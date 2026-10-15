import test from 'node:test'
import assert from 'node:assert/strict'
import { createObservation, hostIdentity } from '../dsh/observation.mjs'

// Collector inputs only; these cases do not attest an executed SDK join.
function boundary(t, { maxEvents = 500 } = {}) {
  let clock = 1_000_000
  const parent = { id: 'parent', status: 'running', session: { id: 'parent', seq: 2, header: { id: 'parent', title: 'Parent' } } }
  const child = { id: 'child', status: 'running', session: { id: 'child', seq: 0, header: { id: 'child', parentSession: 'parent', title: 'Child' } } }
  const collector = createObservation({ identity: hostIdentity({}), now: () => clock, hierarchy: true, maxEvents, readCatalog: session => session.id === 'parent' ? [{ id: 'child', mode: 'one-shot' }] : [] })
  t.after(() => { collector.close(); for (const [key, value] of Object.entries(collector.diagnostics())) if (key !== 'closed') assert.equal(value, 0, 'closed collector ' + key) })
  collector.observe('agent/created', { agent: parent })
  collector.observe('agent/created', { agent: child })
  collector.observe('subagent/start', { parent, child, activation: { id: 'child', runId: 'run-child' }, capturedAt: clock })
  const request = (agent = parent, id = 'request', seq = 0) => {
    const message = { id, source: { kind: 'user' }, content: [{ type: 'text', text: 'Inspect only.' }] }
    collector.observe('agent/inbox/claimed', { agent, message })
    collector.observe('session/event', { session: agent.session, event: { type: 'user/message', data: message, seq, time: clock } })
  }
  return { collector, parent, child, request, now: () => clock, advance: value => { clock += value }, info: waitId => ({ waitId, parent, parentStatusAtStart: 'running', parentSeqAtStart: 2, purpose: 'join-operation', targets: [{ id: 'child', runId: 'run-child', agent: child }], coverage: { complete: true, gaps: [] }, timestamp: clock }) }
}

test('a current running parent and active descendant join remain unconfirmed without an actual blocking caller witness', t => {
  const f = boundary(t), audit = f.info('join-only')
  f.collector.observe('subagent/wait-start', audit)
  const batch = f.collector.batch()
  assert.equal(batch.hierarchyContext.waitBindings?.length ?? 0, 0)
  assert.ok(batch.events.some(event => event.data.nativeType === 'subagent/wait-start'))
  assert.ok(batch.hierarchyContext.coverage.branchGaps.some(gap => gap.sessionId === 'parent' && gap.reason.includes('阻塞调用者')))
  assert.equal(batch.tasks.find(task => task.id === 'parent').status, 'running')
  assert.ok(!batch.events.some(event => event.data.nativeType === 'execution/wait-start'))
})

test('a correlated caller uses its exact current execution and request, retaining start and descendant run evidence', t => {
  const f = boundary(t); f.request()
  const execution = { agent: f.parent, token: {}, callId: 'actual-caller', name: 'read', arguments: {} }
  f.collector.observe('tools/execute', { exec: execution })
  f.collector.observe('subagent/wait-start', f.info('wait-a'))
  f.collector.observe('subagent/wait-caller-start', { parent: f.parent, execution, waitId: 'wait-a', timestamp: f.now() })
  const batch = f.collector.batch(), binding = batch.hierarchyContext.waitBindings?.[0]
  assert.equal(binding?.phase, 'started')
  assert.equal(binding.taskId, 'parent')
  assert.deepEqual(binding.childRefs, [{ taskId: 'child', lifecycleId: batch.tasks.find(task => task.id === 'child').lifecycleId }])
  assert.ok(binding.evidenceRefs.every(ref => batch.events.some(event => event.id === ref.eventId)))
  assert.ok(binding.evidenceRefs.some(ref => batch.events.find(event => event.id === ref.eventId)?.data.nativeType === 'execution/wait-start'))
  assert.equal(batch.hierarchyContext.coverage.complete, true)
})

test('SDK admission gaps remain scoped public coverage without inventing a pending child', t => {
  const f = boundary(t)
  f.collector.observe('subagent/wait-gap', { parent: f.parent, reason: 'audit-capacity', timestamp: f.now() })
  const batch = f.collector.batch()
  assert.equal(batch.hierarchyContext.waitBindings?.length ?? 0, 0)
  assert.ok(batch.hierarchyContext.coverage.branchGaps.some(gap => gap.sessionId === 'parent' && gap.reason.includes('audit-capacity')))
  assert.equal(batch.tasks.find(task => task.id === 'parent').status, 'running')
})

test('late closed-parent audits do not poison another current native lifecycle', t => {
  const f = boundary(t)
  f.collector.observe('subagent/wait-start', f.info('old-wait'))
  f.collector.observe('agent/disposed', { agent: f.parent })
  const before = f.collector.batch().gaps
  f.collector.observe('subagent/wait-end', { ...f.info('old-wait'), outcome: 'scope-disposed', operationSettled: false })
  f.collector.observe('subagent/wait-caller-end', { parent: f.parent, execution: {}, waitId: 'old-wait', timestamp: f.now() })
  assert.deepEqual(f.collector.batch().gaps, before)
})


test('parallel callers retain independent start/end proof after acknowledgement and invalidate on a new request', t => {
  const f = boundary(t); f.request()
  const executions = ['a', 'b'].map(callId => ({ agent: f.parent, token: {}, callId, name: 'read', arguments: {} }))
  for (const execution of executions) {
    f.collector.observe('tools/execute', { exec: execution })
    f.collector.observe('subagent/wait-start', f.info(execution.callId))
    f.collector.observe('subagent/wait-caller-start', { parent: f.parent, execution, waitId: execution.callId, timestamp: f.now() })
  }
  let batch = f.collector.batch()
  assert.equal(batch.hierarchyContext.waitBindings.length, 2)
  const firstStart = batch.events.find(event => event.data.nativeType === 'subagent/wait-start' && event.data.waitId === 'b')
  f.collector.acknowledge(batch.events.map(event => event.id), batch.tasks)
  f.advance(1)
  f.collector.observe('subagent/wait-targets', f.info('b'))
  f.collector.observe('subagent/wait-caller-end', { parent: f.parent, execution: executions[0], waitId: 'a', timestamp: f.now() })
  batch = f.collector.batch()
  assert.equal(batch.hierarchyContext.waitBindings.find(wait => wait.waitId === 'a').phase, 'ended')
  assert.equal(batch.hierarchyContext.waitBindings.find(wait => wait.waitId === 'b').phase, 'started')
  assert.equal(batch.events.find(event => event.id === firstStart.id).capturedAt, firstStart.capturedAt)
  assert.ok(batch.hierarchyContext.waitBindings.every(wait => wait.evidenceRefs.every(ref => batch.events.some(event => event.id === ref.eventId))))
  f.request(f.parent, 'new-request', 2)
  assert.ok(!f.collector.batch().hierarchyContext.waitBindings.some(wait => wait.waitId === 'b'))
})

test('exact execution identity, idle parent and unknown descendant run each refuse caller exemption', t => {
  for (const kind of ['copied-execution', 'idle', 'unknown-run', 'missing-sequence']) {
    const f = boundary(t); f.request()
    const execution = { agent: f.parent, token: {}, callId: kind, name: 'read', arguments: {} }
    f.collector.observe('tools/execute', { exec: execution })
    const info = f.info(kind)
    if (kind === 'idle') { f.parent.status = 'idle'; info.parentStatusAtStart = 'idle'; f.collector.observe('agent/status', { agent: f.parent, status: 'idle' }) }
    if (kind === 'unknown-run') info.targets[0].runId = 'never-published'
    if (kind === 'missing-sequence') delete info.parentSeqAtStart
    f.collector.observe('subagent/wait-start', info)
    f.collector.observe('subagent/wait-caller-start', { parent: f.parent, execution: kind === 'copied-execution' ? { ...execution } : execution, waitId: kind, timestamp: f.now() })
    assert.equal(f.collector.batch().hierarchyContext.waitBindings?.length ?? 0, 0, kind)
  }
})

test('wait records, memberships and gap records stay bounded and first-capture expiry never renews a join', t => {
  const f = boundary(t, { maxEvents: 20 }), first = f.info('first')
  f.collector.observe('subagent/wait-start', first)
  for (let i = 0; i < 40; i++) f.collector.observe('subagent/wait-start', { ...f.info('wait-' + i), targets: Array.from({ length: 20 }, () => f.info('target').targets[0]) })
  const bounded = f.collector.diagnostics()
  assert.ok(bounded.hierarchyWaits <= 20)
  assert.ok(bounded.hierarchyWaitTargets <= 20)
  assert.ok(f.collector.batch().gaps.some(gap => gap.includes('等待')))
  f.advance(24 * 60 * 60 * 1000 + 1)
  assert.equal(f.collector.diagnostics().hierarchyWaits, 0)
  assert.equal(f.collector.diagnostics().hierarchyWaitTargets, 0)
  f.collector.observe('subagent/wait-start', first)
  assert.equal(f.collector.diagnostics().hierarchyWaits, 0)
  assert.equal(f.collector.batch().hierarchyContext.waitBindings?.length ?? 0, 0)
})

test('a late end for an older native parent run cannot close or contaminate the new run wait', t => {
  const f = boundary(t), root = { id: 'root', status: 'running', session: { id: 'root', seq: 0, header: { id: 'root' } } }
  f.collector.observe('agent/created', { agent: root })
  f.collector.observe('subagent/start', { parent: root, child: f.parent, activation: { id: 'parent', runId: 'parent-old' }, capturedAt: f.now() })
  f.collector.observe('subagent/wait-start', f.info('old-run-wait'))
  f.collector.observe('subagent/start', { parent: root, child: f.parent, activation: { id: 'parent', runId: 'parent-new' }, capturedAt: f.now() })
  f.request()
  const execution = { agent: f.parent, token: {}, callId: 'new-caller', name: 'read', arguments: {} }
  f.collector.observe('tools/execute', { exec: execution })
  f.collector.observe('subagent/wait-start', f.info('new-run-wait'))
  f.collector.observe('subagent/wait-caller-start', { parent: f.parent, execution, waitId: 'new-run-wait', timestamp: f.now() })
  const before = f.collector.batch()
  f.collector.observe('subagent/wait-end', { ...f.info('old-run-wait'), operationSettled: true })
  f.collector.observe('subagent/wait-caller-end', { parent: f.parent, execution, waitId: 'old-run-wait', timestamp: f.now() })
  const after = f.collector.batch()
  assert.deepEqual(after.hierarchyContext.waitBindings, before.hierarchyContext.waitBindings)
  assert.deepEqual(after.hierarchyContext.coverage, before.hierarchyContext.coverage)
  assert.deepEqual(after.gaps, before.gaps)
  assert.ok(after.hierarchyContext.waitBindings.some(wait => wait.waitId === 'new-run-wait' && wait.phase === 'started'))
})

test('native first-capture object/string arguments keep the same bounded prompt and command without later rewriting', t => {
  for (const [form, argument] of [['object', { prompt: 'Inspect only.' }], ['string', JSON.stringify({ prompt: 'Inspect only.' })], ['command', JSON.stringify({ command: 'read fixture only' })]]) {
    const f = boundary(t), callId = 'native-' + form
    f.collector.observe('session/event', { session: f.parent.session, event: { type: 'tool/call', seq: 0, time: f.now(), data: { callId, name: 'fixed_read', arguments: argument } } })
    const first = f.collector.batch().events.find(event => event.kind === 'tool-start')
    assert.equal(first.text, form === 'command' ? 'read fixture only' : 'Inspect only.')
    assert.equal(first.data.command, form === 'command' ? 'read fixture only' : undefined)
    f.advance(100)
    f.collector.observe('tools/execute', { exec: { agent: f.parent, token: {}, callId, name: 'fixed_read', arguments: { prompt: 'Later text must not replace first capture.' } } })
    const retained = f.collector.batch().events.filter(event => event.kind === 'tool-start')
    assert.equal(retained.length, 1)
    assert.equal(retained[0].id, first.id)
    assert.equal(retained[0].capturedAt, first.capturedAt)
    assert.equal(retained[0].text, first.text)
  }
})

test('invalid, over-budget, sensitive and old-lifecycle native arguments cannot become fresh action or permission evidence', t => {
  for (const [kind, argument, gap] of [['invalid', '{invalid', 'JSON'], ['over-budget', JSON.stringify({ prompt: 'x'.repeat(24_001) }), '预算'], ['sensitive', JSON.stringify({ prompt: 'Authorization: Bearer secret-native-test-token' }), '认证']]) {
    const f = boundary(t)
    f.collector.observe('session/event', { session: f.parent.session, event: { type: 'tool/call', seq: 0, time: f.now(), data: { callId: kind, name: 'fixed_read', arguments: argument } } })
    const batch = f.collector.batch(), event = batch.events.find(event => event.kind === 'tool-start')
    assert.equal(event.text, '', kind)
    assert.ok(batch.tasks.find(task => task.id === 'parent').gaps.some(reason => reason.includes(gap)), kind)
    assert.ok(!JSON.stringify(batch).includes('secret-native-test-token'))
    assert.equal(event.authorizationEligible, undefined)
  }
  const f = boundary(t)
  f.collector.observe('agent/disposed', { agent: f.parent })
  const before = f.collector.batch()
  f.collector.observe('session/event', { session: f.parent.session, event: { type: 'tool/call', seq: 0, time: f.now(), data: { callId: 'old-life', name: 'fixed_read', arguments: JSON.stringify({ prompt: 'Old task must not resume.' }) } } })
  assert.deepEqual(f.collector.batch(), before)
})
