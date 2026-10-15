import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createCore } from '../src/core/index.mjs'

const checks = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait']
const task = { id: 'actor', lifecycleId: 'life', sessionId: 'root', title: 'Actor', status: 'running' }
const event = (id, kind, capturedAt, data = {}, owner = task) => ({ id, kind, taskId: owner.id, taskLifecycleId: owner.lifecycleId, capturedAt, occurredAt: capturedAt, text: id, data })
const result = (input, status = 'clear', evidenceId = input.events.at(-1).id, recovery = false) => ({ summary: { currentTask: input.task.currentRequest, currentState: 'observed', recentProgress: '', attention: '' }, checks: checks.map(id => ({ id, status: id === 'request_drift' ? status : 'unknown', reason: id === 'request_drift' ? 'scope judgment' : 'No trigger in complete scope', evidenceIds: id === 'request_drift' ? [evidenceId] : [], ...(id === 'request_drift' && recovery && input.task.activeAlerts?.[0] ? { recoveryFor: input.task.activeAlerts[0].id, recoveryEvidenceIds: [evidenceId] } : {}), applicability: { state: id === 'request_drift' ? 'triggered' : 'not-triggered', reason: 'Complete retained scope', evidenceIds: [input.events[0].id] } })), coverage: input.coverage })
async function settled(f) {
  for (let attempt = 0; attempt < 100; attempt++) {
    const actor = await f.actor()
    if (['cooldown', 'failed'].includes(actor.analysis.state)) return actor
    await new Promise(resolve => setTimeout(resolve, 2))
  }
  assert.fail('Own controlled analysis did not settle')
}
async function fixture(t, options = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-gs-status-'))
  let time = 1_800_000_000_000
  const core = await createCore({ dataDir, now: () => time, leaseMs: 172_800_000, ...options })
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }) })
  const source = await core.registerSource({ registrationId: 'gs-status', hostRunId: 'run', name: 'Own fixture' })
  await core.ingest(source, { tasks: [task], events: [event('request', 'input', time, { inputPhase: 'consumed', inputSequence: 1 })] })
  return { core, source, now: () => time, advance: ms => time += ms, async actor() { return (await core.snapshot()).tasks.find(value => value.id === task.id) } }
}

test('GS observes initial inactivity and the exact sixty-second silence boundary independently from analysis', async t => {
  const f = await fixture(t)
  assert.equal((await f.actor()).gs.light, 'yellow')
  assert.equal((await f.actor()).lastActivity, null, 'user input is not a visible execution activity timestamp')
  assert.deepEqual(await f.core.claimNotifications(f.source), [])
  await f.core.ingest(f.source, { events: [event('visible-output', 'output', f.advance(1))] })
  assert.equal((await f.actor()).gs.light, 'gray', 'activity alone does not prove semantic normality')
  f.advance(59_999)
  assert.equal((await f.actor()).gs.light, 'gray')
  f.advance(1)
  assert.equal((await f.actor()).gs.light, 'yellow')
  await f.core.heartbeat(f.source)
  await f.core.ingest(f.source, { tasks: [{ ...task, title: 'Metadata update' }], events: [event('poll', 'status', f.now()), event('history-output', 'output', f.now(), { history: true })] })
  assert.equal((await f.actor()).gs.light, 'yellow', 'heartbeat, metadata, poll and historical capture are not execution activity')
  assert.equal((await f.actor()).lastActivity, f.now() - 60_000)
  assert.doesNotMatch((await f.actor()).activity.summary, /history-output/)
  await f.core.ingest(f.source, { events: [event('tool-begin', 'tool-start', f.advance(1), { callId: 'read' })] })
  assert.equal((await f.actor()).gs.light, 'gray')
  f.advance(60_000)
  assert.equal((await f.actor()).gs.light, 'yellow')
  await f.core.ingest(f.source, { events: [event('tool-result', 'tool-result', f.advance(1), { callId: 'read', exitCode: 0 })] })
  assert.equal((await f.actor()).gs.light, 'gray')
  assert.deepEqual(await f.core.claimNotifications(f.source), [])
})

test('GS retains a grounded red judgment through activity and unknown decisions until a new result supports recovery', async t => {
  let choice = 'attention', citation = 'request', fail = false, recovery = false
  const f = await fixture(t, { analyzer: { async analyze(input) { if (fail) throw new Error('controlled model failure'); return result(input, choice, citation, recovery) } } })
  assert.equal((await settled(f)).gs.light, 'red', 'the first grounded round is sufficient')
  f.advance(3_000)
  assert.equal((await f.core.claimNotifications(f.source)).length, 1)
  await f.core.ingest(f.source, { events: [event('working', 'output', f.advance(1))] })
  assert.equal((await f.actor()).gs.light, 'red')
  choice = 'unknown'; f.advance(60_000); await f.core.snapshot()
  assert.equal((await settled(f)).gs.light, 'red', 'unknown is not recovery and cannot retract retained red evidence')
  fail = true; f.advance(60_000)
  await f.core.ingest(f.source, { events: [event('fresh-diagnostic', 'status', f.now())] })
  assert.equal((await settled(f)).gs.light, 'red', 'a failed current model call cannot repair retained red evidence')
  fail = false
  choice = 'clear'; citation = 'working'; f.advance(60_000); await f.core.snapshot()
  assert.equal((await settled(f)).gs.light, 'red', 'an activity response is not a new result')
  citation = 'unrelated-read'
  await f.core.ingest(f.source, { events: [event(citation, 'tool-result', f.advance(1), { callId: 'pwd', exitCode: 0 })] })
  f.advance(60_000); await f.core.snapshot()
  assert.equal((await settled(f)).gs.light, 'red', 'a clear answer plus an unrelated successful result does not establish recovery of the original issue')
  citation = 'fixed-result'
  recovery = true
  await f.core.ingest(f.source, { events: [event(citation, 'tool-result', f.advance(1), { callId: 'verify', exitCode: 0 })] })
  f.advance(60_000); await f.core.snapshot()
  let actor = await settled(f)
  assert.equal(actor.alerts[0].status, 'recovered')
  assert.equal(actor.gs.light, 'yellow', 'recovery and silence are separate observations')
  await f.core.ingest(f.source, { events: [event('continuing', 'output', f.advance(1))] })
  assert.equal((await f.actor()).gs.light, 'green')
  fail = true; f.advance(60_000); await f.core.snapshot()
  actor = await settled(f)
  assert.equal(actor.gs.light, 'yellow')
  assert.ok(actor.analysis.checks.every(check => check.judgmentState === 'failed'))
  await f.core.ingest(f.source, { events: [event('after-failure', 'output', f.advance(1))] })
  assert.equal((await f.actor()).gs.light, 'gray')
})

test('only a current native user wait exempts silence and its long wait while preserving other alerts and gaps', async t => {
  const f = await fixture(t)
  await f.core.ingest(f.source, { events: [event('claim', 'output', f.advance(1), { toolName: 'ask_user_question', waitingUser: true })] })
  f.advance(60_000)
  assert.equal((await f.actor()).gs.light, 'yellow', 'tool names and parent declarations do not prove a wait')
  const askedAt = f.advance(1)
  await f.core.ingest(f.source, { events: [event('asked', 'status', askedAt, { nativeType: 'approval/asked', approvalId: 'approval-one', callId: 'dangerous-tool' })] })
  f.advance(300_000)
  let actor = await f.actor()
  assert.equal(actor.gs.light, 'green')
  assert.equal(actor.gs.label, '等待你')
  assert.equal(actor.gs.waiting.durationMs, 300_000)
  assert.deepEqual(actor.gs.waiting.evidenceIds, ['asked'])
  await f.core.ingest(f.source, { events: [event('unrelated-decision', 'status', f.now(), { nativeType: 'approval/decided', approvalId: 'another-approval' })] })
  assert.equal((await f.actor()).gs.label, '等待你')
  await f.core.ingest(f.source, { events: [event('decided', 'status', f.now(), { nativeType: 'approval/decided', approvalId: 'approval-one' })] })
  assert.equal((await f.actor()).gs.light, 'yellow')
  await f.core.ingest(f.source, { events: [event('question-open', 'status', f.now(), { nativeType: 'user-questions/request-start', waitId: 'question-one', callId: 'question-call' })] })
  assert.equal((await f.actor()).gs.label, '等待你')
  await f.core.ingest(f.source, { events: [event('question-returned-pending', 'status', f.now(), { nativeType: 'user-questions/request-end', waitId: 'question-one' })] })
  assert.equal((await f.actor()).gs.light, 'yellow', 'still answerable timed questions do not leave the actor blocked')
  await f.core.ingest(f.source, { events: [event('asked-again', 'status', f.now(), { nativeType: 'approval/asked', approvalId: 'approval-two' })], gaps: ['Lost middle events'] })
  assert.equal((await f.actor()).gs.light, 'gray', 'critical observation gaps are retained through a proved wait')
  await f.core.unregisterSource(f.source)
  assert.equal((await f.actor()).gs.light, 'gray')
  assert.equal((await f.actor()).gs.waiting, null)
})

test('proved user waiting suppresses only the corresponding long-wait notice and resumes ordinary rules when it closes', async t => {
  let otherAlert = false
  const f = await fixture(t, { analyzer: { async analyze(input) {
    const value = result(input, otherAlert ? 'attention' : 'clear', 'request')
    if (input.events.some(event => event.id === 'start')) value.checks[5] = { id: 'long_wait', status: 'attention', reason: 'Matched outstanding tool', evidenceIds: ['start'], applicability: { state: 'triggered', reason: 'Retained wait', evidenceIds: ['start'] } }
    return value
  } } })
  await settled(f)
  await f.core.ingest(f.source, { events: [event('start', 'tool-start', f.advance(1), { callId: 'approval-call' }), event('asked', 'status', f.now(), { nativeType: 'approval/asked', approvalId: 'approval', callId: 'approval-call' })] })
  f.advance(60_000); await f.core.snapshot()
  assert.equal((await settled(f)).gs.label, '等待你')
  assert.deepEqual(await f.core.claimNotifications(f.source), [])
  otherAlert = true; f.advance(60_000)
  await f.core.ingest(f.source, { events: [event('drift-diagnostic', 'status', f.now())] })
  assert.equal((await settled(f)).gs.light, 'red')
  f.advance(3_000)
  const [notice] = await f.core.claimNotifications(f.source)
  assert.deepEqual(notice.checkIds, ['request_drift'])
  await f.core.ingest(f.source, { events: [event('decided', 'status', f.now(), { nativeType: 'approval/decided', approvalId: 'approval' })] })
  assert.equal((await f.actor()).gs.light, 'red')
  assert.equal((await f.actor()).gs.waiting, null)
})

test('request replacement, ending, disconnection and expiry invalidate red evidence with distinct public reasons', async t => {
  for (const boundary of ['request', 'ended', 'disconnected', 'expired']) {
    const f = await fixture(t, { analyzer: { async analyze(input) { return result(input, 'attention', 'request') } } })
    assert.equal((await settled(f)).gs.light, 'red')
    if (boundary === 'request') await f.core.ingest(f.source, { events: [event('replacement', 'input', f.advance(1))] })
    if (boundary === 'ended') await f.core.ingest(f.source, { tasks: [{ ...task, status: 'idle' }] })
    if (boundary === 'disconnected') await f.core.unregisterSource(f.source)
    if (boundary === 'expired') { await f.core.pause(); f.advance(86_400_001) }
    const actor = await f.actor()
    assert.equal(actor.alerts.find(alert => alert.evidenceIds?.includes('request') || alert.checkId === 'request_drift')?.status ?? 'expired-record-removed', { request: 'superseded', ended: 'ended', disconnected: 'source-disconnected', expired: 'expired-record-removed' }[boundary])
    assert.ok(!actor.alerts.some(alert => alert.status === 'recovered'), boundary)
    if (boundary === 'disconnected') assert.equal(actor.gs.light, 'gray')
    if (boundary === 'ended') assert.equal(actor.gs.light, null)
    if (boundary !== 'disconnected') assert.deepEqual(await f.core.claimNotifications(f.source), [])
  }
})

test('equal-clock consumed requests use their actual anchor so earlier same-clock activity cannot clear initial silence', async t => {
  const f = await fixture(t)
  await f.core.ingest(f.source, { events: [event('old-same-clock-output', 'output', f.now()), event('next-request', 'input', f.now(), { inputPhase: 'consumed', inputSequence: 2 })] })
  const actor = await f.actor()
  assert.equal(actor.currentRequest, 'next-request')
  assert.equal(actor.gs.light, 'yellow')
  assert.equal(actor.gs.silence.initial, true)
  assert.equal(actor.lastActivity, null)
})

test('proved waiting can be green before semantic completion but cannot hide a completed unknown or failed check', async t => {
  for (const outcome of ['unrun', 'unknown', 'failed']) {
    let finish
    const analyzer = outcome === 'unrun' ? { analyze: input => new Promise(resolve => { finish = () => resolve(result(input, 'clear', 'request')) }) } : { async analyze(input) { if (outcome === 'failed') throw new Error('controlled call failure'); return result(input, 'unknown', 'request') } }
    const f = await fixture(t, { analyzer })
    if (outcome !== 'unrun') await settled(f)
    await f.core.ingest(f.source, { events: [event('wait-user', 'status', f.advance(1), { nativeType: 'approval/asked', approvalId: 'approval' })] })
    const actor = await f.actor()
    assert.equal(actor.gs.light, outcome === 'unrun' ? 'green' : 'gray', outcome)
    assert.equal(actor.gs.waiting.kind, 'user')
    assert.equal(actor.gs.waiting.semanticState, outcome === 'unrun' ? 'pending' : 'insufficient')
    if (finish) { finish(); await settled(f) }
  }
})

test('a new consumed request waiting during its inherited cooldown does not claim the previous request completed semantics', async t => {
  const f = await fixture(t, { analyzer: { async analyze(input) { return result(input, 'clear') } } })
  await settled(f)
  await f.core.ingest(f.source, { events: [event('new-scope', 'input', f.advance(1)), event('new-approval', 'status', f.advance(1), { nativeType: 'approval/asked', approvalId: 'new-approval' })] })
  const actor = await f.actor()
  assert.equal(actor.gs.light, 'green')
  assert.equal(actor.gs.waiting.semanticState, 'pending')
  assert.match(actor.gs.reason, /语义尚待完成/)
})
