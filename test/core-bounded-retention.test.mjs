import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { createCore } from '../src/core/index.mjs'
import { createAnalyzer } from '../src/analysis/index.mjs'

const DAY = 86_400_000
const ref = (taskId, eventId) => ({ taskId, lifecycleId: 'life', eventId })
async function until(predicate, label) {
  const deadline = Date.now() + 10_000
  while (!await predicate()) { assert.ok(Date.now() < deadline, label); await delay(5) }
}

test('graph and grouped notification replay stays bounded, then expired proof and reuse state are reclaimed without renewing active identities', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'gs-bounded-retention-'))
  let clock = Date.now(), core, source, llmCalls = 0, jevCalls = 0, largestRequest = 0, modelWork = 0, completionStarted, completionElapsed, completionReads = 0
  const samples = []
  const models = createServer(async (request, response) => {
    modelWork++
    try {
      let raw = ''; for await (const chunk of request) raw += chunk
      const body = JSON.parse(raw)
      response.setHeader('content-type', 'application/json')
      if (request.url === '/v1/chat/completions') {
        llmCalls++; largestRequest = Math.max(largestRequest, raw.length)
        response.end(JSON.stringify({ model: body.model, choices: [{ finish_reason: 'stop', message: { content: '{"selectedOptionIds":[],"gaps":[]}' } }] }))
      } else { jevCalls++; response.end(JSON.stringify({ model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: id === 'request_drift' ? 'attention' : 'unknown' }])) })) }
    } finally { modelWork-- }
  })
  t.after(async () => {
    const failures = []; let counts
    try {
      for (const cleanup of [async () => { if (core && !(await core.diagnostics()).closed) await core.pause() }, async () => { if (source && core && !(await core.diagnostics()).closed) await core.unregisterSource(source) }, () => core?.close()]) { try { await cleanup() } catch (error) { failures.push(error) } }
    } finally {
      try { if (models.listening) await new Promise(resolve => models.close(resolve)) } catch (error) { failures.push(error) }
    }
    try {
      if (core) {
        await until(async () => (await core.diagnostics()).runtime.unsettledAnalysisCalls === 0, 'cancelled HTTP calls actually settle during cleanup')
        counts = await core.diagnostics()
        assert.equal(counts.temporary.timers.total, 0); assert.equal(counts.temporary.queueKeys, 0)
      }
      assert.equal(modelWork, 0)
    } catch (error) { failures.push(error) }
    finally {
      try { await rm(dataDir, { recursive: true, force: true }) } catch (error) { failures.push(error) }
      t.diagnostic(`cleanup: ${JSON.stringify({ coreCalls: counts?.runtime.unsettledAnalysisCalls ?? null, coreTimers: counts?.temporary.timers.total ?? null, queueKeys: counts?.temporary.queueKeys ?? null, modelWork, modelListening: models.listening, errors: failures.map(error => error.message) })}`)
      t.diagnostic(`resource-pressure: ${JSON.stringify({ runtime: process.version, gc: Boolean(global.gc), nodes: 100, modelCalls: llmCalls, jevCalls, completion: { elapsedMs: completionElapsed ?? (completionStarted == null ? null : Date.now() - completionStarted), diagnosticsReads: completionReads }, largestRequest, replays: 100, samples, memoryAssessment: 'actual RSS/heap/system footprint samples recorded; no unapproved fixed memory threshold' })}`)
    }
    if (failures.length) throw new AggregateError(failures, 'Core/model resource cleanup failed')
  })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  const baseUrl = `http://127.0.0.1:${models.address().port}`
  core = await createCore({ dataDir, now: () => clock, leaseMs: DAY * 2, analyzer: createAnalyzer({ now: () => clock, llm: { baseUrl, model: 'retention-extractor' }, jev: { baseUrl, model: 'retention-judge' } }) })
  source = await core.registerSource({ registrationId: 'retention', hostRunId: 'host', name: 'PRIVATE RETENTION SOURCE' })
  const roots = Array.from({ length: 4 }, (_, index) => `root-${index}`), children = Array.from({ length: 96 }, (_, index) => `node-${index}`), ids = [...roots, ...children]
  const parent = id => roots[Number(id.split('-')[1]) % 4]
  const event = (taskId, id, kind, text, data = {}) => ({ id, taskId, taskLifecycleId: 'life', kind, text, data, capturedAt: clock - 10_000, occurredAt: null })
  const batch = { tasks: ids.map(id => ({ id, lifecycleId: 'life', sessionId: id, title: 'PRIVATE TASK TITLE', status: roots.includes(id) ? 'idle' : 'running' })), events: [], hierarchyContext: { schemaVersion: 1, revision: 1, sessions: ids.map(sessionId => ({ sessionId })), activations: [], relations: [], instructionBindings: [], coverage: { complete: true, branchGaps: [] } } }
  for (const id of roots) {
    batch.events.push(event(id, `${id}-state`, 'status', '', { nativeType: 'agent/status', status: 'idle' }), event(id, `${id}-claim`, 'input', 'Inspect repository without modifying files.', { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: `${id}-message` }), event(id, `${id}-request`, 'input', 'Inspect repository without modifying files.', { nativeType: 'user/message', inputPhase: 'consumed', inputOrigin: 'unknown', sourceKind: 'user', inherited: false, messageId: `${id}-message` }))
    batch.hierarchyContext.activations.push({ taskId: id, lifecycleId: 'life', sessionId: id, runId: null, state: 'ended', stateRefs: [ref(id, `${id}-state`)] })
    batch.hierarchyContext.instructionBindings.push({ taskId: id, lifecycleId: 'life', inputRef: ref(id, `${id}-request`), phase: 'consumed', origin: 'user-entry', originRefs: [ref(id, `${id}-claim`)], ancestorLimitRefs: [] })
  }
  for (const id of children) {
    const root = parent(id)
    batch.events.push(event(id, `${id}-state`, 'status', '', { nativeType: 'agent/status', status: 'running' }), event(root, `${id}-catalog`, 'status', '', { nativeType: 'subagent/catalog', childSessionId: id }), event(root, `${id}-native-start`, 'status', '', { nativeType: 'subagent/start', childSessionId: id, runId: `run-${id}` }), event(id, `${id}-request`, 'input', `Inspect only for ${id}.`, { nativeType: 'user/message', inputPhase: 'consumed', inputOrigin: 'delegated', sourceKind: 'user', inherited: false }), event(id, `${id}-action`, 'output', `Unrelated change for ${id}.`), event(id, `${id}-tool`, 'tool-start', 'Synthetic fixed read', { callId: `${id}-call`, toolName: 'fixed-read' }), event(id, `${id}-result`, 'tool-result', 'Synthetic fixed result', { callId: `${id}-call`, exitCode: 0, sourceVersion: 'fixed-v1', validationScope: id }))
    batch.hierarchyContext.activations.push({ taskId: id, lifecycleId: 'life', sessionId: id, runId: `run-${id}`, state: 'running', stateRefs: [ref(id, `${id}-state`), ref(root, `${id}-native-start`)] })
    batch.hierarchyContext.relations.push({ parentSessionId: root, childSessionId: id, kind: 'delegation', evidenceRefs: [ref(root, `${id}-catalog`)] })
    batch.hierarchyContext.instructionBindings.push({ taskId: id, lifecycleId: 'life', inputRef: ref(id, `${id}-request`), phase: 'consumed', origin: 'delegated', originRefs: [ref(root, `${id}-catalog`)], ancestorLimitRefs: [ref(root, `${root}-request`)] })
  }
  const sample = async replayCount => {
    global.gc?.()
    const value = await core.diagnostics()
    const system = process.platform === 'darwin' ? JSON.parse(execFileSync('python3', [fileURLToPath(new URL('../scripts/memory-footprint.py', import.meta.url)), String(process.pid)], { encoding: 'utf8', timeout: 2000 })) : { metric: 'unavailable' }
    if (process.platform === 'darwin') assert.ok(system.physicalFootprint > 0)
    samples.push({ replayCount, rss: value.memory.rss, heapUsed: value.memory.heapUsed, system, retained: value.retained, temporary: value.temporary, calls: value.runtime.unsettledAnalysisCalls })
  }
  await core.ingest(source, batch)
  completionStarted = Date.now()
  await until(async () => {
    if (jevCalls !== 96) return false
    completionReads++
    const counts = await core.diagnostics()
    return counts.runtime.unsettledAnalysisCalls === 0 && counts.temporary.queueKeys === 0
  }, 'all 96 real HTTP descendant analyses complete')
  completionElapsed = Date.now() - completionStarted
  const completed = (await core.snapshot()).tasks.filter(task => task.status === 'running')
  assert.equal(completed.length, 96)
  assert.ok(completed.every(task => task.analysis.completedAt != null), 'all 96 completed results remain publicly visible')
  const before = await core.diagnostics(), liveKey = (await core.snapshot()).tasks.find(task => task.id === 'node-95').key
  const materialAt = (await core.snapshot()).sources[0].hierarchyMaterialAt
  assert.equal(before.retained.hierarchyActivations, 100)
  assert.equal(before.retained.nativeRunBindings, 96)
  assert.equal(before.retained.taskContextFingerprints, 100)
  assert.equal(before.retained.notificationMemberships, 96)
  assert.equal(before.temporary.pendingNotificationWindows, 4)
  assert.equal(llmCalls, 96)
  assert.ok(largestRequest <= 24_000)
  await sample(0)
  for (let index = 1; index <= 100; index++) {
    clock += 10
    batch.hierarchyContext.revision++
    await core.ingest(source, batch)
    await core.gsSummary({ sourceKey: source, sessionId: roots[index % 4] })
    if (index % 20 === 0) await sample(index)
  }
  assert.equal(llmCalls, 96, 'replay and state reads never re-run unchanged successful HTTP judgments')
  assert.equal((await core.snapshot()).sources[0].hierarchyMaterialAt, materialAt, 'revision-only replays do not renew metadata capture time')
  assert.deepEqual((await core.diagnostics()).retained, before.retained)
  clock += 3000
  const notices = await core.claimNotifications(source)
  assert.equal(notices.length, 4)
  for (const notice of notices) { assert.equal(notice.members.length, 24); await core.ackNotification(notice.id, { status: 'submitted' }) }
  await core.pause()
  for (const id of children.filter(id => id !== 'node-95')) {
    batch.tasks.find(task => task.id === id).status = 'disposed'
    batch.events.push(event(id, `${id}-ended`, 'status', '', { nativeType: 'agent/disposed', status: 'disposed' }))
    const activation = batch.hierarchyContext.activations.find(item => item.taskId === id)
    activation.state = 'ended'; activation.stateRefs = [ref(id, `${id}-ended`), ref(parent(id), `${id}-native-start`)]
  }
  batch.hierarchyContext.revision++; await core.ingest(source, batch)
  const guardedIdentities = (await core.diagnostics()).retained.hierarchyEvidenceIdentities
  clock += DAY + 1
  await core.heartbeat(source)
  const expired = await core.snapshot(), active = expired.tasks.find(task => task.id === 'node-95')
  assert.equal(active.key, liveKey)
  assert.equal(active.gs.light, 'gray', 'expired native execution proof cannot manufacture a new initial-silence episode')
  const counts = await core.diagnostics()
  assert.equal(counts.retained.tasks, 1)
  assert.equal(counts.retained.events, 0)
  assert.equal(counts.retained.hierarchyActivations, 1)
  assert.equal(counts.retained.hierarchyReferences, 0)
  assert.equal(counts.retained.analysisReuseKeys, 0)
  assert.equal(counts.retained.taskContextFingerprints, 0)
  assert.equal(counts.retained.notificationBindingEvidenceRefs, 0)
  assert.equal(counts.retained.notifications, 0)
  assert.equal(counts.retained.notificationMemberships, 0)
  assert.equal(counts.retained.hierarchyEvidenceIdentities, guardedIdentities, 'first-capture guards include the genuine ending events and must not be erased to make body replay fresh')
  const replay = structuredClone(batch); replay.hierarchyContext.revision++
  for (const event of replay.events) event.capturedAt = clock
  await assert.rejects(core.ingest(source, replay), /Expired hierarchy evidence cannot acquire a new first capture timestamp/)
  assert.equal((await core.diagnostics()).retained.events, 0)
  await sample(101)
})

test('supporting evidence expiry releases reuse keys even when the completed result is newer than its evidence', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'gs-earlier-evidence-expiry-'))
  let clock = Date.now()
  const core = await createCore({ dataDir, now: () => clock, leaseMs: DAY * 2, analyzer: { async analyze(input) {
    return { summary: { currentTask: 'fixed', currentState: 'fixed', recentProgress: '', attention: '' }, checks: ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait'].map(id => ({ id, status: 'unknown', reason: 'No conclusion', evidenceIds: [] })), coverage: input.coverage, quality: 'unvalidated' }
  } } })
  t.after(async () => {
    await core.close()
    const counts = await core.diagnostics()
    assert.equal(counts.runtime.unsettledAnalysisCalls, 0); assert.equal(counts.temporary.timers.total, 0)
    await rm(dataDir, { recursive: true, force: true })
    t.diagnostic('cleanup: {"coreCalls":0,"coreTimers":0}')
  })
  const source = await core.registerSource({ registrationId: 'earlier-evidence', hostRunId: 'own', name: 'Own source' })
  await core.ingest(source, { tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Actor' }], events: [{ id: 'request', taskId: 'actor', taskLifecycleId: 'life', kind: 'input', text: 'Fixed request', occurredAt: null, capturedAt: clock - DAY + 100 }] })
  await until(async () => (await core.snapshot()).tasks[0].analysis.completedAt != null, 'completed result is publicly visible')
  const before = (await core.snapshot()).tasks[0]
  assert.equal((await core.diagnostics()).retained.analysisReuseKeys, 2)
  clock += 101
  await core.heartbeat(source)
  const expired = (await core.snapshot()).tasks[0]
  assert.equal(expired.key, before.key)
  assert.equal(expired.status, 'running')
  assert.equal(expired.analysis.error.code, 'evidence_expired')
  assert.equal(expired.analysis.resultDisposition, 'invalid')
  assert.equal(expired.analysis.summary, null)
  assert.equal((await core.diagnostics()).retained.events, 0)
  assert.equal((await core.diagnostics()).retained.analysisReuseKeys, 0)
})
