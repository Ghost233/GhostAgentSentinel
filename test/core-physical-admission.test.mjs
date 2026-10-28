import { withModelSummary } from './helpers/model-summary.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { createCore } from '../src/core/index.mjs'
import { createAnalyzer } from '../src/analysis/index.mjs'

async function until(predicate, label) {
  const deadline = Date.now() + 3000
  while (!await predicate()) { assert.ok(Date.now() < deadline, label); await delay(5) }
}

async function fixture(t, { honorAbort = false } = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), 'gs-physical-admission-'))
  const calls = [], held = [], requests = []
  let core, source, clock = Date.now(), liveHttp = 0, peakHttp = 0, draining = false
  const models = createServer(async (request, response) => {
    if (request.method === 'GET') { response.writeHead(404, { 'content-type': 'application/json' }); response.end('{"error":{"code":"not_found"}}'); return; }
    liveHttp++; peakHttp = Math.max(peakHttp, liveHttp)
    let raw = ''; for await (const chunk of request) raw += chunk
    const body = JSON.parse(raw); requests.push({ path: request.url, body })
    response.setHeader('content-type', 'application/json')
    if (request.url === '/v1/chat/completions') {
      const job = { body, finished: false, closedBeforeFinished: false }
      response.once('close', () => { if (!job.finished) job.closedBeforeFinished = true })
      const reply = () => { if (!job.finished) { job.finished = true; liveHttp--; if (!response.destroyed) response.end(JSON.stringify({ model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds: [], gaps: [] })) } }] })) } }
      held.push({ ...job, reply, get finished() { return job.finished }, get closedBeforeFinished() { return job.closedBeforeFinished } })
      if (draining) reply()
    } else { liveHttp--; response.end(JSON.stringify({ model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: id === 'request_drift' ? 'clear' : 'unknown' }])) })) }
  })
  t.after(async () => {
    const failures = [], cleanup = {}
    for (const step of [
      async () => { draining = true; if (core && !(await core.diagnostics()).closed) await core.pause(); for (const request of held) request.reply(); if (core) await until(async () => (await core.diagnostics()).runtime.cancelledUnsettledRequests === 0, 'ignored-abort analyzer calls finish naturally') },
      async () => { if (source && !(await core.diagnostics()).closed) await core.unregisterSource(source); await core?.close(); if (core) { const value = await core.diagnostics(); cleanup.core = { activeAnalyses: value.runtime.activeAnalyses, cancelledUnsettledRequests: value.runtime.cancelledUnsettledRequests, queueKeys: value.temporary.queueKeys, queueMemberships: value.temporary.queueMemberships, timers: value.temporary.timers.total }; assert.deepEqual(cleanup.core, { activeAnalyses: 0, cancelledUnsettledRequests: 0, queueKeys: 0, queueMemberships: 0, timers: 0 }) } },
      async () => { if (models.listening) await new Promise((resolve, reject) => models.close(error => error ? reject(error) : resolve())); cleanup.http = { listening: models.listening, liveHttp }; assert.deepEqual(cleanup.http, { listening: false, liveHttp: 0 }) },
      () => rm(dataDir, { recursive: true, force: true }),
    ]) { try { await step() } catch (error) { failures.push(error) } }
    t.diagnostic(`cleanup: ${JSON.stringify(cleanup)}`)
    if (failures.length) throw new AggregateError(failures, 'physical admission fixture cleanup failed')
  })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  const baseUrl = `http://127.0.0.1:${models.address().port}`
  const production = createAnalyzer({ llm: { baseUrl, model: 'admission-extractor' }, jev: { baseUrl, model: 'admission-judge' } })
  // This external signal adapter deliberately ignores Abort. Core business and
  // production extraction/JEV still execute; the actual HTTP exchange stays held.
  const analyzer = { analyze(input, options) { calls.push({ signal: options.signal, request: input.task.currentRequest }); return production.analyze(input, honorAbort ? options : undefined) } }
  core = await createCore({ dataDir, analyzer, now: () => clock, leaseMs: 172_800_000 })
  source = await core.registerSource({ registrationId: 'admission', hostRunId: 'host', name: 'Private admission source' })
  const observe = async (index, targetSource = source) => {
    clock++
    await core.ingest(targetSource, { tasks: [{ id: 'actor', lifecycleId: 'life', title: 'Private actor', status: 'running' }], events: [
      { id: `request-${index}`, kind: 'input', text: `Current request ${index}` }, { id: `action-${index}`, kind: 'output', text: `Inspecting current work ${index}` },
    ].map(event => ({ ...event, taskId: 'actor', taskLifecycleId: 'life', capturedAt: clock - 10_000, occurredAt: null })) })
  }
  return { core, source, calls, held, requests, observe, get liveHttp() { return liveHttp }, get peakHttp() { return peakHttp } }
}

test('consecutive request replacements wait for an ignored-abort real HTTP analysis to settle before admitting the latest request', async t => {
  const f = await fixture(t)
  await f.observe(0)
  await until(() => f.held.length === 1, 'the first actual extraction request is held')
  for (let index = 1; index <= 12; index++) await f.observe(index)
  assert.equal(f.calls[0].signal.aborted, true, 'cancellation still promptly seals the old round')
  assert.equal(f.calls.length, 1, 'an unsettled cancelled call still owns the single analysis admission slot')
  assert.equal(f.liveHttp, 1)
  const diagnostics = await f.core.diagnostics(), snapshot = await f.core.snapshot(), actor = snapshot.tasks[0]
  assert.equal(diagnostics.runtime.activeAnalyses, 0)
  assert.equal(diagnostics.runtime.cancelledUnsettledRequests, 1)
  assert.equal(diagnostics.runtime.unsettledAnalysisCalls, 1)
  assert.equal(diagnostics.runtime.analysisAdmissionLimit, 1)
  assert.equal(diagnostics.runtime.analysisAdmissionBlocked, true)
  assert.equal(diagnostics.temporary.queueKeys, 1)
  assert.equal(diagnostics.temporary.queueMemberships, 1)
  assert.equal(actor.currentRequest, 'Current request 12')
  assert.equal(actor.analysis.state, 'queued')
  assert.ok(Number.isFinite(actor.analysis.queuedAt))
  assert.deepEqual(snapshot.monitoring.admission, { limit: 1, unsettledCalls: 1, blocked: true })
  assert.equal(JSON.stringify(diagnostics).includes('Private'), false)

  f.held[0].reply()
  await until(() => f.held.length === 2, 'natural completion of the old HTTP call admits the coalesced current request')
  assert.equal(f.calls.length, 2)
  assert.equal(f.calls[1].request, 'Current request 12')
  assert.equal(f.peakHttp, 1, 'held HTTP exchanges never overlap despite twelve replacements')
  const pending = (await f.core.snapshot()).tasks[0]
  assert.equal(pending.analysis.completedAt, null, 'a sealed late result cannot become the current completed result')
  f.held[1].reply()
  await until(async () => (await f.core.snapshot()).tasks[0].analysis.summary?.currentTask === 'Current request 12', 'the current actual model result completes')
  const finished = await f.core.diagnostics()
  assert.equal(finished.runtime.unsettledAnalysisCalls, 0)
  assert.equal(finished.runtime.analysisAdmissionBlocked, false)
  assert.equal(finished.runtime.cancelledUnsettledRequests, 0)
  assert.equal(finished.temporary.queueKeys, 0)
  assert.equal((await f.core.snapshot()).tasks[0].gs.light, 'green')
})

test('production HTTP scope cancellation retains admission until the submitted response ends without treating abort as server completion', async t => {
  const f = await fixture(t, { honorAbort: true })
  await f.observe(0)
  await until(() => f.held.length === 1, 'the first production extraction request entered the controlled service')
  for (let index = 1; index <= 12; index++) await f.observe(index)
  await delay(25)
  assert.equal(f.calls[0].signal.aborted, true)
  assert.equal(f.held[0].finished, false, 'logical cancellation does not finish server work')
  assert.equal(f.calls.length, 1, 'the ordinary production analyzer must not reopen admission while its submitted old response is unfinished')
  assert.equal(f.liveHttp, 1)
  assert.equal((await f.core.diagnostics()).runtime.cancelledUnsettledRequests, 1)
  assert.equal((await f.core.snapshot()).tasks[0].analysis.state, 'queued')
  f.held[0].reply()
  await until(() => f.held.length === 2, 'the new scope starts only after its old submitted HTTP response ends')
  assert.equal(f.held[0].closedBeforeFinished, false)
  assert.equal(f.peakHttp, 1)
  assert.equal(f.calls[1].request, 'Current request 12')
  assert.equal((await f.core.snapshot()).tasks[0].analysis.completedAt, null)
  f.held[1].reply()
  await until(async () => (await f.core.snapshot()).tasks[0].analysis.summary?.currentTask === 'Current request 12', 'late old output remains sealed and the current result completes')
  t.diagnostic(`server-work: ${JSON.stringify({ enteredHeldRequests: f.held.length, finishedHeldRequests: f.held.filter(job => job.finished).length, peakUnfinishedRequests: f.peakHttp })}`)
})

test('a waiting different source retains FIFO ahead of repeated replacements once old submitted work finishes', async t => {
  const f = await fixture(t, { honorAbort: true })
  await f.observe(0); await until(() => f.held.length === 1, 'first source extraction entered')
  const other = await f.core.registerSource({ registrationId: 'other-admission', hostRunId: 'host', name: 'Other private source' })
  await f.observe(99, other)
  for (let index = 1; index <= 12; index++) await f.observe(index)
  assert.equal((await f.core.diagnostics()).temporary.queueKeys, 2)
  assert.equal(f.calls.length, 1)
  f.held[0].reply()
  await until(() => f.held.length === 2, 'the waiting different source is admitted first')
  assert.equal(f.calls[1].request, 'Current request 99')
  f.held[1].reply()
  await until(() => f.held.length === 3, 'the replacement source is admitted after its FIFO predecessor')
  assert.equal(f.calls[2].request, 'Current request 12')
  f.held[2].reply()
  await until(async () => (await f.core.diagnostics()).runtime.unsettledAnalysisCalls === 0, 'both current sources finish naturally')
  const snapshot = await f.core.snapshot()
  assert.equal(snapshot.tasks.find(task => task.sourceKey === other).analysis.summary.currentTask, 'Current request 99')
  assert.equal(snapshot.tasks.find(task => task.sourceKey === f.source).analysis.summary.currentTask, 'Current request 12')
  assert.notEqual(snapshot.tasks[0].key, snapshot.tasks[1].key)
  assert.equal(f.peakHttp, 1)
  await f.core.unregisterSource(other)
})

test('source teardown releases its retained transport for another source and keeps unfinished server work explicitly separate', async t => {
  const f = await fixture(t, { honorAbort: true })
  await f.observe(0); await until(() => f.held.length === 1, 'old source extraction entered')
  await f.observe(1)
  const other = await f.core.registerSource({ registrationId: 'surviving-admission', hostRunId: 'host', name: 'Surviving private source' })
  await f.observe(99, other)
  await f.core.unregisterSource(f.source)
  await until(() => f.held.length === 2, 'the live source continues after owner transport teardown')
  assert.equal(f.held[0].finished, false, 'aborted client transport does not prove the service finished old computation')
  assert.equal(f.held[0].closedBeforeFinished, true)
  assert.equal(f.calls[1].request, 'Current request 99')
  const diagnostics = await f.core.diagnostics()
  assert.equal(diagnostics.runtime.connectedSources, 1)
  assert.equal(diagnostics.runtime.cancelledUnsettledRequests, 0)
  assert.equal(diagnostics.runtime.unsettledAnalysisCalls, 1)
  f.held[0].reply(); f.held[1].reply()
  await until(async () => (await f.core.diagnostics()).runtime.unsettledAnalysisCalls === 0, 'current work finishes while the service remains resident')
  assert.equal((await f.core.snapshot()).tasks.find(task => task.sourceKey === other).analysis.summary.currentTask, 'Current request 99')
  await f.core.unregisterSource(other)
})

test('core close ends a retained submitted client transport and all owned timers while leaving model work ownership untouched', async t => {
  const f = await fixture(t, { honorAbort: true })
  await f.observe(0); await until(() => f.held.length === 1, 'submitted model request is held')
  await f.observe(1)
  await f.core.close()
  await until(async () => (await f.core.diagnostics()).runtime.unsettledAnalysisCalls === 0, 'owned transport settles after core close')
  const diagnostics = await f.core.diagnostics()
  assert.equal(diagnostics.closed, true)
  assert.equal(diagnostics.temporary.timers.total, 0)
  assert.equal(diagnostics.temporary.queueKeys, 0)
  assert.equal(diagnostics.runtime.cancelledUnsettledRequests, 0)
  assert.equal(f.held[0].finished, false)
  assert.equal(f.held[0].closedBeforeFinished, true)
  f.held[0].reply()
})
