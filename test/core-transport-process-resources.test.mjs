import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { connectCore } from '../src/core-client.mjs'

const alive = pid => { try { process.kill(pid, 0); return true } catch (error) { if (error.code === 'ESRCH') return false; throw error } }
const missing = async path => { try { await stat(path); return false } catch (error) { if (error.code === 'ENOENT') return true; throw error } }
async function until(predicate, deadline, label) {
  while (!await predicate()) { assert.ok(Date.now() < deadline, `${label} exceeded its wall-clock deadline`); await delay(20) }
}

test('ordinary replacements retain one submitted HTTP call, while owner stop releases the transport and the production daemon exits after its default idle grace', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'gs-core-transport-process-'))
  let client, runtime, source, draining = false, modelWork = 0, peak = 0, stoppedAt
  const jobs = [], forcedCleanup = []
  const models = createServer(async (request, response) => {
    let raw = ''; for await (const chunk of request) raw += chunk
    const body = JSON.parse(raw)
    modelWork++; peak = Math.max(peak, modelWork)
    const job = { finished: false, closedBeforeFinished: false, finish() {
      if (job.finished) return
      job.finished = true; modelWork--
      if (!response.destroyed) { response.setHeader('content-type', 'application/json'); response.end(JSON.stringify(request.url === '/v1/chat/completions' ? { model: body.model, choices: [{ finish_reason: 'stop', message: { content: '{"selectedOptionIds":[],"gaps":[]}' } }] } : { model: body.model, answers: {} })) }
    } }
    response.once('close', () => { if (!job.finished) job.closedBeforeFinished = true })
    jobs.push(job)
    if (draining) job.finish()
  })
  t.after(async () => {
    draining = true
    for (const job of jobs) job.finish()
    if (client) { if (source) await client.unregisterSource(source).catch(() => {}); await client.close() }
    if (runtime && alive(runtime.pid)) {
      try { await until(() => !alive(runtime.pid), Date.now() + 13_000, 'owned daemon cleanup') }
      catch (error) {
        const saved = JSON.parse(await readFile(join(dataDir, 'runtime.json'), 'utf8'))
        assert.equal(saved.instanceId, runtime.instanceId); assert.equal(saved.pid, runtime.pid)
        process.kill(runtime.pid, 'SIGTERM'); forcedCleanup.push({ signal: 'SIGTERM', reason: error.message })
        await until(() => !alive(runtime.pid), Date.now() + 3000, 'owned daemon failure cleanup')
      }
    }
    if (models.listening) await new Promise(resolve => models.close(resolve))
    assert.equal(modelWork, 0)
    await rm(dataDir, { recursive: true, force: true })
    t.diagnostic(`cleanup: ${JSON.stringify({ daemonAlive: runtime ? alive(runtime.pid) : false, client: client?.transportDiagnostics(), modelWork, modelListening: models.listening, forcedCleanup })}`)
    assert.equal(forcedCleanup.length, 0, 'force cleanup cannot establish a passing resource lifecycle')
  })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  const baseUrl = `http://127.0.0.1:${models.address().port}`
  client = await connectCore({ dataDir })
  runtime = JSON.parse(await readFile(join(dataDir, 'runtime.json'), 'utf8'))
  const health = await client.health()
  assert.equal(health.instanceId, runtime.instanceId)
  assert.equal((await client.diagnostics()).pid, runtime.pid)
  const config = { llm: { baseUrl, model: 'process-extractor' }, jev: { baseUrl, model: 'process-judge' } }
  await client.configureAnalysis(config)
  source = await client.registerSource({ registrationId: 'owner-stop', hostRunId: 'own', name: 'Own controlled source' })
  const request = index => ({ id: `request-${index}`, taskId: 'actor', taskLifecycleId: 'life', kind: 'input', text: `Fixed request ${index}`, capturedAt: Date.now(), occurredAt: null })
  await client.ingest(source, { tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Own actor' }], events: [request(0)] })
  await until(() => jobs.length === 1, Date.now() + 3000, 'first production HTTP entered')
  for (let index = 1; index <= 12; index++) await client.ingest(source, { events: [request(index)] })
  const pending = await client.snapshot(), counts = await client.diagnostics()
  assert.deepEqual(pending.monitoring.admission, { limit: 1, unsettledCalls: 1, blocked: true })
  assert.equal(pending.tasks[0].analysis.state, 'queued')
  assert.ok(pending.tasks[0].analysis.queuedAt != null)
  assert.equal(counts.runtime.cancelledUnsettledRequests, 1)
  assert.equal(counts.runtime.activeAnalyses, 0)
  assert.equal(jobs.length, 1); assert.equal(modelWork, 1); assert.equal(peak, 1)
  assert.equal(jobs[0].closedBeforeFinished, false)
  await client.unregisterSource(source); source = null; stoppedAt = Date.now()
  await until(async () => (await client.diagnostics()).runtime.unsettledAnalysisCalls === 0, stoppedAt + 3000, 'owner stop releases client HTTP')
  await until(() => jobs[0].closedBeforeFinished, stoppedAt + 3000, 'server observed closed client socket')
  assert.equal(jobs[0].finished, false, 'client transport completion cannot prove remote model computation finished')
  assert.equal(modelWork, 1)
  assert.equal((await client.diagnostics()).runtime.connectedSources, 0)
  await client.close()
  assert.deepEqual(client.transportDiagnostics(), { closed: true, pendingRequests: 0, deadlineTimers: 0 })
  await until(async () => !alive(runtime.pid) && await missing(join(dataDir, 'runtime.json')) && await missing(join(dataDir, 'core.lock')), stoppedAt + 13_000, 'default ten-second daemon idle exit')
  assert.ok(Date.now() - stoppedAt >= 9000, 'the real daemon keeps its existing ten-second idle grace')
  await assert.rejects(fetch(`${runtime.url}/v1/health`, { signal: AbortSignal.timeout(1000) }), /fetch failed/)
  assert.deepEqual(JSON.parse(await readFile(join(dataDir, 'analysis-config.json'), 'utf8')), config)
  assert.equal(models.listening, true, 'closing the last source does not unload the model service')
  assert.equal(jobs[0].finished, false)
  t.diagnostic(`server-work: ${JSON.stringify({ entered: jobs.length, peakUnfinished: peak, clientTransportEnded: true, serviceFinishedAtDaemonExit: jobs[0].finished, idleExitMs: Date.now() - stoppedAt, runtimeRemoved: true, lockRemoved: true, pidGone: true, listenerGone: true, configurationSaved: true })}`)
})
