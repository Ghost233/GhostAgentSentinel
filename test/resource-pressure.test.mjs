import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readFile, stat, rm, mkdir, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { connectCore } from '../src/core-client.mjs'
import { startServer } from '../src/server.mjs'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

test('twenty concurrent startup clients share one real core, isolate same task IDs, and release its process and port after last source', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-twenty-clients-'))
  const clients = await Promise.all(Array.from({ length: 20 }, () => connectCore({ dataDir })))
  const runtime = JSON.parse(await readFile(join(dataDir, 'runtime.json'), 'utf8'))
  t.after(async () => { await Promise.allSettled(clients.map(client => client.close()));
    try { const r = JSON.parse(await readFile(join(dataDir, 'runtime.json'), 'utf8')); if (r.instanceId === runtime.instanceId) process.kill(r.pid, 'SIGTERM') } catch {}
    await rm(dataDir, { recursive: true, force: true }) })
  const sourceKeys = await Promise.all(clients.map((client, index) => client.registerSource({ registrationId: `config-${index}`, hostRunId: `host-${index}`, name: 'pressure test' })))
  await Promise.all(clients.map((client, index) => client.ingest(sourceKeys[index], { tasks: [{ id: 'same-task', lifecycleId: 'same-life', title: `source-${index}`, status: 'idle' }],
    events: [{ id: 'same-event', taskId: 'same-task', taskLifecycleId: 'same-life', kind: 'input', text: `input-${index}`, occurredAt: null, capturedAt: Date.now() }] })))
  const diagnostics = await Promise.all(clients.map(client => client.diagnostics()))
  assert.equal(new Set(diagnostics.map(value => value.pid)).size, 1)
  assert.equal(new Set(diagnostics.map(value => value.instanceId)).size, 1)
  const snapshot = await clients[0].snapshot()
  assert.equal(snapshot.overview.connectedSources, 20)
  assert.equal(snapshot.tasks.length, 20)
  for (let i = 0; i < 20; i++) assert.equal(snapshot.tasks.find(task => task.sourceKey === sourceKeys[i]).currentRequest, `input-${i}`)
  await Promise.all(clients.map((client, index) => client.unregisterSource(sourceKeys[index], 'pressure finished')))
  await Promise.all(clients.map(client => client.close()))
  assert.ok(clients.every(client => client.transportDiagnostics().pendingRequests === 0 && client.transportDiagnostics().deadlineTimers === 0))
  let runtimeGone = false, processGone = false
  const deadline = Date.now() + 14_000
  while (Date.now() < deadline) {
    try { await stat(join(dataDir, 'runtime.json')) } catch (error) { if (error.code === 'ENOENT') runtimeGone = true; else throw error }
    try { process.kill(runtime.pid, 0) } catch (error) { if (error.code === 'ESRCH') processGone = true; else throw error }
    if (runtimeGone && processGone) break
    await delay(100)
  }
  assert.equal(runtimeGone, true)
  assert.equal(processGone, true)
  await assert.rejects(fetch(`${runtime.url}/v1/health`, { signal: AbortSignal.timeout(500) }))
})

test('one hundred connect-call-disconnect cycles restore temporary counters and record warmed heap and RSS samples', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-hundred-cycles-'))
  const server = await startServer({ dataDir })
  const keeper = await connectCore({ dataDir })
  await keeper.registerSource({ registrationId: 'keeper', hostRunId: 'keeper', name: 'pressure keeper' })
  t.after(async () => { await keeper.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  const descriptor = { registrationId: 'cycling', hostRunId: 'stable-runtime', name: 'cycle test' }
  const cycle = async () => {
    const client = await connectCore({ dataDir })
    try { const key = await client.registerSource(descriptor); await client.ingest(key, { tasks: [] }); await client.snapshot(); await client.unregisterSource(key, 'cycle closed') }
    finally { await client.close() }
    assert.deepEqual(client.transportDiagnostics(), { closed: true, pendingRequests: 0, deadlineTimers: 0 })
  }
  for (let i = 0; i < 100; i++) await cycle()
  await delay(1200)
  const samples = []
  const sample = async cycleCount => {
    global.gc?.()
    const diagnostics = await keeper.diagnostics()
    assert.equal(diagnostics.http.activeRequests, 0)
    assert.equal(diagnostics.temporary.queueKeys, 0)
    assert.equal(diagnostics.runtime.activeAnalyses, 0)
    assert.equal(diagnostics.runtime.cancelledUnsettledRequests, 0)
    const system = process.platform === 'darwin'
      ? JSON.parse(execFileSync('python3', [fileURLToPath(new URL('../scripts/memory-footprint.py', import.meta.url)), String(process.pid)], { encoding: 'utf8', timeout: 2000 }))
      : { metric: 'not available on this platform' }
    if (process.platform === 'darwin') assert.ok(system.physicalFootprint > 0)
    samples.push({ cycleCount, rss: diagnostics.memory.rss, heapUsed: diagnostics.memory.heapUsed,
      system, connections: diagnostics.http.connections, retained: diagnostics.retained, temporary: diagnostics.temporary })
  }
  await sample(0)
  for (let i = 1; i <= 100; i++) { await cycle(); if (i % 20 === 0) await sample(i) }
  await delay(1200)
  await sample(100)
  assert.equal(samples.at(-1).connections <= 2, true, 'idle HTTP sockets must also be reclaimed')
  assert.equal(samples[0].retained.sources, samples.at(-1).retained.sources)
  assert.equal(samples[0].retained.sourceGapRecords, samples.at(-1).retained.sourceGapRecords)
  const report = { runtime: process.version, pid: process.pid, gc: !!global.gc, warmupCycles: 100, cycles: 100,
    memoryAssessment: 'samples recorded; no fixed memory budget is inferred', systemFootprint: process.platform === 'darwin' ? 'macOS libproc ri_phys_footprint' : 'unavailable', samples }
  if (process.env.SENTINEL_RESOURCE_REPORT) {
    await mkdir(join(process.env.SENTINEL_RESOURCE_REPORT, '..'), { recursive: true })
    await writeFile(process.env.SENTINEL_RESOURCE_REPORT, JSON.stringify(report, null, 2))
  }
  console.log(JSON.stringify({ resourceMemory: report }))
})
