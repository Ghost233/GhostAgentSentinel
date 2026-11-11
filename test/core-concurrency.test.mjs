import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { createCore } from '../src/core/index.mjs'

const ids = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait']
const result = input => ({ summary: { currentTask: input.task.currentRequest, currentState: 'observed', recentProgress: '', attention: '' }, checks: ids.map(id => ({ id, status: 'unknown', reason: 'Controlled model has insufficient evidence', evidenceIds: [] })), coverage: input.coverage })
async function until(predicate) {
  for (let attempt = 0; attempt < 100; attempt++) { if (await predicate()) return; await delay(3) }
  assert.fail('Expected concurrency state did not become observable')
}
async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-concurrency-')), calls = []
  let time = 1_800_000_000_000
  const analyzer = { analyze(input, options) { return new Promise(resolve => calls.push({ input, ...options, finish: () => resolve(result(input)) })) } }
  const core = await createCore({ dataDir, analyzer, now: () => time, leaseMs: 1e9 })
  t.after(async () => { await core.close(); for (const call of calls) call.finish(); await rm(dataDir, { recursive: true, force: true }) })
  const source = await core.registerSource({ registrationId: 'installation-a', hostRunId: 'host', name: 'Installation A' })
  const add = async (names, sourceKey = source) => core.ingest(sourceKey, {
    tasks: names.map(id => ({ id, lifecycleId: 'life', title: id, status: 'running' })),
    events: names.map(id => ({ id: id + '-request', taskId: id, taskLifecycleId: 'life', kind: 'input', text: id + ' request', capturedAt: time, occurredAt: time })),
  })
  return { core, dataDir, calls, source, add, advance: ms => { time += ms } }
}

test('four configured lanes analyze four chats and keep the fifth queued until a real completion', async t => {
  const f = await fixture(t)
  await f.core.configureInstallationScheduling('installation-a', { concurrency: 4 })
  await f.add(['a', 'b', 'c', 'd', 'e'])
  await until(() => f.calls.length === 4)
  assert.deepEqual(f.calls.map(call => call.input.task.id), ['a', 'b', 'c', 'd'])
  assert.equal((await f.core.diagnostics()).runtime.activeAnalyses, 4)
  assert.equal((await f.core.snapshot()).monitoring.queueLength, 1)
  f.calls[1].finish()
  await until(() => f.calls.length === 5)
  assert.equal(f.calls[4].input.task.id, 'e')
  assert.equal((await f.core.diagnostics()).runtime.activeAnalyses, 4)
  assert.equal((await f.core.snapshot()).monitoring.queueLength, 0)
})

test('cancellation keeps its lane and a replacement cannot overlap the same chat while other lanes continue', async t => {
  const f = await fixture(t)
  await f.core.configureInstallationScheduling('installation-a', { concurrency: 4 })
  await f.add(['a', 'b', 'c', 'd', 'e'])
  await until(() => f.calls.length === 4)
  f.advance(1)
  await f.core.ingest(f.source, { events: [{ id: 'a-next', taskId: 'a', taskLifecycleId: 'life', kind: 'input', text: 'new a request', capturedAt: 1_800_000_000_001, occurredAt: null }] })
  assert.equal(f.calls[0].signal.aborted, true)
  assert.equal((await f.core.diagnostics()).runtime.unsettledAnalysisCalls, 4)
  assert.equal(f.calls.length, 4)
  f.calls[1].finish()
  await until(() => f.calls.length === 5)
  assert.equal(f.calls[4].input.task.id, 'e')
  assert.equal(f.calls.filter(call => call.input.task.id === 'a').length, 1)
  f.calls[0].finish()
  await until(() => f.calls.length === 6)
  assert.equal(f.calls[5].input.task.currentRequest, 'new a request')
  assert.equal((await f.core.diagnostics()).runtime.unsettledAnalysisCalls, 4)
})

test('raising the limit fills free lanes and lowering it drains existing work without cancellation', async t => {
  const f = await fixture(t)
  await f.add(['a', 'b', 'c', 'd', 'e', 'f'])
  await until(() => f.calls.length === 1)
  const before = await f.core.installationAnalysisConfig('installation-a')
  await f.core.configureInstallationScheduling('installation-a', { concurrency: 4 })
  await until(() => f.calls.length === 4)
  assert.equal((await f.core.installationAnalysisConfig('installation-a')).revision, before.revision)
  await f.core.configureInstallationScheduling('installation-a', { concurrency: 2 })
  assert.ok(f.calls.every(call => !call.signal.aborted))
  f.calls[0].finish(); await until(async () => (await f.core.diagnostics()).runtime.activeAnalyses === 3)
  f.calls[1].finish(); await until(async () => (await f.core.diagnostics()).runtime.activeAnalyses === 2)
  assert.equal(f.calls.length, 4)
  f.calls[2].finish(); await until(() => f.calls.length === 5)
  assert.equal(f.calls[4].input.task.id, 'e')
  assert.equal((await f.core.diagnostics()).runtime.activeAnalyses, 2)
})

test('installation limits are independent and never reset model configuration', async t => {
  const f = await fixture(t)
  const config = { llm: { baseUrl: 'http://localhost:54841', model: 'summary' } }
  await f.core.configureInstallationScheduling('installation-a', { concurrency: 4 })
  await f.core.configureInstallationAnalysis('installation-a', config, (await f.core.installationAnalysisConfig('installation-a')).revision)
  assert.equal((await f.core.installationAnalysisConfig('installation-a')).concurrency, 4)
  const other = await f.core.registerSource({ registrationId: 'installation-b', hostRunId: 'host', name: 'Installation B' })
  await f.add(['a', 'b', 'c', 'd', 'e'])
  await f.add(['other-a', 'other-b'], other)
  await until(() => f.calls.length === 5)
  assert.deepEqual(f.calls.map(call => call.input.task.id), ['a', 'b', 'c', 'd', 'other-a'])
  await f.core.configureInstallationScheduling('installation-b', { concurrency: 2 })
  await until(() => f.calls.length === 6)
  assert.equal(f.calls[5].input.task.id, 'other-b')
  assert.deepEqual((await f.core.installationAnalysisConfig('installation-a')).config, config)
  assert.equal((await f.core.installationAnalysisConfig('installation-a')).concurrency, 4)
})

test('concurrency persists across core restart and rejects invalid limits without changing the saved value', async t => {
  const f = await fixture(t)
  await f.core.configureInstallationScheduling('installation-a', { concurrency: 4 })
  for (const value of [0, -1, 17, 1.5, '4', null, true]) await assert.rejects(f.core.configureInstallationScheduling('installation-a', { concurrency: value }), /integer from 1 to 16/)
  await f.core.close()
  const restored = await createCore({ dataDir: f.dataDir })
  try {
    assert.equal((await restored.installationAnalysisConfig('installation-a')).concurrency, 4)
    assert.equal((await restored.installationAnalysisConfig('installation-b')).concurrency, 1)
  } finally { await restored.close() }
})

test('query concurrency and interval persist independently from LLM lanes and model revisions', async t => {
  const f = await fixture(t), initial = await f.core.installationAnalysisConfig('installation-a')
  assert.equal(initial.queryConcurrency, 2)
  assert.equal(initial.concurrency, 1)
  const saved = await f.core.configureInstallationScheduling('installation-a', { queryConcurrency: 3, queryIntervalMs: 5000 })
  assert.equal(saved.revision, initial.revision)
  await f.core.configureInstallationScheduling('installation-a', { concurrency: 4 })
  for (const scheduling of [{ queryConcurrency: 0 }, { queryConcurrency: 17 }, { queryIntervalMs: 0 }, { queryIntervalMs: 1.5 }, {}]) await assert.rejects(f.core.configureInstallationScheduling('installation-a', scheduling))
  await f.core.close()
  const restored = await createCore({ dataDir: f.dataDir })
  try {
    const value = await restored.installationAnalysisConfig('installation-a')
    assert.equal(value.queryConcurrency, 3)
    assert.equal(value.queryIntervalMs, 5000)
    assert.equal(value.concurrency, 4)
    assert.equal(value.revision, initial.revision)
  } finally { await restored.close() }
})
