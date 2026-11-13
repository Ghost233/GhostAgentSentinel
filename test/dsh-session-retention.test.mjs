import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { createCore } from '../src/core/index.mjs'
import { createObservation, hostIdentity } from '../dsh/observation.mjs'
import { DSH_EXECUTION_SUMMARIES_FEATURE } from '../src/protocol.mjs'

test('DSH retains 200 observations per session across executions and host runs', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-dsh-retention-'))
  let clock = Date.now(), core
  t.after(async () => { await core?.close(); await rm(dir, { recursive: true, force: true }) })
  const open = () => createCore({ dataDir: dir, now: () => clock })
  core = await open()
  const source = hostRunId => core.registerSource({ registrationId: 'dsh', hostRunId, name: 'DSH' })
  const first = await source('first'), second = await source('second')
  const task = (id, sessionId) => ({ id, sessionId, lifecycleId: id, title: id, status: 'idle' })
  const old = task('old', 'chat'), recent = task('recent', 'chat'), other = task('other', 'other-chat')
  const output = (task, id) => ({ id, taskId: task.id, taskLifecycleId: task.lifecycleId,
    kind: 'output', text: id, capturedAt: ++clock, occurredAt: clock })
  const older = Array.from({ length: 180 }, (_, i) => output(old, 'old-' + i))
  await core.ingest(first, { tasks: [old], events: older })
  const newer = Array.from({ length: 45 }, (_, i) => output(recent, 'recent-' + i))
  const separate = output(other, 'other')
  await core.ingest(second, { tasks: [recent, other], events: [...newer, separate] })
  await core.ingest(first, { events: older.slice(0, 5) })

  async function check() {
    const records = (await core.snapshot()).tasks.flatMap(task => task.events).map(event => event.id).sort()
    assert.deepEqual(records, [...older.slice(25), ...newer, separate].map(event => event.id).sort())
    assert.equal((await core.diagnostics()).retained.events, 201)
  }
  await check()
  await core.close()
  const db = new DatabaseSync(join(dir, 'observations.sqlite'))
  try {
    const write = db.prepare("INSERT OR IGNORE INTO records VALUES('events', ?, ?)")
    const owner = JSON.parse(db.prepare("SELECT value FROM records WHERE collection='tasks' AND json_extract(value,'$.id')='old'").get().value)
    for (const event of older.slice(0, 25)) {
      const key = JSON.stringify([first, old.id, old.lifecycleId, event.id])
      write.run(key, JSON.stringify({ ...event, key, taskKey: owner.key, sequence: 0 }))
    }
  } finally { db.close() }
  core = await open()
  await check()
})

test('rolling a full DSH window keeps the current request and lets an in-flight analysis finish', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-dsh-retention-analysis-'))
  let clock = Date.now(), core, release, started
  const waiting = new Promise(resolve => { release = resolve })
  const round = new Promise(resolve => { started = resolve })
  t.after(async () => { release(); await core?.close(); await rm(dir, { recursive: true, force: true }) })
  core = await createCore({ dataDir: dir, now: () => clock, analyzer: {
    async analyze(input, { signal }) {
      started({ input, signal })
      await waiting
      return {
        summary: { currentTask: 'request', currentState: 'running', recentProgress: '', attention: '' },
        checks: ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait']
          .map(id => ({ id, status: 'unknown', reason: 'no conclusion', evidenceIds: [] })),
        coverage: input.coverage,
      }
    },
  } })
  const source = await core.registerSource({ registrationId: 'dsh', hostRunId: 'host', name: 'DSH', capabilities: [DSH_EXECUTION_SUMMARIES_FEATURE] })
  const task = { id: 'task', sessionId: 'chat', lifecycleId: 'run', title: 'Task', status: 'running' }
  const event = (id, kind = 'output') => ({ id, taskId: task.id, taskLifecycleId: task.lifecycleId, kind,
    text: id, capturedAt: ++clock, occurredAt: clock })
  await core.pause()
  const request = event('request', 'input')
  await core.ingest(source, { tasks: [task], events: [request, ...Array.from({ length: 520 }, (_, i) => event('output-' + i))] })
  await core.resume()
  const { input, signal } = await round
  assert.equal(input.task.currentRequestId, request.id)
  assert.equal(input.events.length, 200)
  assert.equal(input.events[0].id, request.id)
  assert.equal(input.events[1].id, 'output-321')
  await core.ingest(source, { events: [event('next')] })
  assert.equal(signal.aborted, false)
  release()
  await new Promise(resolve => setImmediate(resolve))
  const result = (await core.snapshot()).tasks[0]
  assert.equal(result.events.length, 200)
  assert.equal(result.events.at(-1).id, 'next')
  assert.equal(result.analysis.state, 'cooldown')
})

test('DSH accepts hierarchy retries and new revisions after dropping old proof bodies', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-dsh-retention-proof-'))
  let clock = Date.now()
  const core = await createCore({ dataDir: dir, now: () => clock })
  t.after(async () => { await core.close(); await rm(dir, { recursive: true, force: true }) })
  const source = await core.registerSource({ registrationId: 'dsh', hostRunId: 'host', name: 'DSH' })
  const old = { id: 'old', sessionId: 'chat', lifecycleId: 'old', title: 'Old', status: 'idle' }
  const active = { ...old, id: 'active', lifecycleId: 'active', title: 'Active', status: 'running' }
  const event = (task, id, kind, data) => ({ id, taskId: task.id, taskLifecycleId: task.lifecycleId, kind, data, text: id, capturedAt: ++clock, occurredAt: clock })
  const end = event(old, 'old-end', 'status', { status: 'idle', nativeType: 'agent/status' })
  const start = event(active, 'start', 'status', { status: 'running', nativeType: 'agent/status' })
  const context = { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'chat' }], relations: [], instructionBindings: [],
    activations: [[old, end, 'ended'], [active, start, 'running']].map(([task, event, state]) => ({
      taskId: task.id, lifecycleId: task.lifecycleId, sessionId: task.sessionId, runId: null, state,
      stateRefs: [{ taskId: task.id, lifecycleId: task.lifecycleId, eventId: event.id }],
    })), coverage: { complete: true, branchGaps: [] },
  }
  await core.ingest(source, { tasks: [old, active], events: [end, start], hierarchyContext: context })
  const output = Array.from({ length: 505 }, (_, i) => event(active, 'output-' + i, 'output'))
  await core.ingest(source, { events: output })
  for (const revision of [1, 2, 2]) {
    const result = await core.ingest(source, { events: [end, start], hierarchyContext: { ...context, revision } })
    assert.equal(result.hierarchyContext.acceptedRevision, revision)
    assert.equal(result.accepted, 0)
  }
  const snapshot = await core.snapshot()
  assert.equal(snapshot.tasks.find(task => task.id === old.id).events.length, 0)
  assert.equal(snapshot.tasks.find(task => task.id === active.id).events.length, 200)
  assert.equal(snapshot.tasks.find(task => task.id === active.id).hierarchy.state, 'running')
})

test('DSH batches join live stream fragments and skip the duplicate complete block', () => {
  let revision = 0, now = 1000
  const collector = createObservation({ identity: hostIdentity({}), now: () => now })
  const agent = { id: 'chat', status: 'running', session: { id: 'chat', header: {} } }
  const chunk = (type, index, data) => collector.observe('agent/assistant-stream', { agent,
    frame: { type: 'chunk', attemptId: 'attempt', revision: ++revision, time: ++now, chunk: { type, index, ...data } } })
  for (const text of ['检查', '完成', '。']) chunk('text-delta', 0, { text })
  chunk('block-end', 0, { block: { type: 'text', text: '检查完成。' } })
  const first = collector.batch()
  assert.deepEqual(first.events.map(event => event.text), ['检查完成。'])
  collector.acknowledge(first.events.map(event => event.id), first.tasks)
  chunk('reasoning-delta', 1, { text: 'private thought' })
  chunk('block-end', 1, { block: { type: 'reasoning', text: 'private thought' } })
  chunk('text-delta', 2, { text: '继续' })
  const live = collector.batch()
  assert.ok(live.events.some(event => event.text === '继续'))
  assert.equal(live.events.filter(event => event.text === 'private thought').length, 1)
  collector.acknowledge(live.events.map(event => event.id), live.tasks)
  chunk('text-delta', 2, { text: '验证' })
  chunk('block-end', 2, { block: { type: 'text', text: '继续验证' } })
  assert.deepEqual(collector.batch().events.map(event => event.text), ['验证'])
  collector.acknowledge(collector.batch().events.map(event => event.id), live.tasks)
  chunk('text-delta', 3, { text: 'Authorization: ' })
  chunk('text-delta', 3, { text: 'Bearer synthetic-secret' })
  assert.equal(collector.batch().events[0].data.redacted, true)
  assert.doesNotMatch(JSON.stringify(collector.batch()), /synthetic-secret/)
  collector.close()
})
