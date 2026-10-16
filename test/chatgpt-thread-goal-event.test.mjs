import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { BoundedReader, supportedProfile } from '../chatgpt/bounded-reader.mjs'

const at = 1_800_000_000_000
const owner = '11111111-1111-4111-8111-111111111111'
const row = (ordinal, type, payload, timestamp = at) => JSON.stringify({ timestamp: new Date(timestamp).toISOString(), ordinal, type, payload }) + '\n'
const header = () => row(0, 'session_meta', { id: owner, originator: 'Codex Desktop', source: 'vscode', cli_version: supportedProfile.cliVersion, cwd: '/synthetic-only', history_mode: 'paginated' })
// The actual envelope has type/threadId/goal and seven direct goal fields.
// Their names and values here are synthetic, not the user's goal or a schema claim.
const goal = (type = 'thread_goal_updated') => ({ type, threadId: owner, goal: Object.fromEntries(Array.from({ length: 7 }, (_, index) => ['synthetic_' + index, 'GOAL_IS_NOT_USER_AUTHORIZATION'])) })
async function fixture(t, records, { mode = 'history-metadata', anchorTurnId = 'current-run', endOrdinalExclusive = records.length + 1 } = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-thread-goal-')), path = join(dir, 'main.jsonl')
  const source = { path, allowedRoot: dir, threadId: owner, parentId: null, sourceKind: 'vscode', originator: 'Codex Desktop', hostRunId: 'owned-host', hostStartedAt: at - 10000, profile: supportedProfile, anchorTurnId }
  let reader
  t.after(async () => { await reader?.close(); await rm(dir, { recursive: true, force: true }) })
  const text = header() + records.join('')
  if (mode.startsWith('history')) source.historyRange = { endOrdinalExclusive, endByteOffset: Buffer.byteLength(text) }
  await writeFile(path, text, { mode: 0o600 })
  reader = new BoundedReader(source, null, { mode })
  return { reader, source }
}

test('locator history metadata reads through the known thread-goal envelope to its exact native cut', async t => {
  const f = await fixture(t, [row(1, 'event_msg', { type: 'task_started', turn_id: 'current-run' }), row(2, 'event_msg', goal(), at + 1000), row(3, 'event_msg', { type: 'task_complete', turn_id: 'current-run' }, at + 2000)])
  const view = await f.reader.pump()
  assert.equal(view.gap, null)
  assert.equal(view.coverage, 'caught-up')
  assert.equal(view.offset, f.source.historyRange.endByteOffset)
  assert.equal(f.reader.checkpoint().nextOrdinal, 4)
  assert.deepEqual(view.recentLifecycle.map(event => event.type), ['task_started', 'task_complete'])
  assert.doesNotMatch(JSON.stringify(view) + JSON.stringify(f.reader.checkpoint()), /GOAL_IS_NOT_USER_AUTHORIZATION/)
})

test('a goal update changes neither lifecycle nor collected material in any reader mode or cursor replay', async t => {
  for (const mode of ['metadata', 'material', 'history-metadata', 'history']) await t.test(mode, async t => {
    const f = await fixture(t, [row(1, 'event_msg', { type: 'task_started', turn_id: 'current-run', turn_attribution: { turn_id: 'current-run', turn_trigger: 'composer' } }), row(2, 'event_msg', goal(), at + 1000), row(3, 'event_msg', { type: 'thread_settings_applied' }, at + 2000)], { mode })
    const view = await f.reader.pump()
    assert.equal(view.gap, null)
    assert.equal(view.coverage, 'caught-up')
    assert.equal(view.observed.phase, 'started')
    assert.equal(view.observed.at, at)
    assert.equal(view.observed.turnId, 'current-run')
    assert.deepEqual(view.materials ?? [], [])
    assert.deepEqual(view.recentLifecycle.map(event => [event.type, event.at]), [['task_started', at]])
    const cursor = f.reader.checkpoint()
    if (['material', 'history'].includes(mode)) assert.equal(cursor.turnTrigger, 'composer')
    assert.doesNotMatch(JSON.stringify(view) + JSON.stringify(cursor), /GOAL_IS_NOT_USER_AUTHORIZATION|thread_goal_updated/)
    await f.reader.close()
    const resumed = new BoundedReader(f.source, cursor, { mode }); t.after(() => resumed.close())
    const replay = await resumed.pump()
    assert.equal(replay.gap, null)
    assert.equal(replay.coverage, 'caught-up')
    assert.deepEqual(replay.materials ?? [], [])
    assert.deepEqual(replay.recentLifecycle, [])
    assert.equal(replay.observed.at, at)
  })
})

test('goal-only records cannot establish a current request, execution start, or inherited user constraint', async t => {
  for (const mode of ['material', 'history']) await t.test(mode, async t => {
    const f = await fixture(t, [row(1, 'event_msg', goal())], { mode })
    const view = await f.reader.pump()
    assert.equal(view.gap, 'current-turn-not-observed')
    assert.equal(view.observed.anchored, false)
    assert.equal(view.observed.turnId, null)
    assert.equal(view.observed.phase, 'unknown')
    assert.deepEqual(view.materials, [])
    assert.deepEqual(view.recentLifecycle, [])
    assert.doesNotMatch(JSON.stringify(f.reader.checkpoint()), /GOAL_IS_NOT_USER_AUTHORIZATION/)
  })
})

test('an independently consumed user request remains the only input when a later goal is metadata', async t => {
  const f = await fixture(t, [row(1, 'event_msg', { type: 'task_started', turn_id: 'current-run', turn_attribution: { turn_id: 'current-run', turn_trigger: 'composer' } }), row(2, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'current-run', item: { type: 'UserMessage', id: 'owned-user-entry', content: [{ type: 'text', text: 'Keep the authorized request unchanged' }] } }), row(3, 'event_msg', goal(), at + 1000)], { mode: 'material' })
  const view = await f.reader.pump()
  assert.equal(view.gap, null)
  assert.deepEqual(view.materials.map(event => [event.kind, event.text, event.data.inputOrigin, event.data.runId]), [['input', 'Keep the authorized request unchanged', 'user-entry', 'current-run']])
  assert.equal(view.observed.at, at)
  assert.deepEqual(view.recentLifecycle.map(event => event.type), ['task_started'])
})

test('unrecognized event types still stop an anchored reader with the same goal-shaped payload', async t => {
  for (const type of ['thread_goal_updated_extra', 'thread_goal_update', 'unknown_native_event']) await t.test(type, async t => {
    const f = await fixture(t, [row(1, 'event_msg', { type: 'task_started', turn_id: 'current-run' }), row(2, 'event_msg', goal(type)), row(3, 'event_msg', { type: 'task_complete', turn_id: 'current-run' })])
    const view = await f.reader.pump()
    assert.equal(view.coverage, 'gap')
    assert.equal(view.gap, 'unsupported-event-type:' + type)
    assert.ok(view.offset < f.source.historyRange.endByteOffset)
    assert.deepEqual(view.recentLifecycle.map(event => event.type), ['task_started'])
  })
})

test('the known metadata event cannot bypass ordinal or JSON integrity', async t => {
  for (const mutation of ['ordinal', 'malformed']) await t.test(mutation, async t => {
    const event = row(mutation === 'ordinal' ? 20 : 2, 'event_msg', goal())
    const f = await fixture(t, [row(1, 'event_msg', { type: 'task_started', turn_id: 'current-run' }), mutation === 'malformed' ? event.replace('"goal":{', '"goal":{bad-json') : event, row(3, 'event_msg', { type: 'task_complete', turn_id: 'current-run' })])
    const view = await f.reader.pump()
    assert.equal(view.coverage, 'gap')
    assert.equal(view.gap, mutation === 'ordinal' ? 'record-ordinal-unverified' : 'record-malformed-or-over-limit')
  })
})

test('large ignored goal values retain bounded physical reads and produce no semantic or verification evidence', async t => {
  const payload = goal(); payload.goal.synthetic_0 = 'GOAL_IS_NOT_USER_AUTHORIZATION'.repeat(20000)
  const f = await fixture(t, [row(1, 'event_msg', { type: 'task_started', turn_id: 'current-run' }), row(2, 'event_msg', payload), row(3, 'event_msg', { type: 'task_complete', turn_id: 'current-run' }, at + 2000)], { mode: 'history' })
  let view, pumps = 0
  const materials = [], events = []
  do {
    view = await f.reader.pump(); pumps++
    materials.push(...view.materials); events.push(...view.recentLifecycle)
    assert.ok(view.readBytes <= 262144)
    assert.ok(view.materials.length + view.recentLifecycle.length <= 16)
    assert.ok(view.metrics.peakProjectionBytes <= 16384)
    assert.ok(pumps < 10)
  } while (view.coverage === 'catching-up')
  assert.ok(pumps > 1)
  assert.equal(view.coverage, 'caught-up')
  assert.equal(view.gap, null)
  assert.deepEqual(materials, [])
  assert.deepEqual(events.map(event => [event.type, event.at]), [['task_started', at], ['task_complete', at + 2000]])
  assert.equal(f.reader.checkpoint().nextOrdinal, 4)
  assert.doesNotMatch(JSON.stringify(view) + JSON.stringify(f.reader.checkpoint()), /GOAL_IS_NOT_USER_AUTHORIZATION/)
})
