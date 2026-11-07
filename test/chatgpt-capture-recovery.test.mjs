import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'

test('partial metadata replay cannot recover an unaccepted native material batch or create repeated capacity notices', async t => {
  let clock = Date.now(), server, core, adapter, rejected = 0, block = true
  t.mock.method(Date, 'now', () => clock)
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-capture-recovery-')), path = join(dir, 'main.jsonl')
  t.after(async () => { await adapter?.close(); await core?.close(); await server?.close(); await fs.rm(dir, { recursive: true, force: true }) })
  let ordinal = 0
  const row = (type, payload) => JSON.stringify({ ordinal: ordinal++, type, timestamp: new Date(clock).toISOString(), payload }) + '\n'
  await fs.writeFile(path, row('session_meta', { id: 'main', cwd: dir, source: 'vscode', originator: 'Codex Desktop', history_mode: 'paginated' }) + row('event_msg', { type: 'task_started', turn_id: 'run', turn_attribution: { turn_id: 'run', turn_trigger: 'composer' } }) + row('event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'run', item: { type: 'UserMessage', id: 'request', content: [{ type: 'Text', text: 'Implement the owned task' }] } }))
  const thread = { id: 'main', path, cwd: dir, source: 'vscode', originator: 'Codex Desktop', parentThreadId: null, forkedFromId: null }
  const hostStartedAt = clock - 10000
  const native = { async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt, profile: supportedProfile } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async listThreads() { return { data: [thread], nextCursor: null } }, async readThread() { return { thread, anchorTurnId: 'run' } }, async close() {} }
  server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }); core = await connectCore({ dataDir: join(dir, 'core') })
  const ingest = core.ingest
  core.ingest = async (source, batch) => { if (block && batch.events?.some(event => event.text === 'New native output')) { rejected++; throw Object.assign(new Error('native_capture_capacity'), { code: 'native_capture_capacity' }) }; return ingest(source, batch) }
  adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'capture-recovery', token: 'a'.repeat(64), collectMaterials: true, nativeOriginVersion: 2, pollMs: 0, now: () => clock })
  await adapter.read({ thread_id: 'main' })
  await fs.appendFile(path, row('event_msg', { type: 'thread_goal_updated', goal: 'x'.repeat(600000) }) + row('event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'run', item: { type: 'AgentMessage', id: 'new-output', content: [{ type: 'Text', text: 'New native output' }] } }))
  const sourceKey = (await core.snapshot()).sources[0].key
  for (let cycle = 0; cycle < 100 && rejected < 3; cycle++) { clock += 1500; await adapter.read({ thread_id: 'main' }) }
  assert.ok(rejected >= 3, 'the same material was retried across production reader recovery')
  const receipts = (await core.failureReceipts({ sourceKey })).receipts.filter(value => value.error.code === 'native_capture_capacity')
  assert.equal(receipts.length, 1, 'metadata-only replay does not create new failure episodes')
  assert.equal(receipts[0].state, 'active', 'the material admission failure has not recovered')
  assert.equal((await core.nativeNotificationRecords({ sourceKey })).records.filter(value => value.receiptIds.includes(receipts[0].id)).length, 1)
  block = false
  for (let cycle = 0; cycle < 100; cycle++) { clock += 1500; await adapter.read({ thread_id: 'main' }); if ((await core.failureReceipts({ sourceKey })).receipts.find(value => value.id === receipts[0].id)?.state === 'recovered') break }
  assert.equal((await core.failureReceipts({ sourceKey })).receipts.find(value => value.id === receipts[0].id).state, 'recovered')
  assert.ok((await core.snapshot()).tasks.flatMap(task => task.events).some(event => event.text === 'New native output'))
})
