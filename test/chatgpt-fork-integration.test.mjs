import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, writeFile, rm, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'

test('a discovered native fork reaches its own panel with inherited user limits and no inherited current activity', async t => {
  let clock = Date.now(); t.mock.method(Date, 'now', () => clock)
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-fork-integration-')), sessionsRoot = join(dir, 'sessions'), archive = join(dir, 'archived_sessions')
  await mkdir(sessionsRoot); await mkdir(archive)
  const row = (ordinal, type, payload, at = clock - 100000) => JSON.stringify({ ordinal, type, payload, timestamp: new Date(at).toISOString() }) + '\n'
  const header = id => ({ id, history_mode: 'paginated', cwd: '/project', originator: 'Codex Desktop', source: 'vscode', cli_version: supportedProfile.cliVersion })
  const start = id => ({ type: 'task_started', turn_id: id, turn_attribution: { turn_id: id, turn_trigger: 'composer' } })
  const user = (thread, turn, text) => ({ type: 'item_completed', thread_id: thread, turn_id: turn, item: { type: 'UserMessage', id: turn + '-user', content: [{ type: 'text', text }] } })
  const inherited = row(0, 'session_meta', header('original')) + row(1, 'event_msg', start('old-run'))
    + row(2, 'event_msg', user('original', 'old-run', 'Do not change files outside the requested directory'))
    + row(3, 'event_msg', { type: 'item_completed', thread_id: 'original', turn_id: 'old-run', item: { type: 'AgentMessage', id: 'promise', content: [{ type: 'Text', text: 'I will change every directory' }] } })
    + row(4, 'event_msg', { type: 'item_completed', thread_id: 'original', turn_id: 'old-run', item: { type: 'CommandExecution', id: 'old-test', aggregated_output: 'Old tests passed', exit_code: 0 } })
    + row(5, 'event_msg', { type: 'item_completed', thread_id: 'original', turn_id: 'old-run', item: { type: 'McpToolCall', id: 'old-mcp', server: 'controlled', tool: 'read', result: { opaque: 'unverified historical MCP body' } } })
    + row(6, 'event_msg', { type: 'task_complete', turn_id: 'old-run' })
  const parentPath = join(archive, 'parent.jsonl'), path = join(sessionsRoot, 'fork.jsonl')
  await writeFile(parentPath, inherited + row(7, 'event_msg', start('source-later')) + row(8, 'event_msg', user('original', 'source-later', 'Unrelated later source request')))
  await writeFile(path, row(7, 'session_meta', { ...header('fork'), forked_from_id: 'original', forked_from_ordinal_exclusive: 7, history_base: { thread_id: 'original', end_ordinal_exclusive: 7, end_byte_offset: Buffer.byteLength(inherited) } }, clock)
    + row(8, 'event_msg', { type: 'thread_settings_applied', thread_id: 'fork' }, clock) + row(9, 'event_msg', start('current'), clock)
    + row(10, 'event_msg', user('fork', 'current', 'Inspect the new fork request'), clock)
    + row(11, 'event_msg', { type: 'item_completed', thread_id: 'fork', turn_id: 'current', item: { type: 'AgentMessage', id: 'new-output', content: [{ type: 'Text', text: 'Inspecting this fork only' }] } }, clock))
  const metadata = id => ({ id, path: id === 'fork' ? path : parentPath, cwd: '/project', parentThreadId: null, forkedFromId: id === 'fork' ? 'original' : null, source: 'vscode', originator: 'Codex Desktop' })
  const reads = [], native = {
    async sampleHost() { return { available: true, hostRunId: 'fork-host', hostStartedAt: clock - 1000, profile: supportedProfile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
    async readThread(id) { reads.push(id); return { thread: metadata(id), anchorTurnId: id === 'fork' ? 'current' : null } },
    async listThreads({ archived }) { return { data: archived ? [] : [metadata('fork')], nextCursor: null } }, async close() {},
  }
  const server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }), core = await connectCore({ dataDir: join(dir, 'core') })
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot, dataDir: join(dir, 'adapter'), installationId: 'fork-integration', token: 'f'.repeat(64), pollMs: 0, collectMaterials: true, now: () => clock })
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  await adapter.refresh()
  const read = async () => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'f'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'fork' } } }) })
    assert.equal(response.status, 200); const result = await response.json(); assert.equal(result.result.isError, undefined); return result.result.structuredContent
  }
  let view
  for (let pump = 0; pump < 30; pump++) { view = await read(); if (view.observation.coverage === 'caught-up') break }
  assert.equal(view.identity.threadId, 'fork')
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view))
  assert.equal(view.forkProvenance.complete, true)
  assert.deepEqual(view.discovery.forkCoverage, { known: 1, verified: 1, pending: 0 })
  assert.equal(view.discovery.gaps.includes('fork-coverage-pending'), false)
  const snapshot = await core.snapshot(), task = snapshot.tasks.find(task => task.lifecycleId === 'current')
  assert.equal(task.currentRequest, 'Inspect the new fork request')
  assert.equal(task.status, 'running')
  assert.equal(task.instructionContext.userLimitRefs.length, 1)
  assert.equal(task.coverage.complete, true, JSON.stringify(task.coverage))
  assert.ok(task.events.some(event => event.text === 'Old tests passed' && event.data.history && event.data.inherited))
  assert.ok(task.events.some(event => event.text === 'Inspecting this fork only' && !event.data.history))
  assert.equal(snapshot.tasks.every(task => task.id === 'fork'), true, 'history sources are not fabricated execution tasks or subagents')
  assert.equal(JSON.stringify(snapshot).includes('Unrelated later source request'), false)
  assert.deepEqual(snapshot.sources[0].hierarchyContext.relations, [])
  assert.ok(reads.includes('original'))
  const cursor = JSON.parse(await readFile(join(dir, 'adapter', 'cursors', view.identity.cursorKey + '.json'), 'utf8'))
  assert.equal(cursor.reader.kind, 'fork-lineage/1')
  assert.ok(cursor.captures.every(capture => capture.offset <= 11))
})
