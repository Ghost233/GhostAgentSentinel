import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { mkdtemp, writeFile, appendFile, readFile, rm, symlink, chmod } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'

const DAY = 86400000
const profile = { appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' }
const row = (type, payload, at) => JSON.stringify({ type, payload, timestamp: new Date(at).toISOString() }) + '\n'

test('accepted lifecycle retransmission keeps its first capture time across failed delivery, adapter/core recovery and 24-hour expiry', async t => {
  let clock = Date.now()
  t.mock.method(Date, 'now', () => clock)
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-capture-'))
  const path = join(dir, 'main.jsonl'), coreDir = join(dir, 'core'), adapterDir = join(dir, 'adapter')
  const sourceBirth = clock - 10000
  await writeFile(path, row('session_meta', { id: 'main-retention' }, clock))
  let server = await startServer({ dataDir: coreDir, now: () => clock }), core, adapter
  const captures = []
  let failNextEvents = false
  const proxy = createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk)
    const body = chunks.length ? Buffer.concat(chunks).toString() : undefined
    const decoded = body ? JSON.parse(body) : null
    if (req.url === '/v1/source/ingest' && decoded?.batch?.events?.length) captures.push(decoded.batch.events.map(event => ({ id: event.id, capturedAt: event.capturedAt })))
    const remote = await fetch(server.url + req.url, { method: req.method, headers: { authorization: req.headers.authorization, 'content-type': 'application/json' }, body })
    const result = await remote.text()
    if (failNextEvents && req.url === '/v1/source/ingest' && decoded?.batch?.events?.length) { failNextEvents = false; res.writeHead(502, { 'content-type': 'application/json' }).end('{"error":"controlled response loss after core acceptance"}'); return }
    res.writeHead(remote.status, { 'content-type': 'application/json' }).end(result)
  })
  await new Promise(resolve => proxy.listen(0, '127.0.0.1', resolve))
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'retention-native:run', hostStartedAt: sourceBirth, profile } },
    async readThread() { return { thread: { id: 'main-retention', parentThreadId: null, source: 'vscode', originator: 'Codex Desktop', path, cwd: dir }, anchorTurnId: 'retention-turn' } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async close() {},
  }
  const connect = async () => {
    const runtimePath = join(coreDir, 'runtime.json'), runtime = JSON.parse(await readFile(runtimePath, 'utf8'))
    await writeFile(runtimePath, JSON.stringify({ ...runtime, url: 'http://127.0.0.1:' + proxy.address().port }))
    core = await connectCore({ dataDir: coreDir })
    adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: adapterDir, installationId: 'retention-installation', token: 'e'.repeat(64), pollMs: 0, now: () => clock })
  }
  t.after(async () => { await adapter?.close(); await core?.close(); await server.close(); proxy.closeAllConnections(); await new Promise(resolve => proxy.close(resolve)); await rm(dir, { recursive: true, force: true }) })
  await connect()
  await adapter.read({ thread_id: 'main-retention' })
  await appendFile(path, row('event_msg', { type: 'task_started', turn_id: 'retention-turn' }, clock))
  failNextEvents = true
  assert.equal((await adapter.read({ thread_id: 'main-retention' })).observation.gap, 'observation-unavailable')
  assert.equal((await core.diagnostics()).retained.events, 1)
  const firstCapture = captures[0][0]
  await adapter.close(); await core.close()
  clock += DAY + 1
  await server.close()
  server = await startServer({ dataDir: coreDir, now: () => clock })
  await connect()
  assert.equal((await core.diagnostics()).retained.events, 0)
  await adapter.read({ thread_id: 'main-retention' })
  assert.deepEqual(captures.at(-1)[0], firstCapture, 'the same record offset must not acquire a fresh capture timestamp')
  assert.equal((await core.diagnostics()).retained.events, 0, 'expired evidence must not be inserted again after retransmission')
})

test('oversized, malformed and unverified cursor files fail closed through the production MCP path', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-cursor-guard-'))
  const sourcePath = join(dir, 'main.jsonl'), started = Date.now() - 10000
  await writeFile(sourcePath, row('session_meta', { id: 'cursor-main' }, started) + row('event_msg', { type: 'task_started', turn_id: 'cursor-turn' }, started + 2000))
  const server = await startServer({ dataDir: join(dir, 'core') })
  const core = await connectCore({ dataDir: join(dir, 'core') })
  let adapter
  const native = { async sampleHost() { return { available: true, hostRunId: 'cursor-native:run', hostStartedAt: started, profile } }, async readThread() { return { thread: { id: 'cursor-main', parentThreadId: null, source: 'vscode', originator: 'Codex Desktop', path: sourcePath, cwd: dir }, anchorTurnId: 'cursor-turn' } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async close() {} }
  const start = async () => adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'cursor-test', token: 'f'.repeat(64), pollMs: 0 })
  t.after(async () => { await adapter?.close(); await core.close(); await server.close(); await rm(dir, { force: true, recursive: true }) })
  await start()
  const valid = await adapter.read({ thread_id: 'cursor-main' })
  const cursorPath = join(dir, 'adapter', 'cursors', valid.identity.cursorKey + '.json')
  const saved = await readFile(cursorPath, 'utf8')
  assert.ok(Buffer.byteLength(saved) < 16384)
  await adapter.close()
  const cases = [
    ['oversized', 'cursor-over-limit', async () => writeFile(cursorPath, saved + ' '.repeat(20000), { mode: 0o600 })],
    ['malformed', 'cursor-unavailable', async () => writeFile(cursorPath, '{', { mode: 0o600 })],
    ['symbolic link', 'cursor-file-unverified', async () => { const target = join(dir, 'foreign-cursor.json'); await writeFile(target, saved, { mode: 0o600 }); await symlink(target, cursorPath) }],
    ['group-readable', 'cursor-file-unverified', async () => { await writeFile(cursorPath, saved, { mode: 0o600 }); await chmod(cursorPath, 0o644) }],
    ['capture metadata missing', 'cursor-format-invalid', async () => { const broken = JSON.parse(saved); delete broken.captures; await writeFile(cursorPath, JSON.stringify(broken), { mode: 0o600 }) }],
  ]
  for (const [name, gap, arrange] of cases) {
    await t.test(name, async () => {
      await rm(cursorPath, { force: true }); await arrange(); await start()
      const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'f'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'cursor-main' } } }) }).then(value => value.json())
      assert.equal(response.result.structuredContent.observation.gap, gap)
      assert.equal(response.result.structuredContent.observation.status, 'unknown')
      assert.equal(adapter.diagnostics().openReaders, 0)
      assert.equal((await core.snapshot()).sources[0].status, 'disconnected')
      await adapter.close()
    })
  }
})
