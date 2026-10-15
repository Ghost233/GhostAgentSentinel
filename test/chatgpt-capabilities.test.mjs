import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'

for (const missing of ['conversation-monitoring/1', 'monitoring-failures/1']) test(`material monitoring cannot silently run against a core missing ${missing}`, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-capability-')), path = join(dir, 'main.jsonl'), started = Date.now() - 10_000
  const line = (type, payload) => JSON.stringify({ type, payload, timestamp: new Date(started + 2000).toISOString() }) + '\n'
  await writeFile(path, line('session_meta', { id: 'main' }) + line('event_msg', { type: 'task_started', turn_id: 'run' }))
  const server = await startServer({ dataDir: join(dir, 'core') }), core = await connectCore({ dataDir: join(dir, 'core') })
  const currentHealth = core.health
  core.health = async () => { const value = await currentHealth(); return { ...value, features: value.features.filter(feature => feature !== missing) } }
  let reads = 0
  const native = { async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: started, profile: { appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' } } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async readThread() { reads++; return { thread: { id: 'main', parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', cwd: dir, path }, anchorTurnId: 'run' } }, async close() {} }
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'capability', token: 'a'.repeat(64), collectMaterials: true, pollMs: 0 })
  let diagnostic
  t.after(async () => { await diagnostic?.close(); await adapter.close(); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const view = await adapter.read({ thread_id: 'main' })
  assert.equal(view.observation.gap, 'core-capability-unavailable')
  assert.equal(reads, 0, 'missing required controls must stop source material collection')
  assert.equal((await core.snapshot()).tasks.length, 0)
  diagnostic = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'diagnostic'), installationId: 'diagnostic', token: 'b'.repeat(64), pollMs: 0 })
  const observation = await diagnostic.read({ thread_id: 'main' })
  assert.equal(observation.observation.phase, 'started', 'lifecycle-only diagnostics retain their smaller capability contract')
  assert.equal(observation.analysis.state, 'not-connected')
})
