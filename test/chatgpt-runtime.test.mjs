import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm, stat, chmod } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { prepareInstallation, startFromInstallation } from '../chatgpt/runtime.mjs'

test('the production helper starts from private installation configuration and generates only a conversation MCP entry', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-installation-'))
  const server = await startServer({ dataDir: join(dir, 'core') })
  let runtime
  t.after(async () => { await runtime?.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, pluginId: 'ghost-sentinel@test', metadataSocket: join(dir, 'metadata.sock') })
  assert.equal((await stat(prepared.configPath)).mode & 0o777, 0o600)
  const initial = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  const again = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, pluginId: 'ghost-sentinel@test', metadataSocket: join(dir, 'metadata.sock') })
  assert.equal(JSON.parse(await readFile(again.configPath, 'utf8')).token, initial.token)
  runtime = await startFromInstallation({ configPath: prepared.configPath, pollMs: 0 })
  const manifest = JSON.parse(await readFile(join(prepared.pluginDir, '.codex-plugin/plugin.json'), 'utf8'))
  assert.equal(manifest.name, 'ghost-sentinel')
  const connection = JSON.parse(await readFile(join(prepared.pluginDir, '.mcp.json'), 'utf8'))
  assert.equal(connection.mcpServers.sentinel.url, runtime.url + '/mcp')
  assert.equal(connection.mcpServers.sentinel.http_headers.Authorization, 'Bearer ' + initial.token)
  const response = await fetch(runtime.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + initial.token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list', params: {} }) }).then(value => value.json())
  assert.equal(response.result.tools.length, 1)
  assert.deepEqual(response.result.tools[0]._meta['openai/ui'].entrypoints, [{ type: 'thread' }])
  assert.equal(JSON.stringify(response).includes(initial.token), false)
  await runtime.close()
  await chmod(prepared.configPath, 0o644)
  await assert.rejects(startFromInstallation({ configPath: prepared.configPath }), /private configuration/)
})

test('the internal helper entry runs as a separate process and exits with its own lock released', async t => {
  const { spawn } = await import('node:child_process')
  const { once } = await import('node:events')
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-helper-entry-'))
  const server = await startServer({ dataDir: join(dir, 'core') })
  let child
  t.after(async () => { if (child && child.exitCode === null) { child.kill('SIGTERM'); await once(child, 'exit') }; await server.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, pluginId: 'ghost-sentinel@test', metadataSocket: join(dir, 'missing-metadata.sock') })
  child = spawn(process.execPath, [new URL('../chatgpt/run.mjs', import.meta.url).pathname, '--config', prepared.configPath], { stdio: ['ignore', 'pipe', 'pipe'] })
  let output = '', errors = ''
  child.stdout.on('data', bytes => { output += bytes })
  child.stderr.on('data', bytes => { errors += bytes })
  const deadline = Date.now() + 5000
  while (!output.includes('\n')) { assert.ok(child.exitCode === null, errors); assert.ok(Date.now() < deadline, 'helper must announce its own endpoint'); await new Promise(resolve => setTimeout(resolve, 10)) }
  const ready = JSON.parse(output.trim())
  assert.equal(ready.policy, 'chatgpt-main/1')
  assert.equal(ready.pid, child.pid)
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  assert.equal(output.includes(config.token), false)
  await assert.rejects(startFromInstallation({ configPath: prepared.configPath }), /already running/)
  const exited = once(child, 'exit')
  child.kill('SIGTERM')
  const [code, signal] = await exited
  assert.equal(code, 0, errors)
  assert.equal(signal, null)
  await assert.rejects(stat(join(dir, 'adapter', 'runtime.lock')), { code: 'ENOENT' })
})
