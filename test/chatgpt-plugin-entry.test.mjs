import test from 'node:test'
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp, readFile, writeFile, rm, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { prepareInstallation, startFromInstallation } from '../chatgpt/runtime.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

async function installStdioDefinition(prepared) {
  await writeFile(join(prepared.pluginDir, '.codex-plugin/plugin.json'), JSON.stringify({ name: 'ghost-sentinel', version: '0.1.0-dev.1', mcpServers: './.mcp.json', interface: { displayName: 'Sentinel 会话检查' } }), { mode: 0o600 })
  await writeFile(join(prepared.pluginDir, '.mcp.json'), JSON.stringify({ mcpServers: { sentinel: { command: process.execPath, args: [new URL('../chatgpt/plugin-bridge.mjs', import.meta.url).pathname, '--config', prepared.configPath], cwd: new URL('../', import.meta.url).pathname } } }), { mode: 0o600 })
}

test('the controlled plugin entry reuses one verified observer and emits no stdout pretending to be MCP stdio', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-entry-')); let runtime, child
  t.after(async () => { if (child && child.exitCode === null) { child.kill('SIGTERM'); await once(child, 'close') }; await runtime?.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@entry' })
  await installStdioDefinition(prepared)
  runtime = await startFromInstallation({ configPath: prepared.configPath, pollMs: 0, ownership: 'plugin', native: { async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: 1, profile: supportedProfile } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async close() {} } })
  child = spawn(process.execPath, [new URL('../chatgpt/plugin-main.mjs', import.meta.url).pathname, '--config', prepared.configPath], { stdio: ['ignore', 'pipe', 'pipe'] })
  let output = '', diagnostic = '', timer
  child.stdout.on('data', bytes => { output += bytes }); child.stderr.on('data', bytes => { diagnostic += bytes })
  try {
    const ended = once(child, 'close')
    const result = await Promise.race([ended, new Promise((resolve, reject) => { timer = setTimeout(() => { child.kill('SIGTERM'); reject(new Error('controlled plugin connection failed to exit')) }, 5000) })])
    assert.deepEqual(result, [0, null], diagnostic)
  } finally { clearTimeout(timer) }
  assert.equal(output, '')
  assert.equal(JSON.parse(diagnostic.trim()).status, 'reused')
  assert.equal(JSON.parse(diagnostic.trim()).runId, runtime.runId)
  assert.equal(runtime.diagnostics().closed, false)
})

test('a qualified plugin observation process exits naturally after disable with no panels and preserves a peer Core client', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-process-')), dataDir = join(dir, 'adapter')
  const server = await startServer({ dataDir: join(dir, 'core') }), peer = await connectCore({ dataDir: join(dir, 'core') })
  let child
  t.after(async () => { if (child && child.exitCode === null) { child.kill('SIGTERM'); await once(child, 'close') }; await peer.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const dshSource = await peer.registerSource({ registrationId: 'dsh-peer', hostRunId: 'dsh-run', name: 'DSH peer' })
  const prepared = await prepareInstallation({ dataDir, coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@process' })
  await installStdioDefinition(prepared)
  const setting = join(dir, 'enabled.json'); await writeFile(setting, 'true')
  const script = `
    import { readFile } from 'node:fs/promises'
    import { startFromInstallation } from ${JSON.stringify(new URL('../chatgpt/runtime.mjs', import.meta.url).href)}
    import { supportedProfile } from ${JSON.stringify(new URL('../chatgpt/bounded-reader.mjs', import.meta.url).href)}
    const native = {
      async sampleHost() { return { available: true, hostRunId: 'child-host', hostStartedAt: 1, profile: supportedProfile } },
      async readEnablement() { const enabled = JSON.parse(await readFile(${JSON.stringify(setting)}, 'utf8')); return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
      async listThreads() { return { data: [], nextCursor: null } }, async close() {},
    }
    const runtime = await startFromInstallation({ configPath: ${JSON.stringify(prepared.configPath)}, native, pollMs: 20, ownership: 'plugin' })
    await runtime.closed
  `
  child = spawn(process.execPath, ['--input-type=module', '-e', script], { stdio: ['ignore', 'pipe', 'pipe'] })
  const ended = once(child, 'close'); let output = '', errors = '', timer
  child.stdout.on('data', bytes => { output += bytes }); child.stderr.on('data', bytes => { errors += bytes })
  try {
    for (let n = 0; n < 200; n++) {
      if ((await peer.snapshot()).sources.some(value => value.descriptor.registrationId.startsWith('chatgpt:') && value.status === 'connected')) break
      await new Promise(resolve => setTimeout(resolve, 10))
    }
    const source = (await peer.snapshot()).sources.find(value => value.descriptor.registrationId.startsWith('chatgpt:'))
    assert.equal(source?.status, 'connected', errors)
    const owner = JSON.parse(await readFile(join(dataDir, 'runtime.lock/owner.json'), 'utf8'))
    assert.equal(owner.pid, child.pid); assert.equal(owner.ownership, 'plugin')
    await writeFile(setting, 'false')
    const result = await Promise.race([ended, new Promise((resolve, reject) => { timer = setTimeout(() => { child.kill('SIGTERM'); reject(new Error('plugin observer remained resident after disable')) }, 5000) })])
    assert.deepEqual(result, [0, null], errors)
    await assert.rejects(stat(join(dataDir, 'runtime.lock')), { code: 'ENOENT' })
    await assert.rejects(stat(join(dataDir, 'runtime.json')), { code: 'ENOENT' })
    const after = await peer.snapshot()
    assert.equal(after.sources.find(value => value.key === source.key).status, 'disconnected')
    assert.equal(after.sources.find(value => value.key === dshSource).status, 'connected')
    await peer.heartbeat(dshSource)
    assert.equal(output, '')
  } finally { clearTimeout(timer) }
})
