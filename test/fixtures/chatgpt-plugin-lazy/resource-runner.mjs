import assert from 'node:assert/strict'
import { mkdtemp, readFile, writeFile, rename, rm, mkdir } from 'node:fs/promises'
import { spawn, execFile } from 'node:child_process'
import { once } from 'node:events'
import { join, isAbsolute } from 'node:path'
import { tmpdir } from 'node:os'
import { pathToFileURL, fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import { setTimeout as delay } from 'node:timers/promises'
import { resourceMaterial } from './resource-material.mjs'

const [variantRoot, label, reportDir, warmupArgument = '30000'] = process.argv.slice(2)
assert.ok(isAbsolute(variantRoot) && isAbsolute(reportDir) && /^[a-z0-9-]+$/.test(label ?? ''))
const warmupMs = Number(warmupArgument)
assert.ok(Number.isSafeInteger(warmupMs) && warmupMs >= 1000 && warmupMs <= 60000)
const execute = promisify(execFile), children = [], live = new Set(), samples = [], cycleResults = []
const dir = await mkdtemp(join(tmpdir(), 'sentinel-real-bridge-resource-')), statePath = join(dir, 'enabled.json')
await mkdir(reportDir, { recursive: true })
const deadline = async (promise, message, ms = 10000) => {
  const controller = new AbortController()
  try { return await Promise.race([promise, delay(ms, null, { signal: controller.signal }).then(() => assert.fail(message))]) }
  finally { controller.abort() }
}
const state = async enabled => { const pending = statePath + '.tmp'; await writeFile(pending, JSON.stringify(enabled), { mode: 0o600 }); await rename(pending, statePath) }
const exited = child => child.exitCode !== null || child.signalCode !== null
function track(child) {
  children.push(child); child.ended = once(child, 'close'); child.errors = ''
  child.stderr.on('data', bytes => { child.errors = (child.errors + bytes).slice(-16384) })
  return child
}
const memory = async pid => JSON.parse((await execute('python3', [fileURLToPath(new URL('../../../scripts/memory-footprint.py', import.meta.url)), String(pid)], { timeout: 2000, maxBuffer: 16384 })).stdout)
let server, client, owner, peer, heartbeat, prepared, failure
const report = { label, variantRoot, node: process.version, warmupMs, officialMCP: true, realModelCalls: 0, modelRoutes: {}, noAppOperations: true, samples, cycleResults, passed: false }
try {
  const { startServer } = await import(pathToFileURL(join(variantRoot, 'src/server.mjs')).href)
  const { connectCore } = await import(pathToFileURL(join(variantRoot, 'src/core-client.mjs')).href)
  const { prepareInstallation, resolvePluginOwner } = await import(pathToFileURL(join(variantRoot, 'chatgpt/runtime.mjs')).href)
  const { supportedProfile } = await import(pathToFileURL(join(variantRoot, 'chatgpt/bounded-reader.mjs')).href)
  await state(true)
  server = await startServer({ dataDir: join(dir, 'core'), leaseMs: 60000 })
  client = await connectCore({ dataDir: join(dir, 'core') })
  peer = await client.registerSource({ registrationId: 'dsh-resource-peer', hostRunId: 'owned-peer-run', name: 'Owned DSH peer' })
  await client.ingest(peer, { tasks: [{ id: 'peer-task', lifecycleId: 'peer-turn', title: 'Owned peer', status: 'running' }] })
  heartbeat = setInterval(() => { void client.heartbeat(peer).catch(() => {}) }, 1000)
  prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'synthetic-unused.sock'), pluginId: 'sentinel@resource-fixture' })
  await writeFile(join(prepared.pluginDir, '.codex-plugin/plugin.json'), JSON.stringify({ name: 'ghost-sentinel', version: '0.1.0-dev.1', mcpServers: './.mcp.json' }), { mode: 0o600 })
  await writeFile(join(prepared.pluginDir, '.mcp.json'), JSON.stringify({ mcpServers: { sentinel: { command: process.execPath, args: [join(variantRoot, 'chatgpt/plugin-bridge.mjs'), '--config', prepared.configPath], cwd: variantRoot } } }), { mode: 0o600 })
  await writeFile(join(dir, 'resource-main.jsonl'), resourceMaterial({ at: Date.now() - 1000, cwd: dir, producerVersion: supportedProfile.cliVersion }))
  owner = track(spawn(process.execPath, [fileURLToPath(new URL('./resource-owner.mjs', import.meta.url)), variantRoot, prepared.configPath, statePath], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] }))
  const [identity] = await deadline(once(owner, 'message'), 'owned resource observer did not start')
  assert.equal(identity.pid, owner.pid); assert.equal(identity.reused, false)
  report.owner = identity
  const initialHealth = await client.health()
  async function bridge() {
    const child = track(spawn(process.execPath, [join(variantRoot, 'chatgpt/plugin-bridge.mjs'), '--config', prepared.configPath], { cwd: variantRoot, stdio: ['pipe', 'pipe', 'pipe'] }))
    let buffer = '', sequence = 0
    const pending = new Map()
    child.stdout.on('data', bytes => {
      buffer += bytes
      assert.ok(buffer.length <= 2 * 1024 * 1024)
      while (buffer.includes('\n')) {
        const end = buffer.indexOf('\n'), message = JSON.parse(buffer.slice(0, end)); buffer = buffer.slice(end + 1)
        if (pending.has(message.id)) { pending.get(message.id)(message); pending.delete(message.id) }
      }
    })
    const request = async (method, params = {}) => {
      const id = ++sequence, response = new Promise(resolve => { pending.set(id, resolve) })
      child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n')
      const message = await deadline(response, 'resource bridge request failed: ' + method + ': ' + child.errors)
      assert.equal(message.error, undefined, JSON.stringify(message.error)); return message.result
    }
    const result = { child, async eof() {
      child.stdin.end(); assert.deepEqual(await deadline(child.ended, 'resource bridge did not exit on EOF'), [0, null], child.errors); live.delete(result)
    } }
    const initialized = await request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'owned-resource-fixture', version: '1' } })
    assert.equal(initialized.serverInfo.name, 'ghost-sentinel-chatgpt')
    child.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n')
    assert.ok((await request('tools/list')).tools.some(tool => tool.name === 'sentinel_current_chat'))
    assert.equal((await request('tools/call', { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'resource-main' } })).structuredContent.identity.threadId, 'resource-main')
    live.add(result); return result
  }
  async function sample(phase, point) {
    const entries = await Promise.all([...live].map(async value => ({ role: 'bridge', ...await memory(value.child.pid) })))
    entries.push({ role: 'owner', ...await memory(owner.pid) })
    const value = { at: new Date().toISOString(), phase, point, bridgeCount: live.size, entries }
    samples.push(value); await writeFile(join(reportDir, label + '-samples.json'), JSON.stringify(samples, null, 2) + '\n')
  }
  for (const count of [1, 4, 20]) {
    await Promise.all(Array.from({ length: count - live.size }, bridge))
    await delay(warmupMs)
    for (let point = 0; point < 3; point++) { if (point) await delay(2000); await sample('clients-' + count, point) }
    assert.equal((await resolvePluginOwner({ configPath: prepared.configPath })).runId, identity.runId)
    process.stdout.write(JSON.stringify({ label, phase: 'clients-' + count, samples: 3, ownerPid: owner.pid }) + '\n')
  }
  await [...live][0].eof()
  for (let cycle = 1; cycle <= 100; cycle++) {
    const connection = await bridge(); await connection.eof()
    assert.equal(live.size, 19)
    const current = await resolvePluginOwner({ configPath: prepared.configPath })
    assert.equal(current.runId, identity.runId); assert.equal(current.pid, identity.pid)
    cycleResults.push({ cycle, exitedNaturally: true })
    if (cycle % 20 === 0) process.stdout.write(JSON.stringify({ label, phase: 'cycles', cycle }) + '\n')
  }
  await sample('after-100-cycles', 0)
  assert.deepEqual(await client.analysisConfig(), {})
  const beforeStop = await client.snapshot()
  assert.equal(beforeStop.tasks.find(task => task.id === 'resource-main').currentRequest, 'Read this synthetic resource fixture only')
  await state(false)
  assert.deepEqual(await deadline(owner.ended, 'disabled resource observer did not exit'), [0, null], owner.errors)
  for (const value of live) assert.deepEqual(await deadline(value.child.ended, 'disabled resource bridge did not exit'), [0, null], value.child.errors)
  live.clear()
  assert.ok(children.every(exited))
  const afterStop = await client.snapshot()
  assert.equal(afterStop.sources.find(source => source.key === peer).status, 'connected')
  assert.equal(afterStop.tasks.find(task => task.sourceKey === peer).status, 'running')
  await client.heartbeat(peer)
  assert.equal((await client.health()).instanceId, initialHealth.instanceId)
  report.remainingChildren = children.filter(child => !exited(child)).length
  report.startedBridgeProcesses = children.length - 1
  report.dshPeerRetained = true; report.coreInstancePreserved = true; report.passed = true
} catch (error) { failure = error; report.error = { name: error.name, message: error.message }; process.exitCode = 1 }
finally {
  clearInterval(heartbeat)
  await state(false).catch(() => {})
  for (const child of children) if (!exited(child)) {
    child.stdin?.end()
    try { await deadline(child.ended, 'owned resource cleanup deadline', 5000) }
    catch { report.forcedCleanup = true; report.passed = false; process.exitCode = 1; child.kill('SIGTERM'); await deadline(child.ended, 'owned resource process did not stop after signal', 5000) }
  }
  await client?.close(); await server?.close(); await rm(dir, { recursive: true, force: true })
  await writeFile(join(reportDir, label + '-result.json'), JSON.stringify(report, null, 2) + '\n')
}
if (failure) throw failure
