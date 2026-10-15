import assert from 'node:assert/strict'
import plugin from 'ghost-agent-sentinel'
import { readFile, writeFile, access, realpath } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { join, dirname } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { createHash } from 'node:crypto'

// This file is copied into an isolated npm receiver before execution. Product
// imports therefore use that receiver's ordinary package resolution.
const [loaderURL, expectedPackage, dataDir] = process.argv.slice(2)
const receiver = dirname(fileURLToPath(import.meta.url))
assert.equal(await realpath(join(receiver, 'node_modules/ghost-agent-sentinel')), expectedPackage)
assert.ok(dataDir.startsWith(receiver + '/'))
const hostPath = fileURLToPath(import.meta.resolve('ghost-agent-sentinel'))
const clientPath = fileURLToPath(import.meta.resolve('ghost-agent-sentinel/client'))
const packagePath = fileURLToPath(import.meta.resolve('ghost-agent-sentinel/package.json'))
for (const path of [hostPath, clientPath, packagePath]) {
  assert.ok(path.startsWith(expectedPackage + '/'), 'product exports must come from the installed package')
  await access(path)
}
const manifest = JSON.parse(await readFile(packagePath, 'utf8'))
assert.equal(hostPath, join(expectedPackage, 'dsh/host.mjs'))
assert.equal(clientPath, join(expectedPackage, 'dsh/client.js'))
const { req } = await import(loaderURL)
const nativeFingerprint = async () => Object.fromEntries(await Promise.all(['cordis', 'dsh-llm', 'dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop'].map(async name => {
  const path = req.resolve(`@deepseek-ai/${name}`)
  return [name, { path, sha256: createHash('sha256').update(await readFile(path)).digest('hex') }]
})))
const nativeInputs = await nativeFingerprint()
const { Context } = req('@deepseek-ai/cordis')
const ctx = new Context()
const alive = pid => { try { process.kill(pid, 0); return true } catch (error) { if (error.code === 'ESRCH') return false; throw error } }
const gone = async path => { try { await access(path); return false } catch (error) { if (error.code === 'ENOENT') return true; throw error } }
async function until(predicate, deadline, message) {
  while (!await predicate()) { assert.ok(Date.now() < deadline, message); await delay(50) }
}
const registrationId = 'owned-packed-package-context'
let service, runtime, connected, before, closeResult, after, failure = null
const cleanupFailures = []
const startedAt = Date.now()
try {
  await ctx.plugin(req('@deepseek-ai/dsh-llm').default)
  for (const name of ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop']) {
    await ctx.plugin(req(`@deepseek-ai/${name}`).default, { agents: [] })
  }
  assert.equal(ctx.agents.list().length, 0)
  await ctx.plugin(plugin, { dataDir, registrationId })
  service = ctx.get('sentinel')
  await until(() => service?.connected, Date.now() + 12000, 'installed adapter did not connect to its own core')
  connected = await service.snapshot()
  const source = connected.sources.find(value => value.key === connected.adapter.sourceKey)
  assert.equal(connected.adapter.connected, true)
  assert.equal(source.status, 'connected')
  assert.equal(source.descriptor.registrationId, registrationId)
  assert.equal(connected.sources.length, 1)
  assert.equal(connected.tasks.length, 0)
  assert.deepEqual(await service.analysisConfig(), {})
  runtime = JSON.parse(await readFile(join(dataDir, 'runtime.json'), 'utf8'))
  const { protocol, instanceId, pid, url } = runtime
  assert.equal(protocol, 1)
  assert.ok(Number.isSafeInteger(pid) && pid !== process.pid && alive(pid))
  assert.match(url, /^http:\/\/127\.0\.0\.1:\d+$/)
  await writeFile(join(receiver, 'runtime-owned.json'), JSON.stringify({ protocol, instanceId, pid, url }))
  const diagnosticsResponse = await fetch(`${url}/v1/diagnostics`, { headers: { authorization: `Bearer ${runtime.token}` }, redirect: 'error', signal: AbortSignal.timeout(1000) })
  assert.equal(diagnosticsResponse.status, 200)
  const diagnostics = await diagnosticsResponse.json()
  assert.equal(diagnostics.instanceId, instanceId)
  assert.equal(diagnostics.runtime.connectedSources, 1)
  assert.equal(diagnostics.runtime.activeAnalyses, 0)
  assert.equal(diagnostics.http.listening, true)
  before = { core: { protocol, instanceId, pid, url }, sourceKey: source.key, registrationId,
    sourceStatus: source.status, tasks: connected.tasks.length, adapter: service.diagnostics(), diagnostics }
} catch (error) {
  failure = String(error.message)
} finally {
  for (const close of [async () => { closeResult = await service?.close() }, () => ctx.fiber.dispose()]) {
    try { await close() } catch (error) { cleanupFailures.push(String(error.message)) }
  }
  try {
    if (runtime) {
      await until(async () => !alive(runtime.pid) && await gone(join(dataDir, 'runtime.json')), Date.now() + 16000, 'installed core did not stop naturally after its last source closed')
      assert.equal(await gone(join(dataDir, 'core.lock')), true)
      assert.equal(await gone(join(dataDir, 'startup.lock')), true)
      await assert.rejects(fetch(`${runtime.url}/v1/health`, { signal: AbortSignal.timeout(1000) }))
    }
    after = { adapter: service?.diagnostics(), corePidAlive: runtime ? alive(runtime.pid) : null,
      runtimeRemoved: await gone(join(dataDir, 'runtime.json')), coreLockRemoved: await gone(join(dataDir, 'core.lock')), startupLockRemoved: await gone(join(dataDir, 'startup.lock')) }
    if (service) {
      assert.equal(closeResult.status, 'closed')
      assert.equal(closeResult.timedOut, false)
      assert.equal(after.adapter.adapter.activeRequests, 0)
      assert.equal(after.adapter.adapter.timers, 0)
      assert.equal(after.adapter.adapter.handlers, 0)
      assert.equal(after.adapter.adapter.subscriptions, 0)
    }
    assert.deepEqual(await nativeFingerprint(), nativeInputs, 'native SDK inputs changed during the installed package lifecycle')
  } catch (error) { cleanupFailures.push(String(error.message)) }
}
const result = { node: process.version, manifest: { name: manifest.name, version: manifest.version }, exports: { hostPath, clientPath, packagePath },
  nativeInputs, before, closeResult, after, failure, cleanupFailures, elapsedMs: Date.now() - startedAt,
  passed: !failure && !cleanupFailures.length && !!before && !!after && after.corePidAlive === false,
  scope: 'Installed package in an isolated npm receiver, native in-memory SDK Context, own production core process; no Desktop/CLI application assembly or model inference' }
await writeFile(join(receiver, 'runtime-proof.json'), JSON.stringify(result, null, 2) + '\n')
console.log('PACKED_RUNTIME_RESULT:' + JSON.stringify(result))
process.exitCode = result.passed ? 0 : 1
