import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, readFile, mkdir, writeFile, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { randomUUID } from 'node:crypto'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { prepareInstallation, startFromInstallation, HELPER_PROTOCOL, HELPER_VERSION } from '../chatgpt/runtime.mjs'
import { createLazyCore } from '../chatgpt/lazy-core.mjs'

const off = () => ({ async sampleHost() { return { available: false, reason: 'execution-source-unavailable' } }, async readEnablement() { assert.fail('App off must not read plugin state') }, async close() {} })
async function installation(t) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-helper-runtime-')), owned = []
  t.after(async () => {
    const results = await Promise.allSettled(owned.map(value => Promise.resolve().then(() => value.close())))
    const failures = results.filter(result => result.status === 'rejected').map(result => result.reason)
    try { await rm(dir, { recursive: true, force: true }) } catch (error) { failures.push(error) }
    if (failures.length) {
      const message = 'Runtime fixture cleanup failed: ' + failures.map(error => error.message).join('; ')
      t.diagnostic(message)
      throw new AggregateError(failures, message)
    }
  })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'missing.sock'), pluginId: 'ghost-sentinel@test' })
  const start = async () => { const runtime = await startFromInstallation({ configPath: prepared.configPath, native: off(), pollMs: 0 }); owned.push(runtime); return runtime }
  return { dir, prepared, owned, start }
}

test('concurrent helper starts share verified identity and duplicate close cannot stop the owner', async t => {
  const f = await installation(t), values = await Promise.all([f.start(), f.start(), f.start()])
  assert.equal(values.filter(value => !value.reused).length, 1)
  assert.equal(new Set(values.map(value => value.url)).size, 1)
  assert.equal(new Set(values.map(value => value.runId)).size, 1)
  await Promise.all(values.filter(value => value.reused).map(value => value.close()))
  const config = JSON.parse(await readFile(f.prepared.configPath, 'utf8'))
  const response = await fetch(values[0].url + '/health', { headers: { authorization: 'Bearer ' + config.token } })
  assert.equal(response.status, 200)
  assert.equal((await response.json()).runId, values[0].runId)
  assert.equal((await fetch(values[0].url + '/health')).status, 401)
  await Promise.all(values.map(value => value.close()))
  await assert.rejects(stat(join(f.dir, 'adapter/runtime.lock')), { code: 'ENOENT' })
  await assert.rejects(stat(join(f.dir, 'core/runtime.json')), { code: 'ENOENT' })
})

test('a foreign runtime endpoint is refused even when the matching owner record claims a dead process', async t => {
  const f = await installation(t), first = await f.start(), url = first.url
  await first.close()
  const child = spawn(process.execPath, ['-e', ''], { stdio: 'ignore' }); await once(child, 'exit')
  const config = JSON.parse(await readFile(f.prepared.configPath, 'utf8'))
  const owner = { protocol: HELPER_PROTOCOL, version: HELPER_VERSION, installationId: config.installationId, pid: child.pid, runId: randomUUID() }
  const lock = join(f.dir, 'adapter/runtime.lock')
  await mkdir(lock, { mode: 0o700 }); await writeFile(join(lock, 'owner.json'), JSON.stringify(owner), { mode: 0o600 })
  const foreign = { ...owner, installationId: 'foreign-installation', url }
  await writeFile(join(f.dir, 'adapter/runtime.json'), JSON.stringify(foreign), { mode: 0o600 })
  await assert.rejects(f.start(), /untrusted helper endpoint/)
  assert.deepEqual(JSON.parse(await readFile(join(f.dir, 'adapter/runtime.json'), 'utf8')), foreign)
})

test('a demonstrably dead owned helper recovers the same port without replacing its token', async t => {
  const f = await installation(t), first = await f.start(), url = first.url
  await first.close()
  const config = JSON.parse(await readFile(f.prepared.configPath, 'utf8'))
  const child = spawn(process.execPath, ['-e', ''], { stdio: 'ignore' }); await once(child, 'exit')
  const owner = { protocol: HELPER_PROTOCOL, version: HELPER_VERSION, installationId: config.installationId, pid: child.pid, runId: randomUUID() }
  const lock = join(f.dir, 'adapter/runtime.lock')
  await mkdir(lock, { mode: 0o700 }); await writeFile(join(lock, 'owner.json'), JSON.stringify(owner), { mode: 0o600 })
  await writeFile(join(f.dir, 'adapter/runtime.json'), JSON.stringify({ ...owner, url }), { mode: 0o600 })
  const recovered = await f.start()
  assert.equal(recovered.reused, false); assert.equal(recovered.url, url)
  assert.equal(JSON.parse(await readFile(f.prepared.configPath, 'utf8')).token, config.token)
})

test('only qualified health attaches Core and a stale client reconnects without global controls', async () => {
  let connects = 0, closes = 0, stale = false
  const client = index => ({ async health() { if (stale && index === 1) throw Object.assign(new Error('old endpoint closed'), { code: 'ECONNREFUSED' }); return { instanceId: 'core-' + index, features: [] } }, async close() { closes++ }, async unregisterSource() { return null }, async snapshot() { return { sources: [], tasks: [] } }, async cancelPending() {} })
  const core = createLazyCore({ dataDir: '/synthetic-core', connect: async () => client(++connects) })
  await core.unregisterSource('old-own-source'); await core.setNativeNotificationEligibility({ appRunning: false }); await core.snapshot()
  assert.equal(connects, 0)
  assert.equal(core.pause, undefined); assert.equal(core.configureAnalysis, undefined)
  const [a, b] = await Promise.all([core.health(), core.health()])
  assert.equal(connects, 1); assert.equal(a.instanceId, b.instanceId)
  stale = true
  await core.unregisterSource('old-own-source')
  assert.equal(connects, 1, 'standby cleanup cannot reconnect')
  assert.equal((await core.health()).instanceId, 'core-2')
  assert.equal(connects, 2); assert.equal(closes, 1)
  await core.close(); await core.close()
  assert.equal(closes, 2)
})

test('closing a pending lazy attachment cancels startup and never adopts a late client', async () => {
  let attempted = false, clientClosed = 0
  const core = createLazyCore({ dataDir: '/synthetic-core', connect: async ({ signal }) => {
    attempted = true
    await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }))
    return { health: async () => ({ instanceId: 'late' }), close: async () => { clientClosed++ } }
  } })
  const pending = core.health().catch(error => error)
  await core.close()
  assert.equal(attempted, true); assert.equal(clientClosed, 1)
  assert.ok(await pending instanceof Error)
  assert.equal(core.transportDiagnostics().pendingRequests, 0)
})

test('cancelPending cancels attachment without closing the facade or starting a replacement', async t => {
  let connects = 0
  const core = createLazyCore({ dataDir: '/synthetic-core', connect: async ({ signal }) => {
    connects++
    if (connects === 1) await new Promise((resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }))
    return { async health() { return { instanceId: 'next-authorized-health' } }, async close() {}, async cancelPending() {} }
  } })
  t.after(() => core.close())
  const pending = core.health().catch(error => error)
  await core.cancelPending()
  let timeout
  try {
    const settled = await Promise.race([pending.then(() => true), new Promise(resolve => { timeout = setTimeout(() => resolve(false), 60) })])
    assert.equal(settled, true, 'cancelPending must release a blocked startup before adapter shutdown waits for it')
  } finally { clearTimeout(timeout) }
  assert.equal(connects, 1, 'cancellation must not respawn Core')
  assert.equal((await core.health()).instanceId, 'next-authorized-health')
  assert.equal(connects, 2)
})

test('an occupied stable endpoint fails without moving the installation or touching Core', async t => {
  const { createServer } = await import('node:http')
  const f = await installation(t), owner = await f.start(), url = owner.url
  await owner.close()
  const config = JSON.parse(await readFile(f.prepared.configPath, 'utf8'))
  const occupied = createServer((request, response) => response.end('foreign owner'))
  await new Promise(resolve => occupied.listen(config.port, '127.0.0.1', resolve))
  t.after(() => new Promise(resolve => { occupied.closeAllConnections(); occupied.close(resolve) }))
  await assert.rejects(f.start(), { code: 'EADDRINUSE' })
  assert.equal(JSON.parse(await readFile(f.prepared.configPath, 'utf8')).port, config.port)
  assert.equal(JSON.parse(await readFile(join(f.prepared.pluginDir, '.mcp.json'), 'utf8')).mcpServers.sentinel.url, url + '/mcp')
  await assert.rejects(stat(join(f.dir, 'adapter/runtime.lock')), { code: 'ENOENT' })
  await assert.rejects(stat(join(f.dir, 'core/runtime.json')), { code: 'ENOENT' })
})

test('an unknown live helper endpoint cannot be reused or replaced', async t => {
  const { createServer } = await import('node:http')
  const f = await installation(t), config = JSON.parse(await readFile(f.prepared.configPath, 'utf8'))
  const foreign = createServer((request, response) => { response.setHeader('content-type', 'application/json'); response.end(JSON.stringify({ protocol: HELPER_PROTOCOL, installationId: 'other-installation', pid: process.pid, runId: randomUUID(), version: HELPER_VERSION })) })
  await new Promise(resolve => foreign.listen(0, '127.0.0.1', resolve)); t.after(() => new Promise(resolve => { foreign.closeAllConnections(); foreign.close(resolve) }))
  const owner = { protocol: HELPER_PROTOCOL, installationId: config.installationId, pid: process.pid, runId: randomUUID(), version: HELPER_VERSION }
  const lock = join(f.dir, 'adapter/runtime.lock'); await mkdir(lock, { mode: 0o700 }); await writeFile(join(lock, 'owner.json'), JSON.stringify(owner), { mode: 0o600 })
  const record = { ...owner, url: 'http://127.0.0.1:' + foreign.address().port }
  await writeFile(join(f.dir, 'adapter/runtime.json'), JSON.stringify(record), { mode: 0o600 })
  await assert.rejects(f.start(), /identity mismatch/)
  assert.deepEqual(JSON.parse(await readFile(join(lock, 'owner.json'), 'utf8')), owner)
  assert.deepEqual(JSON.parse(await readFile(join(f.dir, 'adapter/runtime.json'), 'utf8')), record)
})

test('the helper CLI signal closes once and bounded timeout exits without exposing errors', async t => {
  const { copyFile } = await import('node:fs/promises')
  for (const timeout of [false, true]) await t.test(timeout ? 'bounded-close-timeout' : 'normal-signal-close', async t => {
    const dir = await mkdtemp(join(tmpdir(), 'sentinel-helper-cli-')), marker = join(dir, 'closed.json'), config = join(dir, 'installation.json')
    let child
    t.after(async () => { if (child && child.exitCode === null && child.signalCode === null) { child.kill('SIGKILL'); await once(child, 'close') }; await rm(dir, { recursive: true, force: true }) })
    await copyFile(new URL('../chatgpt/helper-main.mjs', import.meta.url), join(dir, 'helper-main.mjs'))
    await writeFile(config, '{}', { mode: 0o600 })
    await writeFile(join(dir, 'runtime.mjs'), `import fs from 'node:fs/promises'; export async function startFromInstallation({configPath}){if(configPath!==${JSON.stringify(config)})throw new Error('wrong config'); const keep=setInterval(()=>{},1000);return {protocol:'ghost-sentinel-chatgpt-helper/1',installationId:'synthetic-installation',runId:'synthetic-run',pid:process.pid,version:'0.1.0-dev.1',url:'http://127.0.0.1:1',reused:false,async close(){await fs.writeFile(${JSON.stringify(marker)},'closed');${timeout ? 'await new Promise(()=>{})' : 'clearInterval(keep)'}}}}`)
    child = spawn(process.execPath, [join(dir, 'helper-main.mjs'), '--config', config], { stdio: ['ignore', 'pipe', 'pipe'] })
    let output = '', errors = ''; child.stdout.on('data', bytes => { output += bytes }); child.stderr.on('data', bytes => { errors += bytes })
    const ended = once(child, 'close'), deadline = Date.now() + 5000
    while (!output.includes('\n')) { assert.equal(child.exitCode, null, errors); assert.ok(Date.now() < deadline); await new Promise(resolve => setTimeout(resolve, 5)) }
    child.kill('SIGTERM')
    const [code, signal] = await ended
    assert.equal(signal, null); assert.equal(code, timeout ? 1 : 0)
    assert.equal(await readFile(marker, 'utf8'), 'closed')
    assert.equal(output.includes(dir) || errors.includes(dir), false)
    assert.equal(JSON.stringify({ output, errors }).includes('wrong config'), false)
    assert.match(timeout ? errors : output, timeout ? /helper_close_timeout/ : /stopped/)
  })
})

test('installation maintenance blocks Native before qualification and survives helper restart', async t => {
  const f = await installation(t), config = JSON.parse(await readFile(f.prepared.configPath, 'utf8'))
  const marker = { format: 1, installationId: config.installationId, operationId: randomUUID() }, path = join(f.dir, 'adapter/maintenance.json')
  await writeFile(path, JSON.stringify(marker), { mode: 0o600 })
  let hostReads = 0
  const native = () => ({ async sampleHost() { hostReads++; return { available: false, reason: 'execution-source-unavailable' } }, async readEnablement() { assert.fail('maintenance cannot query metadata') }, async close() {} })
  let runtime = await startFromInstallation({ configPath: f.prepared.configPath, native: native(), pollMs: 0 }); f.owned.push(runtime)
  await runtime.refresh()
  assert.equal(hostReads, 0, 'maintenance must be checked before opening Native')
  const url = runtime.url
  await runtime.close()
  assert.deepEqual(JSON.parse(await readFile(path, 'utf8')), marker)
  runtime = await startFromInstallation({ configPath: f.prepared.configPath, native: native(), pollMs: 0 }); f.owned.push(runtime)
  await runtime.refresh(); assert.equal(hostReads, 0); assert.equal(runtime.url, url)
  await rm(path)
  await runtime.refresh()
  assert.equal(hostReads, 1, 'removing the installer-owned marker resumes normal qualification on the same endpoint')
  await assert.rejects(stat(join(f.dir, 'core/runtime.json')), { code: 'ENOENT' })
})

test('polling and authenticated reads fail closed for unconfirmed maintenance markers', async t => {
  const { chmod, symlink, rename } = await import('node:fs/promises')
  const f = await installation(t), config = JSON.parse(await readFile(f.prepared.configPath, 'utf8')), path = join(f.dir, 'adapter/maintenance.json')
  const marker = { format: 1, installationId: config.installationId, operationId: randomUUID() }
  let hostReads = 0, disconnects = 0
  await writeFile(path, JSON.stringify(marker), { mode: 0o600 })
  const native = { async sampleHost() { hostReads++; return { available: false, reason: 'execution-source-unavailable' } }, async readEnablement() { assert.fail('maintenance cannot query plugin metadata') }, async listThreads() { assert.fail('maintenance cannot query sessions') }, async disconnectMetadata() { disconnects++ }, async close() {} }
  const runtime = await startFromInstallation({ configPath: f.prepared.configPath, native, pollMs: 20 }); f.owned.push(runtime)
  const deadline = Date.now() + 1000
  while (disconnects < 2 && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10))
  assert.ok(disconnects >= 2, 'real scheduled polls must reach the maintenance gate')
  assert.equal(hostReads, 0)
  const read = async () => {
    const response = await fetch(runtime.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + config.token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'synthetic-maintenance-main' } } }) })
    return (await response.json()).result.structuredContent
  }
  assert.equal((await read()).observation.gap, 'installation-maintenance')
  const cases = [
    ['malformed', temporary => writeFile(temporary, '{', { mode: 0o600 })],
    ['wrong-installation', temporary => writeFile(temporary, JSON.stringify({ ...marker, installationId: 'foreign-installation' }), { mode: 0o600 })],
    ['invalid-operation', temporary => writeFile(temporary, JSON.stringify({ ...marker, operationId: 'unknown' }), { mode: 0o600 })],
    ['non-private', async temporary => { await writeFile(temporary, JSON.stringify(marker), { mode: 0o600 }); await chmod(temporary, 0o644) }],
    ['oversized', temporary => writeFile(temporary, JSON.stringify(marker) + ' '.repeat(1025), { mode: 0o600 })],
    ['symlink', async temporary => { const target = join(f.dir, 'marker-target.json'); await writeFile(target, JSON.stringify(marker), { mode: 0o600 }); await symlink(target, temporary) }],
  ]
  for (const [name, create] of cases) {
    // Absence permits Native qualification, so polling must always see a marker.
    const temporary = path + '.' + randomUUID() + '.tmp'
    await create(temporary); await rename(temporary, path)
    // Drain any poll that sampled the previous marker before asserting this one.
    await runtime.refresh()
    assert.equal((await read()).observation.gap, 'installation-maintenance-unconfirmed', name)
    assert.equal(hostReads, 0, name + ' must not sample Native')
    await assert.rejects(stat(join(f.dir, 'core/runtime.json')), { code: 'ENOENT' })
  }
  await rm(path)
  await runtime.refresh()
  assert.equal((await read()).observation.gap, 'execution-source-unavailable')
  assert.ok(hostReads > 0)
})

test('the formal helper refuses a stage01 dev0 owner before any endpoint or Native request', async t => {
  const f = await installation(t), config = JSON.parse(await readFile(f.prepared.configPath, 'utf8'))
  const old = { protocol: HELPER_PROTOCOL, installationId: config.installationId, pid: process.pid, runId: randomUUID(), version: '0.1.0-dev.0' }
  const lock = join(f.dir, 'adapter/runtime.lock')
  await mkdir(lock, { mode: 0o700 }); await writeFile(join(lock, 'owner.json'), JSON.stringify(old), { mode: 0o600 })
  await writeFile(join(f.dir, 'adapter/runtime.json'), JSON.stringify({ ...old, url: 'http://127.0.0.1:12345' }), { mode: 0o600 })
  let requests = 0
  t.mock.method(globalThis, 'fetch', async () => { requests++; return Response.json(old) })
  await assert.rejects(f.start(), /untrusted helper owner/)
  assert.equal(requests, 0)
  assert.deepEqual(JSON.parse(await readFile(join(lock, 'owner.json'), 'utf8')), old)
})


test('only a new private installation receives a one-shot initial enrollment nonce and port writes keep it consumed', async t => {
  const f = await installation(t), before = JSON.parse(await readFile(f.prepared.configPath, 'utf8'));
  assert.match(before.initialEnrollmentBaseline.id, /^[a-f0-9-]{36}$/);
  assert.equal(before.initialEnrollmentBaseline.claimable, true);
  const first = await f.start(), consumed = JSON.parse(await readFile(f.prepared.configPath, 'utf8'));
  assert.deepEqual(consumed.initialEnrollmentBaseline, { id: before.initialEnrollmentBaseline.id, claimable: false });
  assert.equal(consumed.port, Number(new URL(first.url).port));
  await first.close();
  await f.start();
  assert.deepEqual(JSON.parse(await readFile(f.prepared.configPath, 'utf8')).initialEnrollmentBaseline, consumed.initialEnrollmentBaseline);
});

test('re-preparing a legacy installation never manufactures a complete initial enrollment baseline', async t => {
  const f = await installation(t), original = JSON.parse(await readFile(f.prepared.configPath, 'utf8'));
  delete original.initialEnrollmentBaseline;
  await writeFile(f.prepared.configPath, JSON.stringify(original), { mode: 0o600 });
  await prepareInstallation({ dataDir: original.dataDir, coreDataDir: original.coreDataDir, sessionsRoot: original.sessionsRoot, metadataSocket: original.metadataSocket, pluginId: original.pluginId });
  const prepared = JSON.parse(await readFile(f.prepared.configPath, 'utf8'));
  assert.equal(Object.hasOwn(prepared, 'initialEnrollmentBaseline'), false);
  assert.equal(prepared.installationId, original.installationId);
  await f.start();
  assert.equal(Object.hasOwn(JSON.parse(await readFile(f.prepared.configPath, 'utf8')), 'initialEnrollmentBaseline'), false);
});
