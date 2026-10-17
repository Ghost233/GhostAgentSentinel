import test from 'node:test'
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { prepareInstallation, startFromInstallation } from '../chatgpt/runtime.mjs'

async function guarded(prepared, script) {
  const events = join(prepared.pluginDir, 'import-events.jsonl')
  const child = spawn(process.execPath, ['--import', new URL('./fixtures/chatgpt-plugin-lazy/import-guard.mjs', import.meta.url).pathname, '--input-type=module', '-e', script], { stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, SENTINEL_LAZY_GUARD_ROOT: new URL('../chatgpt/', import.meta.url).href, SENTINEL_LAZY_GUARD_OWNER: join(prepared.pluginDir, '../runtime.lock/owner.json'), SENTINEL_LAZY_GUARD_EVENTS: events } })
  const ended = once(child, 'close'); let output = '', errors = '', timer
  child.stdout.on('data', bytes => { output += bytes }); child.stderr.on('data', bytes => { errors += bytes })
  try {
    const result = await Promise.race([ended, new Promise((resolve, reject) => { timer = setTimeout(() => { child.kill('SIGTERM'); reject(new Error('guarded owned test did not exit')) }, 5000) })])
    return { result, output, errors, events }
  } finally { clearTimeout(timer) }
}

test('authenticated owner lookup remains available when collection modules cannot be loaded', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-lazy-lookup-')); let runtime
  t.after(async () => { await runtime?.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@lazy-lookup' })
  runtime = await startFromInstallation({ configPath: prepared.configPath, pollMs: 0, ownership: 'plugin', native: { async close() {} } })
  const script = `
    import assert from 'node:assert/strict'
    const { resolvePluginOwner } = await import(${JSON.stringify(new URL('../chatgpt/runtime.mjs', import.meta.url).href)})
    const owner = await resolvePluginOwner({ configPath: ${JSON.stringify(prepared.configPath)} })
    assert.equal(owner.runId, ${JSON.stringify(runtime.runId)})
    assert.equal(owner.pid, ${runtime.pid})
    assert.equal(owner.ownership, 'plugin')
  `
  const child = await guarded(prepared, script)
  assert.deepEqual(child.result, [0, null], child.errors)
  assert.equal(child.output, '')
  await assert.rejects(stat(child.events), { code: 'ENOENT' })
  assert.equal(runtime.diagnostics().closed, false)
})

test('an owner collection import failure occurs after taking its own lock and releases it before Core construction', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-lazy-failure-'))
  t.after(() => rm(dir, { recursive: true, force: true }))
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@lazy-failure' })
  const script = `
    import assert from 'node:assert/strict'
    const { startFromInstallation } = await import(${JSON.stringify(new URL('../chatgpt/runtime.mjs', import.meta.url).href)})
    await assert.rejects(startFromInstallation({ configPath: ${JSON.stringify(prepared.configPath)}, pollMs: 0, ownership: 'plugin', native: { async sampleHost() { assert.fail('failed bootstrap cannot sample the host') } } }), error => error.code === 'owned_import_failure')
  `
  const child = await guarded(prepared, script)
  assert.deepEqual(child.result, [0, null], child.errors)
  const events = (await readFile(child.events, 'utf8')).trim().split('\n').map(line => JSON.parse(line))
  assert.ok(events.length > 0); assert.ok(events.every(event => event.ownedLock))
  await assert.rejects(stat(join(dir, 'adapter/runtime.lock')), { code: 'ENOENT' })
  await assert.rejects(stat(join(dir, 'adapter/runtime.json')), { code: 'ENOENT' })
  await assert.rejects(stat(join(dir, 'core')), { code: 'ENOENT' })
})
