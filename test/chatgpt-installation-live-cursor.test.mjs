import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { syncBuiltinESMExports } from 'node:module'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import { previewPluginChange, applyPluginChange } from '../chatgpt/plugin-installation.mjs'
import { saveCursor } from '../chatgpt/cursor-store.mjs'
import { resolvePluginOwner } from '../chatgpt/runtime.mjs'
import { installationWorld } from './fixtures/chatgpt-plugin-installation/world.mjs'

const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done }); return { promise, resolve } }
async function installed(t) {
  const f = await installationWorld(t), first = await f.packageFixture('0.2.0-dev.1')
  assert.equal((await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot: first }, f.io), f.io)).status, 'complete')
  const directory = join(f.installRoot, 'state/cursors')
  await fs.mkdir(directory, { recursive: true, mode: 0o700 })
  return { ...f, directory, configPath: join(f.installRoot, 'state/installation.json') }
}

for (const vanish of [false, true]) test(`a live atomic cursor ${vanish ? 'rename during inspection' : 'save'} preserves the approved upgrade`, { timeout: 30000 }, async t => {
  const f = await installed(t), second = await f.packageFixture('0.2.0-dev.2'), owner = await resolvePluginOwner({ configPath: f.configPath })
  const options = { ...f.options, operation: 'upgrade', packageRoot: second }, plan = await previewPluginChange(options, f.io)
  const path = join(f.directory, 'd'.repeat(64) + '.json'), entered = deferred(), release = deferred(), rename = fs.rename, lstat = fs.lstat
  let saving, temporary
  fs.rename = async (from, to, ...args) => { if (to === path) { temporary = from; entered.resolve(); await release.promise }; return rename(from, to, ...args) }
  syncBuiltinESMExports()
  try {
    saving = saveCursor(path, { offset: 0 }, new Map(), -1)
    await entered.promise
    if (vanish) { fs.lstat = async (target, ...args) => { if (target === temporary) { release.resolve(); await saving }; return lstat(target, ...args) }; syncBuiltinESMExports() }
    const calls = f.host.calls.length, current = await previewPluginChange(options, f.io)
    assert.equal(current.canApply, true, JSON.stringify(current.blockers)); assert.equal(current.stateDigest, plan.stateDigest)
    assert.equal(f.host.calls.length, calls)
    const same = await resolvePluginOwner({ configPath: f.configPath }); assert.equal(same.pid, owner.pid); assert.equal(same.runId, owner.runId)
  } finally { release.resolve(); try { await saving } finally { fs.rename = rename; fs.lstat = lstat; syncBuiltinESMExports() } }
  const upgrade = await applyPluginChange(plan, f.io)
  assert.equal(upgrade.status, 'complete', JSON.stringify(upgrade))
  const removed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io)
  assert.equal(removed.status, 'complete'); assert.equal(removed.processes.naturalExit, true); await f.core.heartbeat(f.peer)
})

test('a live-looking cursor temporary left after shutdown is preserved before any purge', { timeout: 30000 }, async t => {
  const f = await installed(t), path = join(f.directory, 'd'.repeat(64) + '.json.' + randomUUID() + '.tmp'), bytes = 'self-owned diagnostic marker'
  await fs.writeFile(path, bytes, { mode: 0o600 })
  const plan = await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io)
  assert.equal(plan.canApply, true, JSON.stringify(plan.blockers))
  let purgeConnections = 0
  f.io.connectCore = async () => { purgeConnections++; assert.fail('a leftover temporary must be detected before Core purge') }
  let failed
  try { failed = await applyPluginChange(plan, f.io) } finally { delete f.io.connectCore }
  assert.equal(failed.status, 'recovery-required'); assert.equal(failed.errorCode, 'installation_files_unconfirmed_after_stop')
  assert.equal(purgeConnections, 0); assert.equal(await fs.readFile(path, 'utf8'), bytes)
  assert.equal(await resolvePluginOwner({ configPath: f.configPath }), null)
  const stopped = await previewPluginChange({ ...f.options, operation: 'uninstall', resumeOperationId: failed.operationId }, f.io)
  assert.equal(stopped.canApply, false)
  await fs.rm(path)
  const removed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall', resumeOperationId: failed.operationId }, f.io), f.io)
  assert.equal(removed.status, 'complete'); await f.core.heartbeat(f.peer)
})

test('unknown, malformed, symlink and nonprivate cursor temporaries remain blocked while an owner is live', { timeout: 30000 }, async t => {
  const f = await installed(t), target = join(f.dir, 'foreign-marker'), bytes = 'preserve the foreign target'
  await fs.writeFile(target, bytes, { mode: 0o600 })
  for (const kind of ['unknown-name', 'invalid-uuid', 'symlink', 'permissions']) {
    const name = kind === 'unknown-name' ? 'foreign.tmp' : 'd'.repeat(64) + '.json.' + (kind === 'invalid-uuid' ? 'not-a-uuid' : randomUUID()) + '.tmp', path = join(f.directory, name)
    if (kind === 'symlink') await fs.symlink(target, path)
    else await fs.writeFile(path, bytes, { mode: kind === 'permissions' ? 0o644 : 0o600 })
    const calls = f.host.calls.length, plan = await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io)
    assert.equal(plan.canApply, false, kind); assert.equal(f.host.calls.length, calls)
    assert.equal(await fs.readFile(target, 'utf8'), bytes); await fs.rm(path)
  }
  const removed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io)
  assert.equal(removed.status, 'complete'); await f.core.heartbeat(f.peer)
})
