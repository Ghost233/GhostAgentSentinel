import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, readFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import { prepareInstallation, startFromInstallation, resolvePluginOwner } from '../chatgpt/runtime.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { startServer } from '../src/server.mjs'

const directory = tmpdir()
const enabled = { user: 'enabled', effective: 'enabled' }
const host = { available: true, hostRunId: 'local-startup-fixture-only', hostStartedAt: 1, profile: supportedProfile }
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done }); return { promise, resolve } }
async function observe(promise, ms = 1000) {
  const controller = new AbortController()
  try { return await Promise.race([promise, delay(ms, { pending: true }, { signal: controller.signal })]) }
  finally { controller.abort() }
}
async function fixture(t, overrides = {}, { realCore = false, release = () => {} } = {}) {
  const root = await mkdtemp(join(directory, 'sentinel-startup-readiness-'))
  let runtime, server, nativeClosed = 0, materialReads = 0
  const native = {
    async sampleHost() { return host }, async readEnablement() { return enabled },
    async listThreads() { materialReads++; return { data: [], nextCursor: null } },
    async close() { nativeClosed++ }, ...overrides
  }
  t.after(async () => { release(); await runtime?.close(); await server?.close(); await rm(root, { recursive: true, force: true }) })
  await mkdir(join(root, 'sessions'), { mode: 0o700 })
  if (realCore) server = await startServer({ dataDir: join(root, 'core') })
  const prepared = await prepareInstallation({ dataDir: join(root, 'adapter'), coreDataDir: join(root, 'core'), sessionsRoot: join(root, 'sessions'), metadataSocket: join(root, 'nonexistent-fixture.sock'), pluginId: 'sentinel@local-startup-repro' })
  return {
    async start() { runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 20, ownership: 'plugin' }); return runtime },
    async authenticatedOwner() { try { const value = await resolvePluginOwner({ configPath: prepared.configPath }); return { present: Boolean(value) } } catch (error) { return { present: false, error: error.code ?? error.name } } },
    async grayView() {
      const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
      const response = await fetch(runtime.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + config.token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'local-unverified-thread' } } }), signal: AbortSignal.timeout(1000) })
      const value = (await response.json()).result.structuredContent
      return { signal: value.signal, gap: value.observation.gap }
    },
    counts: () => ({ nativeClosed, materialReads })
  }
}

test('enabled initial qualification retains a real authenticated plugin runtime', { timeout: 5000 }, async t => {
  const own = await fixture(t, {}, { realCore: true }), runtime = await own.start()
  await runtime.refresh()
  const owner = await own.authenticatedOwner()
  t.diagnostic('[startup-readiness] ' + JSON.stringify({ scenario: 'enabled', startResolved: true, adapterClosed: runtime.diagnostics().closed, authenticatedOwner: owner.present, realTemporaryCore: true }))
  assert.equal(runtime.diagnostics().closed, false); assert.equal(owner.present, true)
})

test('enabled unsupported profile retains a legal authenticated gray runtime', { timeout: 5000 }, async t => {
  const own = await fixture(t, { async sampleHost() { return { ...host, profile: { ...supportedProfile, appBuild: 'unsupported-local-fixture' } } } }), runtime = await own.start()
  await runtime.refresh()
  const owner = await own.authenticatedOwner(), view = await own.grayView()
  t.diagnostic('[startup-readiness] ' + JSON.stringify({ scenario: 'unsupported-profile', startResolved: true, adapterClosed: runtime.diagnostics().closed, authenticatedOwner: owner.present, view, ...own.counts() }))
  assert.equal(runtime.diagnostics().closed, false); assert.equal(owner.present, true)
  assert.deepEqual(view, { signal: 'gray', gap: 'unsupported-host-version' }); assert.equal(own.counts().materialReads, 0)
})

for (const state of ['disabled', 'unknown', 'host-unavailable']) test(`initial ${state} must not publish ready after qualification has already closed the adapter`, { timeout: 5000 }, async t => {
  const own = await fixture(t, state === 'host-unavailable' ? { async sampleHost() { return { available: false, reason: 'local-fixture-host-unavailable' } } } : { async readEnablement() { return { user: state, effective: state } } })
  let runtime, error
  try { runtime = await own.start() } catch (value) { error = value }
  const atReturn = runtime ? { adapterClosed: runtime.diagnostics().closed, owner: await own.authenticatedOwner() } : null
  const autoClose = runtime ? await observe(runtime.closed) : null
  t.diagnostic('[startup-readiness] ' + JSON.stringify({ scenario: state, startResolved: Boolean(runtime), rejected: Boolean(error), atReturn, autoClose, after: await own.authenticatedOwner(), ...own.counts() }))
  assert.equal(Boolean(runtime), false, 'startup must reject initial qualification loss instead of returning a ready-looking runtime that immediately closes')
})

test('pending initial qualification must not expose authenticated readiness before a later disabled answer', { timeout: 5000 }, async t => {
  const entered = deferred(), answer = deferred()
  const own = await fixture(t, { async readEnablement() { entered.resolve(); return answer.promise } }, { release: () => answer.resolve({ user: 'disabled', effective: 'disabled' }) })
  const starting = own.start()
  assert.notDeepEqual(await observe(entered.promise), { pending: true })
  const beforeAnswer = await observe(starting.then(runtime => ({ runtime })), 100)
  if (beforeAnswer.pending) {
    answer.resolve({ user: 'disabled', effective: 'disabled' })
    await assert.rejects(starting)
    t.diagnostic('[startup-readiness] ' + JSON.stringify({ scenario: 'pending-then-disabled', startupResolvedBeforeQualification: false }))
    return
  }
  const runtime = beforeAnswer.runtime, ownerBeforeAnswer = await own.authenticatedOwner()
  answer.resolve({ user: 'disabled', effective: 'disabled' })
  const autoClose = await observe(runtime.closed)
  t.diagnostic('[startup-readiness] ' + JSON.stringify({ scenario: 'pending-then-disabled', startupResolvedBeforeQualification: true, authenticatedBeforeQualification: ownerBeforeAnswer.present, autoClose, after: await own.authenticatedOwner(), ...own.counts() }))
  assert.fail('runtime was publicly authenticated before initial qualification completed, then naturally closed after its disabled answer')
})

test('startup qualification failure preserves its cause when native cleanup also fails', { timeout: 5000 }, async t => {
  const cleanup = Object.assign(new Error('synthetic native cleanup failed'), { code: 'synthetic_native_cleanup_failed' })
  const own = await fixture(t, { async readEnablement() { return { user: 'disabled', effective: 'disabled' } }, async close() { throw cleanup } })
  await assert.rejects(own.start(), error => error instanceof AggregateError && error.code === 'plugin_startup_qualification_revoked' && error.cause?.code === 'plugin_startup_qualification_revoked' && error.errors[0] === error.cause && error.errors[1] === cleanup)
  assert.equal((await own.authenticatedOwner()).present, false)
})
