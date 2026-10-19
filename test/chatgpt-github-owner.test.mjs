import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, writeFile, rm, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createHash } from 'node:crypto'
import { prepareInstallation, loadInstallationConfig, resolvePluginOwner } from '../chatgpt/runtime.mjs'
import { startGitHubOwner } from '../chatgpt/github-plugin-main.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { startServer } from '../src/server.mjs'

async function installation(root, channel) {
  const releaseRoot = join(root, channel, 'payload'), bytes = JSON.stringify({ channel })
  await mkdir(releaseRoot, { recursive: true, mode: 0o700 })
  await writeFile(join(releaseRoot, 'plugin-package.json'), bytes, { mode: 0o600 })
  const prepared = await prepareInstallation({ dataDir: join(root, channel, 'state'), coreDataDir: join(root, 'core'), sessionsRoot: root, metadataSocket: join(root, channel, 'unused.sock'), pluginId: (channel === 'test' ? 'ghost-sentinel-test' : 'ghost-sentinel') + '@ghostsentinel', releaseId: createHash('sha256').update(bytes).digest('hex') })
  return { ...prepared, channel, releaseRoot }
}

test('GitHub owner binds its own release before opening metadata or publishing an endpoint', async t => {
  const root = await mkdtemp(join(tmpdir(), 'sentinel-github-main-'))
  t.after(() => rm(root, { recursive: true, force: true }))
  const prepared = await installation(root, 'stable')
  await writeFile(join(prepared.releaseRoot, 'plugin-package.json'), '{}')
  let calls = 0
  const metadataClient = { request() { calls++; assert.fail('old payload must not request metadata') }, close() { calls++ } }
  await assert.rejects(startGitHubOwner({ ...prepared, metadataClient }), /release identity mismatch/)
  assert.equal(calls, 0)
  await assert.rejects(stat(join(root, 'stable/state/runtime.lock')), { code: 'ENOENT' })
  await assert.rejects(startGitHubOwner({ ...prepared, channel: 'test', metadataClient }), /installation identity mismatch/)
})

test('GitHub stable and test share Core but keep separate owners, qualification and panel identity', async t => {
  const root = await mkdtemp(join(tmpdir(), 'sentinel-github-channels-'))
  const core = await startServer({ dataDir: join(root, 'core') }), owners = []
  t.after(async () => { for (const owner of owners) await owner.close(); await core.close(); await rm(root, { recursive: true, force: true }) })
  const stable = await installation(root, 'stable'), testing = await installation(root, 'test')
  const enabled = { stable: true, test: true }, closes = { stable: 0, test: 0 }
  const options = prepared => ({ ...prepared, metadataClient: { async close() { closes[prepared.channel]++ } }, native: {
    async sampleHost() { return { available: true, hostRunId: 'github-owned-app', hostStartedAt: 1, profile: supportedProfile } },
    async readEnablement() { const value = enabled[prepared.channel] ? 'enabled' : 'disabled'; return { user: value, effective: value } },
    async listThreads() { return { data: [], nextCursor: null } },
    async close() {},
  } })
  const normal = await startGitHubOwner(options(stable)); owners.push(normal)
  const testingOwner = await startGitHubOwner(options(testing)); owners.push(testingOwner)
  assert.notEqual(normal.installationId, testingOwner.installationId)
  assert.notEqual(normal.runId, testingOwner.runId)
  const config = await loadInstallationConfig(stable.configPath), testConfig = await loadInstallationConfig(testing.configPath)
  assert.notEqual(config.token, testConfig.token)
  assert.notEqual(normal.url, testingOwner.url)
  for (const [owner, current, expected] of [[normal, config, 'GhostSentinel 会话检查'], [testingOwner, testConfig, 'GhostSentinel 会话检查 test']]) {
    const response = await fetch(owner.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + current.token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list', params: {} }) }).then(value => value.json())
    assert.ok(response.result.tools.some(tool => tool.title === expected))
    assert.equal((await fetch(owner.url + '/health', { headers: { authorization: 'Bearer ' + current.token } }).then(value => value.json())).releaseId, current.releaseId)
  }
  const duplicate = await startGitHubOwner(options(stable))
  assert.equal(duplicate.reused, true); assert.equal(duplicate.runId, normal.runId)
  await duplicate.close(); assert.equal(closes.stable, 1)
  enabled.test = false; await testingOwner.refresh(); await testingOwner.closed
  assert.equal(closes.test, 1)
  assert.equal((await resolvePluginOwner(stable)).runId, normal.runId)
  await assert.rejects(stat(join(testConfig.dataDir, 'runtime.lock')), { code: 'ENOENT' })
  assert.equal((await loadInstallationConfig(testing.configPath)).installationId, testConfig.installationId, 'disable preserves test data')
  enabled.stable = false; await normal.refresh(); await normal.closed
  assert.equal(closes.stable, 2)
})
