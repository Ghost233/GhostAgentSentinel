import test from 'node:test'
import assert from 'node:assert/strict'
import { readFile, stat, mkdtemp, mkdir, writeFile, copyFile, chmod, rm, readdir } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createHash } from 'node:crypto'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { PLUGIN_NODE_SHA256 } from '../chatgpt/plugin-package.mjs'

const root = fileURLToPath(new URL('../', import.meta.url)), execute = promisify(execFile)
const hash = bytes => createHash('sha256').update(bytes).digest('hex')

test('GitHub catalog ships two isolated, self-contained plugin sources with test suffix', async () => {
  const catalog = JSON.parse(await readFile(join(root, '.agents/plugins/marketplace.json')))
  assert.equal(catalog.name, 'ghostsentinel')
  assert.deepEqual(catalog.plugins.map(value => value.name), ['ghost-sentinel', 'ghost-sentinel-test'])
  const digests = [], keys = []
  for (const entry of catalog.plugins) {
    assert.equal(entry.policy.installation, 'AVAILABLE')
    const directory = join(root, 'plugins', entry.name)
    assert.equal(entry.source.path, './plugins/' + entry.name)
    if (entry.source.source === 'git-subdir') assert.equal(entry.source.url, 'https://github.com/Ghost233/GhostAgentSentinel')
    else assert.equal(entry.source.source, 'local')
    const manifest = JSON.parse(await readFile(join(directory, '.codex-plugin/plugin.json')))
    assert.equal(manifest.name, entry.name)
    assert.equal(manifest.interface.displayName, entry.name.endsWith('-test') ? 'GhostSentinel test' : 'GhostSentinel')
    assert.equal(manifest.requires_local_executor, true)
    const servers = JSON.parse(await readFile(join(directory, '.mcp.json'))).mcpServers
    assert.equal(Object.keys(servers).length, 1); keys.push(...Object.keys(servers))
    const launch = Object.values(servers)[0]
    assert.equal(launch.command, './bin/launch-mcp'); assert.equal(launch.cwd, '.')
    assert.ok((await stat(join(directory, launch.command))).mode & 0o111)
    assert.ok((await stat(join(directory, 'payload.tar.gz'))).size < 100 * 1024 * 1024)
    const digest = hash(await readFile(join(directory, 'payload.tar.gz')))
    assert.equal((await readFile(join(directory, 'payload.sha256'), 'utf8')).trim(), digest); digests.push(digest)
    assert.ok((await readFile(join(directory, 'licenses/node-LICENSE'), 'utf8')).includes('Node.js'))
    assert.equal(JSON.stringify({ manifest, servers }).includes('/Users/'), false)
  }
  assert.equal(digests[0], digests[1], 'both Git trees carry the same immutable payload, not cross-plugin relative paths')
  assert.deepEqual(keys, ['ghost_sentinel', 'ghost_sentinel_test'])
  const directory = join(root, 'plugins/ghost-sentinel')
  const packed = JSON.parse((await execute('/usr/bin/tar', ['-xOf', join(directory, 'payload.tar.gz'), './plugin-package.json'], { maxBuffer: 8 * 1024 * 1024 })).stdout)
  assert.ok(packed.notificationPath, 'the distribution includes the existing short-lived native notifier')
  for (const [path, value] of Object.entries(packed.files)) {
    if (path.startsWith('chatgpt/node_modules/') || path.startsWith('runtime/') || path.startsWith('notification/')) continue
    assert.ok(/^(src\/(?:[A-Za-z0-9_-]+\/)*[A-Za-z0-9_.-]+\.(mjs|js|json)|chatgpt\/[^/]+\.(mjs|html|json)|scripts\/manage-chatgpt-plugin.mjs|package(?:-lock)?\.json)$/.test(path), 'finite production source path: ' + path)
    assert.equal(hash(await readFile(join(root, path))), value.sha256, 'published runtime must match final source: ' + path)
  }
  for (const [channel, name] of [['stable', 'ghost-sentinel'], ['test', 'ghost-sentinel-test']]) {
    const actual = await readFile(join(root, 'plugins', name, 'bin/launch-mcp'), 'utf8')
    assert.equal(actual, (await readFile(join(root, 'scripts/templates/github-launch.sh'), 'utf8')).replaceAll('@CHANNEL@', channel).replaceAll('@NODE_SHA256@', PLUGIN_NODE_SHA256))
  }
})

test('installed GitHub launchers unpack and verify without Node/npm or the development checkout', { skip: process.platform !== 'darwin' || process.arch !== 'arm64' }, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'sentinel-github-launch-'))
  t.after(() => rm(directory, { recursive: true, force: true }))
  for (const [channel, name] of [['stable', 'ghost-sentinel'], ['test', 'ghost-sentinel-test']]) {
    const stateRoot = join(directory, channel)
    const { stdout } = await execute(join(root, 'plugins', name, 'bin/launch-mcp'), ['--verify', '--state-root', stateRoot], { cwd: directory, timeout: 60000, maxBuffer: 1048576 })
    const value = JSON.parse(stdout)
    assert.equal(value.status, 'verified'); assert.equal(value.channel, channel); assert.equal(value.launched, false)
    const again = await execute(join(root, 'plugins', name, 'bin/launch-mcp'), ['--verify', '--state-root', stateRoot], { cwd: directory, timeout: 60000, maxBuffer: 1048576 })
    assert.equal(JSON.parse(again.stdout).manifestSHA256, value.manifestSHA256)
    assert.equal((await readdir(join(stateRoot, 'payloads'))).length, 1, 'repeated connections reuse the same private release')
    await assert.rejects(stat(join(stateRoot, 'state/installation.json')), { code: 'ENOENT' })
  }
  const corrupt = join(directory, 'corrupt')
  await mkdir(join(corrupt, 'bin'), { recursive: true })
  for (const file of ['bin/launch-mcp', 'payload.sha256', 'payload.manifest.sha256']) await copyFile(join(root, 'plugins/ghost-sentinel', file), join(corrupt, file))
  await chmod(join(corrupt, 'bin/launch-mcp'), 0o755)
  await writeFile(join(corrupt, 'payload.tar.gz'), 'corrupt archive')
  await mkdir(join(directory, 'bad'), { mode: 0o700 })
  await assert.rejects(execute(join(corrupt, 'bin/launch-mcp'), ['--verify', '--state-root', join(directory, 'bad/stable')], { timeout: 10000 }), error => error.code === 1 && /checksum mismatch/.test(error.stderr))
})
