import assert from 'node:assert/strict'
import { mkdtemp, realpath, mkdir, copyFile, chmod, writeFile, readFile, rm, lstat } from 'node:fs/promises'
import { constants } from 'node:fs'
import { createHash } from 'node:crypto'
import { createReadStream } from 'node:fs'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { tmpdir } from 'node:os'
import { startServer } from '../../../src/server.mjs'
import { connectCore } from '../../../src/core-client.mjs'
import { supportedProfile } from '../../../chatgpt/bounded-reader.mjs'
import { resourceMaterial } from '../chatgpt-plugin-lazy/resource-material.mjs'

const root = fileURLToPath(new URL('../../../', import.meta.url)), hash = data => createHash('sha256').update(data).digest('hex')
async function fileHash(path) { const value = createHash('sha256'); for await (const bytes of createReadStream(path)) value.update(bytes); return value.digest('hex') }

// Filesystem/transaction fault loop: no observer, Core, model, App or native component is launched.
export async function unlaunchedWorld(t) {
  const dir = await realpath(await mkdtemp(join(tmpdir(), 'sentinel-installation-fault-'))), installRoot = join(dir, 'install'), packageRoot = join(dir, 'payload'), digest = 'a'.repeat(64)
  await mkdir(packageRoot, { mode: 0o700 }); await writeFile(join(packageRoot, 'marker'), 'owned package marker', { mode: 0o600 })
  const host = { state: { pluginId: 'ghost-sentinel@fault-loop', installed: false, enabled: 'disabled', revision: 'r0', pluginPath: null, marketplaceRegistered: false }, revision: 0,
    async inspect() { return structuredClone(this.state) },
    async register() { this.state = { ...this.state, installed: true, enabled: 'enabled', marketplaceRegistered: true, pluginPath: join(installRoot, 'state/plugin'), revision: 'r' + ++this.revision }; return this.inspect() },
    async setEnabled(value, revision) { assert.equal(revision, this.state.revision); this.state.enabled = value ? 'enabled' : 'disabled'; this.state.revision = 'r' + ++this.revision; return this.inspect() },
    async unregister() { this.state = { ...this.state, installed: false, enabled: 'disabled', marketplaceRegistered: false, pluginPath: null, revision: 'r' + ++this.revision }; return this.inspect() },
  }
  const options = { operation: 'install', installRoot, packageRoot, pluginId: host.state.pluginId, pluginName: 'ghost-sentinel', marketplaceName: 'fault-loop', coreDataDir: join(dir, 'core'), sessionsRoot: join(dir, 'sessions'), metadataSocket: join(dir, 'metadata.sock') }
  const io = { host, timeoutMs: 1, async inspectPackage(root) { return { root, digest, manifest: { version: '0.2.0-dev.1', runtimeVersion: '0.1.0-dev.1', files: { marker: {} } }, nodePath: join(root, 'runtime/node'), bridgePath: join(root, 'chatgpt/plugin-bridge.mjs'), ownerPath: join(root, 'chatgpt/plugin-main.mjs'), notificationApp: null } } }
  t.after(() => rm(dir, { recursive: true, force: true }))
  return { dir, installRoot, options, io, host, marker: join(installRoot, 'releases', digest, 'marker') }
}

export async function installationWorld(t) {
  const dir = await realpath(await mkdtemp(join(tmpdir(), 'sentinel-installation-world-'))), installRoot = join(dir, 'install'), statePath = join(dir, 'fake-host.json')
  const hostState = { enabled: false, hostStartedAt: Date.now() - 10000 }, children = []
  await writeFile(statePath, JSON.stringify(hostState), { mode: 0o600 })
  await writeFile(join(dir, 'resource-main.jsonl'), resourceMaterial({ at: Date.now(), cwd: dir, producerVersion: supportedProfile.cliVersion }))
  const server = await startServer({ dataDir: join(dir, 'core'), leaseMs: 60000 }), core = await connectCore({ dataDir: join(dir, 'core') })
  const peer = await core.registerSource({ registrationId: 'dsh-installation-test', hostRunId: 'peer', name: 'Owned DSH peer' })
  await core.ingest(peer, { tasks: [{ id: 'dsh-peer', lifecycleId: 'peer', title: 'Owned peer', status: 'running' }] })
  const host = { state: { pluginId: 'ghost-sentinel@managed-test', installed: false, enabled: 'disabled', revision: 'r0', pluginPath: null, marketplaceRegistered: false }, revision: 0, calls: [], failNextRegister: false, failNextEnable: null, autoStart: true,
    async inspect() { return structuredClone(this.state) },
    async register() { this.calls.push('register'); if (this.failNextRegister) { this.failNextRegister = false; throw Object.assign(new Error('Owned registration failure'), { code: 'owned_registration_failed' }) }; this.state = { ...this.state, installed: true, enabled: 'enabled', marketplaceRegistered: true, pluginPath: join(installRoot, 'state/plugin'), revision: 'r' + ++this.revision }; return this.inspect() },
    async setEnabled(enabled, expected) {
      this.calls.push('setEnabled:' + enabled)
      if (expected !== this.state.revision) throw Object.assign(new Error('Owned config conflict'), { code: 'host_config_conflict' })
      if (enabled && this.failNextEnable) { const code = this.failNextEnable; this.failNextEnable = null; throw Object.assign(new Error('Owned enable fault'), { code }) }
      this.state.enabled = enabled ? 'enabled' : 'disabled'; this.state.revision = 'r' + ++this.revision
      hostState.enabled = enabled; await writeFile(statePath, JSON.stringify(hostState), { mode: 0o600 })
      if (enabled && this.autoStart) {
        const launch = JSON.parse(await readFile(join(installRoot, 'state/plugin/.mcp.json'), 'utf8')).mcpServers.sentinel
        const child = spawn(launch.command, launch.args, { cwd: launch.cwd, stdio: ['pipe', 'pipe', 'pipe'] })
        child.ended = once(child, 'close'); child.errors = ''; child.stderr.on('data', data => { child.errors += data }); child.stdout.on('data', () => {}); child.stdin.on('error', () => {}); children.push(child)
        child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'owned-installation-host', version: '1' } } }) + '\n')
      }
      return this.inspect()
    },
    async unregister() { this.calls.push('unregister'); this.state = { ...this.state, installed: false, enabled: 'disabled', marketplaceRegistered: false, pluginPath: null, revision: 'r' + ++this.revision }; return this.inspect() },
  }
  const nodeHash = await fileHash(process.execPath)
  async function packageFixture(version, { notificationExecutable } = {}) {
    const packageRoot = join(dir, 'package-' + version)
    await mkdir(join(packageRoot, 'runtime'), { recursive: true, mode: 0o700 }); await mkdir(join(packageRoot, 'chatgpt'), { mode: 0o700 })
    await copyFile(process.execPath, join(packageRoot, 'runtime/node'), constants.COPYFILE_FICLONE); await chmod(join(packageRoot, 'runtime/node'), 0o700)
    const owner = `
      import { readFile } from 'node:fs/promises'
      import { join } from 'node:path'
      import { startFromInstallation } from ${JSON.stringify(new URL('../../../chatgpt/runtime.mjs', import.meta.url).href)}
      const configPath = process.argv[3], config = JSON.parse(await readFile(configPath, 'utf8'))
      const read = async () => JSON.parse(await readFile(${JSON.stringify(statePath)}, 'utf8'))
      const initial = await read(), thread = { id:'resource-main',parentThreadId:null,forkedFromId:null,source:'vscode',originator:'Codex Desktop',path:join(config.sessionsRoot,'resource-main.jsonl'),cwd:config.sessionsRoot }
      const native = { async sampleHost(){return {available:true,hostRunId:'installation-test-host',hostStartedAt:initial.hostStartedAt,profile:${JSON.stringify(supportedProfile)}}},async readEnablement(){const enabled=(await read()).enabled;return {user:enabled?'enabled':'disabled',effective:enabled?'enabled':'disabled'}},async listThreads(){return {data:[thread],nextCursor:null}},async readThread(){return {thread,anchorTurnId:'resource-turn'}},async close(){} }
      const notificationIO = config.notificationBundlePath ? {async settings(){return {authorization:'denied',alertSetting:'disabled',notificationCenterSetting:'disabled',observedAt:Date.now()}},async snapshot(){return {pending:[],delivered:[],actions:[],observedAt:Date.now()}},async close(){}} : undefined
      const runtime = await startFromInstallation({configPath,native,notificationIO,pollMs:100,ownership:'plugin'})
      if(!runtime.reused) await runtime.closed
    `
    const bridge = `
      import { spawn } from 'node:child_process'
      import { once } from 'node:events'
      import { startPluginBridge } from ${JSON.stringify(new URL('../../../chatgpt/plugin-bridge.mjs', import.meta.url).href)}
      const configPath=process.argv[3]
      const bridge=await startPluginBridge({configPath, async startOwner(path){const child=spawn(process.execPath,[new URL('./plugin-main.mjs',import.meta.url).pathname,'--config',path],{detached:true,stdio:'ignore'});await once(child,'spawn');child.unref()}})
      await bridge.closed
    `
    await writeFile(join(packageRoot, 'chatgpt/plugin-main.mjs'), owner, { mode: 0o600 }); await writeFile(join(packageRoot, 'chatgpt/plugin-bridge.mjs'), bridge, { mode: 0o600 })
    const manifest = { format: 1, version, runtimeVersion: '0.1.0-dev.1', files: { 'runtime/node': { sha256: nodeHash }, 'chatgpt/plugin-main.mjs': { sha256: hash(owner) }, 'chatgpt/plugin-bridge.mjs': { sha256: hash(bridge) } } }
    if (notificationExecutable) {
      manifest.notificationPath = 'notification/GhostSentinelNotifications.app'
      const key = manifest.notificationPath + '/Contents/MacOS/GhostSentinelNotifications'
      await mkdir(join(packageRoot, manifest.notificationPath, 'Contents/MacOS'), { recursive: true, mode: 0o700 }); await copyFile(notificationExecutable, join(packageRoot, key)); await chmod(join(packageRoot, key), 0o700)
      manifest.files[key] = { sha256: await fileHash(join(packageRoot, key)) }
    }
    await writeFile(join(packageRoot, 'plugin-package.json'), JSON.stringify(manifest), { mode: 0o600 })
    return packageRoot
  }
  async function inspectPackage(path, expected = {}) {
    const bytes = await readFile(join(path, 'plugin-package.json')), manifest = JSON.parse(bytes), digest = hash(bytes)
    if (expected.digest) assert.equal(digest, expected.digest)
    for (const [key, value] of Object.entries(manifest.files)) { assert.ok((await lstat(join(path, key))).isFile()); assert.equal(await fileHash(join(path, key)), value.sha256) }
    return { root: await realpath(path), digest, manifest, nodePath: join(path, 'runtime/node'), bridgePath: join(path, 'chatgpt/plugin-bridge.mjs'), ownerPath: join(path, 'chatgpt/plugin-main.mjs'), notificationApp: manifest.notificationPath ? join(path, manifest.notificationPath) : null }
  }
  t.after(async () => {
    hostState.enabled = false; await writeFile(statePath, JSON.stringify(hostState)).catch(() => {})
    let forcedCleanup = false
    for (const child of children) if (child.exitCode === null && child.signalCode === null) {
      const timer = setTimeout(() => { forcedCleanup = true; child.kill('SIGTERM') }, 7000)
      try { await child.ended } finally { clearTimeout(timer) }
    }
    await core.close(); await server.close(); await rm(dir, { recursive: true, force: true })
    assert.equal(forcedCleanup, false, 'forced fixture cleanup cannot count as lifecycle success')
  })
  const options = { operation: 'install', installRoot, pluginName: 'ghost-sentinel', marketplaceName: 'managed-test', pluginId: host.state.pluginId, coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'unused.sock') }
  return { dir, installRoot, options, io: { host, inspectPackage }, host, core, peer, packageFixture, children }
}
