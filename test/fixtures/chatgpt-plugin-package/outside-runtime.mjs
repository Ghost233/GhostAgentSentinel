import { readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'

const [root, directory] = process.argv.slice(2)
const load = relative => import(pathToFileURL(join(root, relative)).href)
const [packaging, installation, host] = await Promise.all([load('chatgpt/plugin-package.mjs'), load('chatgpt/plugin-installation.mjs'), load('chatgpt/plugin-host.mjs')])
const inspected = await packaging.inspectPluginPackage(root)
const managementImports = inspected.root === root && typeof installation.previewPluginChange === 'function' && typeof host.createPluginHost === 'function'
const [{ startServer }, { prepareInstallation, startFromInstallation, resolvePluginOwner }, { supportedProfile }, { Client }, { StdioClientTransport }, { connectCore }] = await Promise.all([
  load('src/server.mjs'), load('chatgpt/runtime.mjs'), load('chatgpt/bounded-reader.mjs'),
  load('chatgpt/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js'),
  load('chatgpt/node_modules/@modelcontextprotocol/sdk/dist/esm/client/stdio.js'),
  load('src/core-client.mjs'),
])
const core = await startServer({ dataDir: join(directory, 'core') })
const coreClient = await connectCore({ dataDir: join(directory, 'core') })
const prepared = await prepareInstallation({ dataDir: join(directory, 'state'), coreDataDir: join(directory, 'core'), sessionsRoot: directory, metadataSocket: join(directory, 'external-native.sock'), pluginId: 'sentinel@packaged-test' })
let runtime, client, transport, nativeClosed = false
try {
  const path = join(directory, 'package-main.jsonl')
  await writeFile(path, JSON.stringify({ type: 'session_meta', payload: { id: 'package-main' } }) + '\n' + JSON.stringify({ type: 'event_msg', timestamp: new Date().toISOString(), payload: { type: 'task_started', turn_id: 'turn' } }) + '\n')
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'package-host', hostStartedAt: 1, profile: supportedProfile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
    async listThreads() { return { data: [], nextCursor: null } },
    async readThread(id) { return { thread: { id, parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', path, cwd: directory }, anchorTurnId: null } },
    async close() { nativeClosed = true },
  }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' })
  await runtime.refresh()
  await writeFile(join(prepared.pluginDir, '.codex-plugin/plugin.json'), JSON.stringify({ name: 'ghost-sentinel', version: '0.1.0-dev.1', mcpServers: './.mcp.json' }), { mode: 0o600 })
  await writeFile(join(prepared.pluginDir, '.mcp.json'), JSON.stringify({ mcpServers: { sentinel: { command: join(root, 'runtime/node'), args: [join(root, 'chatgpt/plugin-bridge.mjs'), '--config', prepared.configPath], cwd: root } } }), { mode: 0o600 })
  client = new Client({ name: 'package-runtime-test', version: '1' })
  transport = new StdioClientTransport({ command: join(root, 'runtime/node'), args: [join(root, 'chatgpt/plugin-bridge.mjs'), '--config', prepared.configPath], cwd: directory, env: { HOME: directory, PATH: '/usr/bin:/bin' }, stderr: 'inherit' })
  await client.connect(transport)
  const result = await client.callTool({ name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'package-main' } })
  const resources = await client.listResources(), panel = await client.readResource({ uri: resources.resources[0].uri })
  const owner = await resolvePluginOwner({ configPath: prepared.configPath })
  const connected = (await coreClient.snapshot()).sources.some(source => source.status === 'connected')
  await client.close(); client = null
  await runtime.close()
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  const after = await coreClient.snapshot()
  process.stdout.write(JSON.stringify({ version: process.version, sourceRootSeen: Object.values(config).some(value => typeof value === 'string' && value.includes('GhostCodexSentinel')), ownerPid: owner.pid, pid: process.pid, currentThread: result.structuredContent.identity.threadId, panel: panel.contents.some(value => value.text?.includes('Sentinel')), managementImports, disconnected: connected && after.sources.length > 0 && after.sources.every(source => source.status === 'disconnected') && nativeClosed && await resolvePluginOwner({ configPath: prepared.configPath }) === null }) + '\n')
} finally { await client?.close(); await transport?.close(); await runtime?.close(); await coreClient.close(); await core.close() }
