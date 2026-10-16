import WebSocket from 'ws'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { stat } from 'node:fs/promises'
import { homedir } from 'node:os'
import { join } from 'node:path'

const execute = promisify(execFile)
const appPath = '/Applications/ChatGPT.app'
const nativePath = appPath + '/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex'
const appExecutable = appPath + '/Contents/MacOS/ChatGPT'
const readonlyMethods = new Set(['thread/read', 'thread/turns/list', 'thread/list', 'config/read'])
const metadataMessages = {
  deadline: 'metadata request timed out',
  'metadata-rpc-error': 'metadata request failed',
  'metadata-unavailable': 'metadata connection unavailable',
  'metadata-send-failed': 'metadata send failed',
  'metadata-invalid-response': 'metadata response invalid',
  cancelled: 'metadata client closed',
}
const metadataError = code => Object.assign(new Error(metadataMessages[code] ?? 'metadata operation failed'), { code, stage: 'metadata' })

// The locked desktop version prefixes its subcommand with global config
// overrides. ps presents a command string, so keep quoted and nested TOML
// values together; ambiguous text is not execution-domain proof.
function commandToken(text, start) {
  while (/\s/.test(text[start] ?? '') && start < text.length) start++
  if (start >= text.length) return null
  let quote = null, escaped = false
  const nesting = []
  let at = start
  for (; at < text.length; at++) {
    const character = text[at]
    if (escaped) { escaped = false; continue }
    if (quote) {
      if (quote === '"' && character === '\\') escaped = true
      else if (character === quote) quote = null
      continue
    }
    if (character === '"' || character === "'") quote = character
    else if (character === '[' || character === '{') nesting.push(character)
    else if (character === ']' || character === '}') {
      if (nesting.pop() !== (character === ']' ? '[' : '{')) return null
    } else if (/\s/.test(character) && !nesting.length) break
  }
  if (quote || escaped || nesting.length) return null
  return { text: text.slice(start, at), end: at }
}

function executionSubcommand(command) {
  if (typeof command !== 'string' || command.length > 65536 || !command.startsWith(nativePath + ' ')) return null
  let at = nativePath.length
  while (at < command.length) {
    const token = commandToken(command, at)
    if (!token) return null
    at = token.end
    if (token.text === '-c' || token.text === '--config') {
      const value = commandToken(command, at)
      if (!value || value.text.indexOf('=') < 1) return null
      at = value.end
    } else if (token.text.startsWith('--config=')) {
      if (token.text.slice('--config='.length).indexOf('=') < 1) return null
    } else return token.text.startsWith('-') ? null : token.text
  }
  return null
}

// Connect only to the existing metadata service. This object has no API for
// launching an App, resuming a thread or writing configuration.
export function createMetadataClient({ socketPath = join(homedir(), '.codex/app-server-control/app-server-control.sock') } = {}) {
  let connection = null, opening = null, closed = false, sequence = 0, initialized = false
  const pending = new Map()
  const fail = (code = 'metadata-unavailable') => { for (const entry of pending.values()) { clearTimeout(entry.timer); entry.reject(metadataError(code)) }; pending.clear(); initialized = false }
  async function dispose(ws) {
    if (!ws || ws.readyState === WebSocket.CLOSED) return
    await new Promise(resolve => { const timer = setTimeout(() => { ws.terminate() }, 1000); ws.once('close', () => { clearTimeout(timer); resolve() }); ws.close() })
  }
  async function wire(method, params) {
    if (closed) throw metadataError('cancelled')
    if (connection?.readyState !== WebSocket.OPEN) throw metadataError('metadata-unavailable')
    return await new Promise((resolve, reject) => {
      const id = ++sequence
      const timer = setTimeout(() => { pending.delete(id); reject(metadataError('deadline')) }, 5000)
      pending.set(id, { resolve, reject, timer })
      connection.send(JSON.stringify({ jsonrpc: '2.0', id, method, params }), error => { if (!error) return; clearTimeout(timer); pending.delete(id); reject(metadataError('metadata-send-failed')) })
    })
  }
  async function connect() {
    if (closed) throw metadataError('cancelled')
    if (connection?.readyState === WebSocket.OPEN && initialized) return
    if (opening) return opening
    opening = (async () => {
      const ws = new WebSocket(`ws+unix://${socketPath}:/`, { handshakeTimeout: 3000, maxPayload: 1024 * 1024 })
      connection = ws
      ws.on('error', () => { if (connection === ws) fail() })
      ws.on('close', () => { if (connection === ws) { fail(); connection = null } })
      ws.on('message', bytes => {
        let message
        try { message = JSON.parse(bytes) } catch { if (connection === ws) fail('metadata-invalid-response'); ws.terminate(); return }
        const entry = pending.get(message?.id)
        if (!entry) return
        pending.delete(message.id); clearTimeout(entry.timer)
        if (message.error) entry.reject(metadataError('metadata-rpc-error'))
        else entry.resolve(message.result)
      })
      try {
        await new Promise((resolve, reject) => { ws.once('open', resolve); ws.once('error', () => reject(metadataError('metadata-unavailable'))) })
        await wire('initialize', { clientInfo: { name: 'ghost_sentinel_chatgpt', version: '0.1.0-dev.0' }, capabilities: { experimentalApi: true } })
        if (closed || connection !== ws) throw metadataError(closed ? 'cancelled' : 'metadata-unavailable')
        ws.send(JSON.stringify({ method: 'initialized' })); initialized = true
      } catch (error) {
        if (connection === ws) { fail(); connection = null }
        await dispose(ws)
        throw error
      }
    })().finally(() => { opening = null })
    return opening
  }
  return {
    async request(method, params) { if (!readonlyMethods.has(method)) throw new Error('non-observation method rejected'); await connect(); return wire(method, params) },
    diagnostics: () => ({ closed, pendingRequests: pending.size, deadlineTimers: pending.size, connected: connection?.readyState === WebSocket.OPEN }),
    async close() {
      if (closed) return
      closed = true; fail('cancelled')
      const ws = connection; connection = null
      await dispose(ws)
    },
  }
}

export function createNativeIO({ pluginId, socketPath, processSample, profileSample } = {}) {
  if (typeof pluginId !== 'string' || !pluginId) throw new Error('plugin identity required')
  const rpc = createMetadataClient({ socketPath })
  let profileCache = null, profileSampling = null, closed = false
  async function sampleProfile() {
    if (profileSample) return profileSample()
    if (profileSampling) return profileSampling
    profileSampling = (async () => {
      const plist = appPath + '/Contents/Info.plist'
      const [infoStat, binaryStat] = await Promise.all([stat(plist), stat(nativePath)])
      const signature = [infoStat.dev, infoStat.ino, infoStat.size, infoStat.mtimeMs, binaryStat.dev, binaryStat.ino, binaryStat.size, binaryStat.mtimeMs].join(':')
      if (profileCache?.signature === signature) return profileCache.value
      const [info, version] = await Promise.all([
        execute('/usr/bin/plutil', ['-convert', 'json', '-o', '-', plist], { timeout: 3000, maxBuffer: 262144 }),
        execute(nativePath, ['--version'], { timeout: 3000, maxBuffer: 16384 }),
      ])
      const app = JSON.parse(info.stdout)
      const value = { appVersion: app.CFBundleShortVersionString, appBuild: app.CFBundleVersion, cliVersion: version.stdout.trim().replace(/^codex-cli /, '') }
      profileCache = { signature, value }; return value
    })().finally(() => { profileSampling = null })
    return profileSampling
  }
  async function sampleProcesses() {
    if (processSample) return processSample()
    const result = await execute('/bin/ps', ['-ww', '-axo', 'pid=,ppid=,lstart=,comm='], { env: { ...process.env, TZ: 'UTC', LC_ALL: 'C' }, timeout: 3000, maxBuffer: 4 * 1024 * 1024 })
    const inventory = result.stdout.split('\n').flatMap(line => {
      const match = line.trim().match(/^(\d+)\s+(\d+)\s+(.{24})\s+(.+)$/)
      return match ? [{ pid: Number(match[1]), ppid: Number(match[2]), started: match[3], executable: match[4] }] : []
    })
    const apps = inventory.filter(row => row.executable === appExecutable)
    const appIds = new Set(apps.map(row => row.pid))
    const candidates = inventory.filter(row => row.executable === nativePath && appIds.has(row.ppid))
    if (!candidates.length) return apps.map(row => ({ ...row, command: row.executable }))
    // Only the identified App-owned native executables need argument reads.
    const args = await execute('/bin/ps', ['-ww', '-p', candidates.map(row => row.pid).join(','), '-o', 'pid=,args='], { timeout: 3000, maxBuffer: 65536 })
    const commands = new Map(args.stdout.split('\n').flatMap(line => { const match = line.trim().match(/^(\d+)\s+(.+)$/); return match ? [[Number(match[1]), match[2]]] : [] }))
    return [...apps.map(row => ({ ...row, command: row.executable })), ...candidates.map(row => ({ ...row, command: commands.get(row.pid) ?? '' }))]

  }
  return {
    async sampleHost() {
      if (closed) return { available: false, reason: 'source-closed' }
      try {
        const [processes, profile] = await Promise.all([sampleProcesses(), sampleProfile()])
        const apps = new Set(processes.filter(row => row.command === appExecutable || row.command.startsWith(appExecutable + ' ')).map(row => row.pid))
        const candidates = processes.filter(row => apps.has(row.ppid) && executionSubcommand(row.command) === 'app-server')
        if (candidates.length !== 1) return { available: false, reason: candidates.length ? 'execution-domain-ambiguous' : 'execution-source-unavailable' }
        const native = candidates[0], hostStartedAt = Date.parse(native.started + ' UTC')
        if (!Number.isSafeInteger(hostStartedAt)) return { available: false, reason: 'source-generation-unverified' }
        return { available: true, hostRunId: native.pid + ':' + native.started, hostStartedAt, profile }
      } catch { return { available: false, reason: 'host-verification-unavailable' } }
    },
    async listThreads({ cursor = null, limit = 50, archived = false } = {}) {
      if (cursor !== null && (typeof cursor !== 'string' || cursor.length > 4096) || !Number.isSafeInteger(limit) || limit < 1 || limit > 50 || typeof archived !== 'boolean') throw new Error('invalid directory budget')
      return rpc.request('thread/list', { cursor, limit, archived, useStateDbOnly: true, sortKey: 'created_at', sortDirection: 'desc', sourceKinds: ['cli', 'vscode', 'exec', 'appServer', 'subAgent', 'subAgentReview', 'subAgentCompact', 'subAgentThreadSpawn', 'subAgentOther', 'unknown'] })
    },
    async readThread(id) {
      const result = await rpc.request('thread/read', { threadId: id, includeTurns: false })
      const thread = result?.thread
      if (!thread || thread.id !== id || thread.parentThreadId !== null || typeof thread.source !== 'string' || thread.originator !== 'Codex Desktop') return { thread }
      const turns = await rpc.request('thread/turns/list', { threadId: id, limit: 1, sortDirection: 'desc', itemsView: 'notLoaded' })
      return { thread, anchorTurnId: turns?.data?.[0]?.id ?? null }
    },
    async readEnablement(cwd) {
      const result = await rpc.request('config/read', { cwd, includeLayers: true })
      const setting = config => config?.plugins?.[pluginId]?.enabled === true ? 'enabled' : config?.plugins?.[pluginId]?.enabled === false ? 'disabled' : 'unknown'
      const layers = result.layers
      if (!Array.isArray(layers)) return { user: 'unknown', effective: 'unknown' }
      const users = layers.filter(layer => (layer.name?.type ?? layer.source?.type) === 'user')
      const user = users.length === 1 && !users[0].disabledReason ? setting(users[0].config) : 'unknown'
      const explicitDisabled = layers.some(layer => !layer.disabledReason && setting(layer.config) === 'disabled')
      return { user, effective: explicitDisabled ? 'disabled' : setting(result.config) }
    },
    diagnostics: () => rpc.diagnostics(),
    async close() { closed = true; await rpc.close(); profileCache = null },
  }
}
