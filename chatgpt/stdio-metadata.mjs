import { spawn as spawnChild } from 'node:child_process'
import { isAbsolute } from 'node:path'
import { NATIVE_EXECUTABLE } from './native-io.mjs'

const methods = new Set(['thread/read', 'thread/turns/list', 'thread/list', 'config/read', 'plugin/list', 'plugin/read'])
const MAX_BYTES = 1024 * 1024, MAX_PENDING = 32
const messages = { deadline: 'metadata request timed out', 'metadata-rpc-error': 'metadata request failed', 'metadata-unavailable': 'metadata connection unavailable', 'metadata-send-failed': 'metadata send failed', 'metadata-invalid-response': 'metadata response invalid', cancelled: 'metadata client closed', 'metadata-cleanup-failed': 'metadata process cleanup failed' }
const failure = code => Object.assign(new Error(messages[code] ?? 'metadata operation failed'), { code, stage: 'metadata' })

// One owned foreground metadata process, started only by an allowed request.
export function createStdioMetadataClient({ executable = NATIVE_EXECUTABLE, args = ['app-server'], spawn = spawnChild } = {}) {
  if (typeof executable !== 'string' || !isAbsolute(executable) || executable.includes('\0') || !Array.isArray(args) || args.length > 16 || args.some(value => typeof value !== 'string' || value.includes('\0')) || Buffer.byteLength(args.join(' ')) > 16384 || typeof spawn !== 'function') throw new Error('metadata executable or arguments invalid')
  args = [...args]
  let connection = null, opening = null, disconnecting = null, closing = null, closed = false, sequence = 0, requests = 0, cleanupFailure = null
  const pending = new Map()
  function failPending(state, code) {
    for (const [id, entry] of pending) if (entry.state === state) { pending.delete(id); clearTimeout(entry.timer); entry.reject(failure(code)) }
  }
  async function waitEnd(state) {
    if (state.ended) return true
    let timer
    try { return await Promise.race([state.end.then(() => true), new Promise(resolve => { timer = setTimeout(() => resolve(false), 1000) })]) }
    finally { clearTimeout(timer) }
  }
  function retire(state, code = 'cancelled') {
    if (!state) return Promise.resolve()
    if (state.stopping) return state.stopping
    if (connection === state) connection = null
    failPending(state, code)
    state.retired = true
    const stopped = (async () => {
      let forcedSignals = 0
      if (!state.ended) {
        try { state.child.stdin.end() } catch { /* Exit is still independently awaited. */ }
        if (!await waitEnd(state)) {
          forcedSignals++
          try { state.child.kill('SIGKILL') } catch { /* A failed rescue remains a cleanup failure. */ }
          await waitEnd(state)
        }
      }
      if (!state.ended || forcedSignals || Number.isSafeInteger(state.child.pid) && (state.terminal.code !== 0 || state.terminal.signal !== null)) {
        const error = failure('metadata-cleanup-failed'); error.forcedSignals = forcedSignals
        cleanupFailure ??= error
        throw error
      }
    })()
    const tracked = stopped.finally(() => { if (disconnecting === tracked) disconnecting = null })
    state.stopping = tracked; disconnecting = tracked
    tracked.catch(() => {})
    return tracked
  }
  function send(state, bytes, onError) {
    const size = Buffer.byteLength(bytes)
    if (state.retired || state.ended || state.writingBytes + size > MAX_BYTES) throw failure('metadata-send-failed')
    state.writingBytes += size
    try { state.child.stdin.write(bytes, error => { state.writingBytes -= size; if (error) onError() }) }
    catch { state.writingBytes -= size; throw failure('metadata-send-failed') }
  }
  function wire(state, method, params, timeoutMs = 5000) {
    if (closed) return Promise.reject(failure('cancelled'))
    if (connection !== state || state.retired || state.ended || pending.size >= MAX_PENDING || sequence >= Number.MAX_SAFE_INTEGER) return Promise.reject(failure('metadata-unavailable'))
    const id = ++sequence
    let bytes
    try { bytes = JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'; if (Buffer.byteLength(bytes) > MAX_BYTES) throw new Error() }
    catch { return Promise.reject(failure('metadata-send-failed')) }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { pending.delete(id); reject(failure('deadline')) }, timeoutMs)
      pending.set(id, { state, resolve, reject, timer })
      const failed = () => { const entry = pending.get(id); if (!entry || entry.state !== state) return; pending.delete(id); clearTimeout(entry.timer); entry.reject(failure('metadata-send-failed')) }
      try { send(state, bytes, failed) } catch { failed() }
    })
  }
  function receive(state, bytes) {
    if (connection !== state || state.retired) return
    state.buffer = Buffer.concat([state.buffer, bytes])
    let end
    while ((end = state.buffer.indexOf(10)) !== -1) {
      const line = state.buffer.subarray(0, end); state.buffer = state.buffer.subarray(end + 1)
      if (!line.length) continue
      let message
      try {
        if (line.length > MAX_BYTES) throw new Error()
        message = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(line))
        if (!message || typeof message !== 'object' || Array.isArray(message) || message.jsonrpc !== undefined && message.jsonrpc !== '2.0' || typeof message.method === 'string' && message.id !== undefined) throw new Error()
      } catch { void retire(state, 'metadata-invalid-response'); return }
      if (typeof message.method === 'string') continue
      const entry = pending.get(message.id)
      if (!entry || entry.state !== state) continue
      pending.delete(message.id); clearTimeout(entry.timer)
      if (message.error) entry.reject(failure('metadata-rpc-error'))
      else if (Object.hasOwn(message, 'result')) entry.resolve(message.result)
      else { entry.reject(failure('metadata-invalid-response')); void retire(state, 'metadata-invalid-response'); return }
    }
    if (state.buffer.length > MAX_BYTES) void retire(state, 'metadata-invalid-response')
  }
  async function connect() {
    if (disconnecting) await disconnecting
    if (closed) throw failure('cancelled')
    if (cleanupFailure) throw cleanupFailure
    if (connection?.initialized) return connection
    if (opening) return opening
    opening = (async () => {
      let child
      try { child = spawn(executable, args, { stdio: ['pipe', 'pipe', 'pipe'], detached: false, windowsHide: true }) }
      catch { throw failure('metadata-unavailable') }
      const state = { child, buffer: Buffer.alloc(0), writingBytes: 0, stderrBytes: 0, initialized: false, retired: false, ended: false, terminal: null, stopping: null }
      state.end = new Promise(resolve => { state.endedResolve = resolve })
      connection = state
      child.on('error', () => { void retire(state, 'metadata-unavailable') })
      child.on('close', (code, signal) => {
        state.ended = true; state.terminal = { code, signal }; state.endedResolve()
        if (connection === state) connection = null
        failPending(state, 'metadata-unavailable')
        if (Number.isSafeInteger(child.pid) && (code !== 0 || signal !== null)) cleanupFailure ??= failure('metadata-cleanup-failed')
      })
      child.stdin.on('error', () => { if (!state.retired) void retire(state, 'metadata-send-failed') })
      child.stdout.on('data', bytes => receive(state, bytes))
      child.stdout.on('error', () => { void retire(state, 'metadata-unavailable') })
      child.stdout.on('end', () => { if (!state.retired) void retire(state, 'metadata-unavailable') })
      child.stderr.on('data', bytes => { state.stderrBytes += bytes.length; if (state.stderrBytes > MAX_BYTES) void retire(state, 'metadata-invalid-response') })
      child.stderr.on('error', () => { void retire(state, 'metadata-unavailable') })
      try {
        await wire(state, 'initialize', { clientInfo: { name: 'ghost_sentinel_chatgpt', version: '0.1.0-dev.1' }, capabilities: { experimentalApi: true } }, 3000)
        if (closed || connection !== state || state.retired) throw failure(closed ? 'cancelled' : 'metadata-unavailable')
        send(state, JSON.stringify({ method: 'initialized' }) + '\n', () => { void retire(state, 'metadata-send-failed') })
        state.initialized = true
        return state
      } catch (error) { await retire(state).catch(() => {}); throw error }
    })().finally(() => { opening = null })
    return opening
  }
  async function disconnect() {
    const pendingOpening = opening
    await (connection ? retire(connection) : disconnecting)
    await pendingOpening?.catch(() => {})
    if (cleanupFailure) throw cleanupFailure
  }
  return {
    async request(method, params) {
      if (!methods.has(method)) throw new Error('non-observation method rejected')
      if (closed) throw failure('cancelled')
      if (requests >= MAX_PENDING) throw failure('metadata-unavailable')
      let copy
      try { const bytes = JSON.stringify(params ?? {}); if (Buffer.byteLength(bytes) > MAX_BYTES - 1024) throw new Error(); copy = JSON.parse(bytes) }
      catch { throw failure('metadata-send-failed') }
      requests++
      try { const state = await connect(); return await wire(state, method, copy) }
      finally { requests-- }
    },
    diagnostics: () => ({ closed, pendingRequests: pending.size, deadlineTimers: pending.size, connected: Boolean(connection?.initialized && !connection.retired && !connection.ended) }),
    disconnect,
    close() { if (closing) return closing; closed = true; closing = disconnect(); return closing },
  }
}
