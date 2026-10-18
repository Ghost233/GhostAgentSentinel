import { isAbsolute } from 'node:path'
import { pathToFileURL } from 'node:url'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { setTimeout as delay } from 'node:timers/promises'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js'
import { resolvePluginOwner } from './runtime.mjs'

async function startOwner(configPath) {
  const child = spawn(process.execPath, [new URL('./plugin-main.mjs', import.meta.url).pathname, '--config', configPath], { detached: true, stdio: 'ignore' })
  await once(child, 'spawn'); child.unref()
}

// A per-chat protocol connection only; collection belongs to plugin-main.
export async function startPluginBridge({ configPath, input = process.stdin, output = process.stdout, startOwner: launch = startOwner } = {}) {
  const deadline = Date.now() + 5000
  let owner, launched = false, unavailable
  while (!owner) {
    // Do not start another fixed 750 ms health probe beyond the startup budget.
    if (Date.now() + 750 > deadline) throw unavailable ?? new Error('plugin observation did not start')
    try { owner = await resolvePluginOwner({ configPath }) }
    catch (error) {
      if (error.code !== 'plugin_owner_health_unavailable') throw error
      unavailable = error
    }
    if (Date.now() > deadline) throw unavailable ?? new Error('plugin observation did not start')
    if (owner) break
    if (!unavailable && !launched) { await launch(configPath); launched = true }
    await delay(25)
  }
  const stdio = new StdioServerTransport(input, output, { maxBufferSize: 1024 * 1024 })
  const requests = new Map(), clientIds = new Set()
  let http, opening, protocolVersion, sequence = 0
  let stopped = false, closing, finished, timer, pending = 0, replies = 0
  const closed = new Promise(resolve => { finished = resolve })
  const close = (reason = 'connection-closed') => {
    if (closing) return closing
    stopped = true; clearTimeout(timer)
    input.off('end', eof); input.off('close', eof); output.off('error', failed)
    requests.clear(); clientIds.clear()
    closing = Promise.resolve().then(() => Promise.allSettled([stdio.close(), http?.close(), opening?.then(current => current.close())])).then(() => { finished({ reason }); return { reason } })
    return closing
  }
  const eof = () => { void close('stdio-eof') }
  const failed = () => { void close('transport-unavailable') }
  async function qualified() {
    try {
      const current = await resolvePluginOwner({ configPath })
      if (current?.runId === owner.runId && current.pid === owner.pid) return true
      void close('owner-ended')
    } catch (error) { if (error.code !== 'plugin_owner_health_unavailable') void close('owner-unconfirmed') }
    return false
  }
  async function settle(entry, message) {
    if (stopped || requests.get(entry.wireId) !== entry) return
    requests.delete(entry.wireId); clientIds.delete(entry.clientId)
    try { await stdio.send({ ...message, id: entry.clientId }) }
    catch { failed() }
  }
  function unconfirmed(entry) {
    return entry ? settle(entry, { jsonrpc: '2.0', error: { code: -32603, message: 'Observation temporarily unconfirmed; request outcome may be unknown' } }) : Promise.resolve()
  }
  function endpoint() {
    if (http) return Promise.resolve(http)
    return opening ??= (async () => {
      const current = new StreamableHTTPClientTransport(new URL(owner.url + '/mcp'), {
        requestInit: { headers: { authorization: 'Bearer ' + owner.token } },
        reconnectionOptions: { maxRetries: 0 },
      })
      if (protocolVersion) current.setProtocolVersion(protocolVersion)
      // send() rejects its own request; optional SSE errors have no request ID.
      current.onerror = () => {}
      current.onclose = () => {
        if (stopped || http !== current) return
        http = null
        void (async () => {
          await qualified()
          for (const entry of requests.values()) if (entry.transport === current) await unconfirmed(entry)
        })()
      }
      current.onmessage = message => {
        if (stopped || http !== current) return
        const response = message.id !== undefined && typeof message.method !== 'string'
        const entry = response ? requests.get(message.id) : null
        if (response && (!entry || entry.transport !== current)) return
        if (++replies > 32) { void close('connection-over-limit'); return }
        void (async () => {
          if (!await qualified()) return unconfirmed(entry)
          if (stopped || http !== current) return
          if (message.result?.protocolVersion) { protocolVersion = message.result.protocolVersion; current.setProtocolVersion(protocolVersion) }
          if (entry) await settle(entry, message)
          else await stdio.send(message)
        })().catch(failed).finally(() => { replies-- })
      }
      await current.start()
      if (stopped) { await current.close(); throw new Error('connection closed') }
      http = current
      return current
    })().finally(() => { opening = null })
  }
  async function watch() {
    await qualified()
    if (!stopped) timer = setTimeout(watch, 1000)
  }
  stdio.onmessage = message => {
    if (stopped) return
    if (++pending > 32) { void close('connection-over-limit'); return }
    let entry
    if (message.id !== undefined && typeof message.method === 'string') {
      if (requests.size >= 32 || clientIds.has(message.id) || sequence >= Number.MAX_SAFE_INTEGER) { void close('connection-over-limit'); return }
      // An internal ID prevents a late response from satisfying a reused client ID.
      entry = { clientId: message.id, wireId: ++sequence, transport: null }
      requests.set(entry.wireId, entry); clientIds.add(entry.clientId)
    }
    void (async () => {
      if (!await qualified()) return unconfirmed(entry)
      if (stopped) return
      const current = await endpoint()
      if (stopped) return
      if (entry) entry.transport = current
      let outgoing = entry ? { ...message, id: entry.wireId } : message
      if (message.method === 'notifications/cancelled') {
        const cancelled = [...requests.values()].find(value => value.clientId === message.params?.requestId)
        if (!cancelled || cancelled.transport !== current) return
        outgoing = { ...message, params: { ...message.params, requestId: cancelled.wireId } }
      }
      await current.send(outgoing)
    })().catch(async () => { await qualified(); await unconfirmed(entry) }).finally(() => { pending-- })
  }
  stdio.onerror = failed; stdio.onclose = eof
  input.once('end', eof); input.once('close', eof); output.once('error', failed)
  try { await endpoint(); await stdio.start(); timer = setTimeout(watch, 1000) }
  catch (error) { await close('connection-start-failed'); throw error }
  return { runId: owner.runId, pid: owner.pid, closed, close }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  let bridge, stopping = false
  const stop = () => { stopping = true; void bridge?.close('process-stopped') }
  process.once('SIGTERM', stop); process.once('SIGINT', stop)
  try {
    const args = process.argv.slice(2)
    if (args.length !== 2 || args[0] !== '--config' || !isAbsolute(args[1])) throw new Error('invalid plugin bridge arguments')
    bridge = await startPluginBridge({ configPath: args[1] })
    if (stopping) await bridge.close('process-stopped')
    await bridge.closed
  } catch { process.stderr.write('{"status":"failed","code":"plugin_bridge_failed"}\n'); process.exitCode = 1 }
  finally { process.removeListener('SIGTERM', stop); process.removeListener('SIGINT', stop) }
}
