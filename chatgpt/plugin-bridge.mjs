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
  let owner = await resolvePluginOwner({ configPath })
  if (!owner) {
    await launch(configPath)
    const deadline = Date.now() + 5000
    while (!owner && Date.now() < deadline) { await delay(25); owner = await resolvePluginOwner({ configPath }) }
    if (!owner) throw new Error('plugin observation did not start')
  }
  const stdio = new StdioServerTransport(input, output, { maxBufferSize: 1024 * 1024 })
  const http = new StreamableHTTPClientTransport(new URL(owner.url + '/mcp'), {
    requestInit: { headers: { authorization: 'Bearer ' + owner.token } },
    reconnectionOptions: { maxRetries: 0 },
  })
  let stopped = false, closing, finished, timer, pending = 0, replies = 0
  const closed = new Promise(resolve => { finished = resolve })
  const close = (reason = 'connection-closed') => {
    if (closing) return closing
    stopped = true; clearTimeout(timer)
    input.off('end', eof); input.off('close', eof); output.off('error', failed)
    closing = Promise.resolve().then(() => Promise.allSettled([stdio.close(), http.close()])).then(() => { finished({ reason }); return { reason } })
    return closing
  }
  const eof = () => { void close('stdio-eof') }
  const failed = () => { void close('transport-unavailable') }
  async function qualified() {
    const current = await resolvePluginOwner({ configPath })
    return current?.runId === owner.runId && current.pid === owner.pid
  }
  async function watch() {
    try { if (!await qualified()) return void close('owner-ended') }
    catch { return void close('owner-unconfirmed') }
    if (!stopped) timer = setTimeout(watch, 1000)
  }
  stdio.onmessage = message => {
    if (stopped) return
    if (++pending > 32) { void close('connection-over-limit'); return }
    void (async () => {
      if (!await qualified()) return void close('owner-ended')
      if (!stopped) await http.send(message)
    })().catch(failed).finally(() => { pending-- })
  }
  http.onmessage = message => {
    if (stopped) return
    if (++replies > 32) { void close('connection-over-limit'); return }
    void (async () => {
      if (stopped) return
      if (!await qualified()) return void close('owner-ended')
      if (message.result?.protocolVersion) http.setProtocolVersion(message.result.protocolVersion)
      if (!stopped) await stdio.send(message)
    })().catch(failed).finally(() => { replies-- })
  }
  stdio.onerror = failed; stdio.onclose = eof
  http.onerror = failed; http.onclose = () => { void close('owner-connection-closed') }
  input.once('end', eof); input.once('close', eof); output.once('error', failed)
  try { await http.start(); await stdio.start(); timer = setTimeout(watch, 1000) }
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
