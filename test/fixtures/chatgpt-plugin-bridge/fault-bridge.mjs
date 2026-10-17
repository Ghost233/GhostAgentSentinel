import { readFile, appendFile } from 'node:fs/promises'
import { setTimeout as delay } from 'node:timers/promises'
import { startPluginBridge } from '../../../chatgpt/plugin-bridge.mjs'
import { StreamableHTTPClientTransport } from '../../../chatgpt/node_modules/@modelcontextprotocol/sdk/dist/esm/client/streamableHttp.js'

const [configPath, faultPath, auditPath] = process.argv.slice(2), originalFetch = globalThis.fetch
let armed, activeTransport
const audit = value => appendFile(auditPath, JSON.stringify(value) + '\n')
const send = StreamableHTTPClientTransport.prototype.send
StreamableHTTPClientTransport.prototype.send = function (message, options) { activeTransport = this; return send.call(this, message, options) }
globalThis.fetch = async (url, options) => {
  const fault = JSON.parse(await readFile(faultPath, 'utf8'))
  if (String(url).endsWith('/health') && (armed || fault.kind === 'persistent-health-network')) {
    const kind = armed ?? fault.kind; armed = null
    await audit({ event: 'health-fault', kind })
    if (kind === 'health-network' || kind === 'persistent-health-network') throw new TypeError('Owned secret-network-error')
    const response = await originalFetch(url, options)
    if (kind === 'health-timeout') await delay(900, undefined, { signal: options.signal })
    if (/^health-(401|403|404|503)$/.test(kind)) return new Response('{}', { status: Number(kind.slice(7)) })
    if (kind === 'health-identity') return new Response(JSON.stringify({ ...(await response.json()), runId: '00000000-0000-0000-0000-000000000000' }), { status: 200 })
    return response
  }
  const message = options?.method === 'POST' && options.body ? JSON.parse(options.body) : null
  if (message) await audit({ event: 'mcp-request', method: message.method ?? 'response', id: message.id, requestId: message.params?.requestId, protocolVersion: new Headers(options.headers).get('mcp-protocol-version') })
  if (message?.method === 'notifications/initialized' && fault.kind === 'notification-network') {
    await audit({ event: 'notification-fault' }); throw new TypeError('Owned secret-notification-error')
  }
  const response = await originalFetch(url, options)
  if (message?.method === 'tools/list' && fault.kind === 'server-request') {
    const value = await response.json()
    return new Response(JSON.stringify([value, { jsonrpc: '2.0', id: 'owned-server-request', method: fault.method ?? 'ping' }]), { status: 200, headers: { 'content-type': 'application/json' } })
  }
  if (message?.method === 'tools/call' && message.params.name === 'sentinel_set_chat_analysis') {
    const value = await response.clone().json()
    await audit({ event: 'control-committed', paused: value.result?.structuredContent?.monitoring?.paused ?? null })
    if (fault.kind?.startsWith('health-')) armed = fault.kind
    if (fault.kind === 'http-503') return new Response('{"error":"Owned secret-http-error"}', { status: 503 })
    if (fault.kind === 'http-close') {
      await response.body?.cancel(); await activeTransport.close()
      await delay(200)
      await audit({ event: 'late-old-transport-response' })
      return new Response(JSON.stringify(value), { status: 200, headers: { 'content-type': 'application/json' } })
    }
  }
  return response
}
const bridge = await startPluginBridge({ configPath, startOwner: async () => { throw new Error('Owned observer must already exist') } })
await audit({ event: 'bridge-ready' })
await bridge.closed
