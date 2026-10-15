import { createServer } from 'node:http'
import { timingSafeEqual } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js'
import { registerAppResource, registerAppTool, RESOURCE_MIME_TYPE } from '@modelcontextprotocol/ext-apps/server'

export async function startPanelServer({ read, token, port = 0 }) {
  if (typeof token !== 'string' || !/^[a-f0-9]{64}$/.test(token)) throw new Error('installation authentication is required')
  const html = await readFile(new URL('./panel.html', import.meta.url), 'utf8'), sockets = new Set(), cleanups = new Set()
  let closed = false, activeRequests = 0, transports = 0
  const server = createServer(async (req, res) => {
    const host = '127.0.0.1:' + server.address().port
    const credential = req.headers.authorization ?? ''
    const expected = Buffer.from('Bearer ' + token), credentialBytes = Buffer.from(credential)
    if (req.headers.host !== host || credentialBytes.length !== expected.length || !timingSafeEqual(credentialBytes, expected)) return res.writeHead(401, { 'content-type': 'application/json' }).end('{"error":"Unauthorized"}')
    if (req.headers.origin && !['https://chatgpt.com', 'https://chat.openai.com'].includes(req.headers.origin)) return res.writeHead(403, { 'content-type': 'application/json' }).end('{"error":"Origin rejected"}')
    if (req.url !== '/mcp') return res.writeHead(404, { 'content-type': 'application/json' }).end('{"error":"Not found"}')
    if (closed) return res.writeHead(503).end()
    activeRequests++
    const mcp = new McpServer({ name: 'ghost-sentinel-chatgpt', version: '0.1.0-dev.0' })
    const uri = 'ui://ghost-sentinel/current-chat.html'
    registerAppResource(mcp, 'sentinel-current-chat', uri, {}, async () => ({ contents: [{ uri, mimeType: RESOURCE_MIME_TYPE, text: html }] }))
    registerAppTool(mcp, 'sentinel_current_chat', {
      title: 'Sentinel 会话检查', description: '只读查看当前本机主会话的观察、检查结果及证据缺口。', inputSchema: {},
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false, idempotentHint: true },
      _meta: { ui: { resourceUri: uri, visibility: ['app'] }, 'openai/ui': { entrypoints: [{ type: 'thread' }] } },
    }, async (_args, extra) => {
      const view = await read(extra._meta)
      return { content: [{ type: 'text', text: view.summary }], structuredContent: view }
    })
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true })
    transports++
    let done = false
    const cleanup = async () => { if (done) return; done = true; cleanups.delete(cleanup); try { await transport.close(); await mcp.close() } finally { activeRequests--; transports-- } }
    cleanups.add(cleanup); res.once('close', () => { void cleanup() })
    try { await mcp.connect(transport); await transport.handleRequest(req, res) }
    catch { if (!res.headersSent) res.writeHead(500, { 'content-type': 'application/json' }).end('{"error":"Panel request failed"}'); else res.end() }
    finally { if (res.writableEnded) await cleanup() }
  })
  server.requestTimeout = 10000
  server.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)) })
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, '127.0.0.1', resolve) })
  return { url: 'http://127.0.0.1:' + server.address().port, diagnostics: () => ({ activeRequests, transports, sockets: sockets.size }), async close() { if (closed) return; closed = true; for (const socket of sockets) socket.destroy(); await Promise.allSettled([...cleanups].map(close => close())); await new Promise(resolve => server.close(resolve)) } }
}
