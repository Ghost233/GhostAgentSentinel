import { createServer } from 'node:http'
import { timingSafeEqual } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js'
import { registerAppResource, registerAppTool, RESOURCE_MIME_TYPE } from '@modelcontextprotocol/ext-apps/server'
import { z } from 'zod'

export async function startPanelServer({ read, control, modelSettings, requestNotifications, notificationRecords, runtimeStatus, displayName = 'Sentinel', token, port = 0 }) {
  if (typeof token !== 'string' || !/^[a-f0-9]{64}$/.test(token)) throw new Error('installation authentication is required')
  if (!['Sentinel', 'GhostSentinel', 'GhostSentinel test'].includes(displayName)) throw new Error('invalid panel display name')
  const title = displayName === 'GhostSentinel test' ? 'GhostSentinel 会话检查 test' : displayName + ' 会话检查'
  const html = (await readFile(new URL('./panel.html', import.meta.url), 'utf8')).replace('<title>Sentinel 会话检查</title>', '<title>' + title + '</title>').replace('<h1>Sentinel · 当前会话</h1>', '<h1>' + displayName + ' · 当前会话</h1>'), sockets = new Set(), cleanups = new Set()
  const settingsTitle = displayName === 'GhostSentinel test' ? 'GhostSentinel 安装设置 test' : displayName + ' 安装设置'
  const settingsHtml = modelSettings ? (await readFile(new URL('./settings.html', import.meta.url), 'utf8')).replace('<title>Sentinel 安装设置</title>', '<title>' + settingsTitle + '</title>').replace('<h1>Sentinel · 安装设置</h1>', '<h1>' + settingsTitle + '</h1>') : null
  let closed = false, activeRequests = 0, transports = 0
  const server = createServer(async (req, res) => {
    const host = '127.0.0.1:' + server.address().port
    const credential = req.headers.authorization ?? ''
    const expected = Buffer.from('Bearer ' + token), credentialBytes = Buffer.from(credential)
    if (req.headers.host !== host || credentialBytes.length !== expected.length || !timingSafeEqual(credentialBytes, expected)) return res.writeHead(401, { 'content-type': 'application/json' }).end('{"error":"Unauthorized"}')
    if (req.headers.origin && !['https://chatgpt.com', 'https://chat.openai.com'].includes(req.headers.origin)) return res.writeHead(403, { 'content-type': 'application/json' }).end('{"error":"Origin rejected"}')
    if (req.method === 'GET' && req.url === '/health' && typeof runtimeStatus === 'function') {
      try {
        const { installationId, runId, pid, version, ownership, releaseId } = await runtimeStatus()
        if ([installationId, runId, version].some(value => typeof value !== 'string' || !value || value.length > 256) || !Number.isSafeInteger(pid) || pid < 1) throw new Error('invalid helper identity')
        if (ownership !== undefined && ownership !== 'plugin') throw new Error('invalid observation ownership')
        if (releaseId !== undefined && (typeof releaseId !== 'string' || !/^[a-f0-9]{64}$/.test(releaseId))) throw new Error('invalid release identity')
        return res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ protocol: 'ghost-sentinel-chatgpt-helper/1', installationId, runId, pid, version, ...(ownership ? { ownership } : {}), ...(releaseId ? { releaseId } : {}) }))
      } catch { return res.writeHead(503, { 'content-type': 'application/json' }).end('{"error":"Helper status unavailable"}') }
    }
    if (req.url !== '/mcp') return res.writeHead(404, { 'content-type': 'application/json' }).end('{"error":"Not found"}')
    if (closed) return res.writeHead(503).end()
    activeRequests++
    const mcp = new McpServer({ name: 'ghost-sentinel-chatgpt', version: '0.1.0-dev.1' })
    const uri = 'ui://ghost-sentinel/current-chat.html'
    registerAppResource(mcp, 'sentinel-current-chat', uri, {}, async () => ({ contents: [{ uri, mimeType: RESOURCE_MIME_TYPE, text: html }] }))
    registerAppTool(mcp, 'sentinel_current_chat', {
      title, description: '只读查看当前本机主会话及24小时内已监控执行的检查结果。',
      inputSchema: { executionKey: z.string().min(1).max(256).optional(), historyCursor: z.string().min(1).max(256).optional(), expectedThreadId: z.string().min(1).max(128).optional() },
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false, idempotentHint: true },
      _meta: { ui: { resourceUri: uri, visibility: ['app'] }, 'openai/ui': { entrypoints: [{ type: 'thread' }] } },
    }, async (selection, extra) => {
      const view = await read(extra._meta, selection)
      return { content: [{ type: 'text', text: view.summary }], structuredContent: view }
    })
    if (modelSettings) {
      const settingsUri = 'ui://ghost-sentinel/settings.html'
      registerAppResource(mcp, 'sentinel-settings', settingsUri, {}, async () => ({ contents: [{ uri: settingsUri, mimeType: RESOURCE_MIME_TYPE, text: settingsHtml }] }))
      const model = z.object({ baseUrl: z.string().min(1).max(2000), model: z.string().min(1).max(2000) }).strict()
      registerAppTool(mcp, 'sentinel_model_settings', {
        title: settingsTitle, description: '打开独立安装设置，读取本安装的总结模型和 JEV 配置，不依赖当前聊天采集。', inputSchema: {},
        annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false, idempotentHint: true }, _meta: { ui: { resourceUri: settingsUri, visibility: ['app'] }, 'openai/ui': { entrypoints: [{ type: 'global' }] } },
      }, async () => ({ content: [{ type: 'text', text: '本安装模型设置' }], structuredContent: await modelSettings.read() }))
      registerAppTool(mcp, 'sentinel_save_model_settings', {
        title: '保存本安装模型设置', description: '按读取的配置版本保存，仅影响本安装的分析。',
        inputSchema: { revision: z.string().min(1).max(2000), config: z.object({ llm: model.optional(), jev: model.optional() }).strict() },
        annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false, idempotentHint: false }, _meta: { ui: { visibility: ['app'] } },
      }, async change => ({ content: [{ type: 'text', text: '模型设置已保存' }], structuredContent: await modelSettings.save(change) }))
      if (typeof modelSettings.directory === 'function') registerAppTool(mcp, 'sentinel_model_directory', {
        title: '读取本机模型列表', description: '仅检查服务在线及模型名称，不调用生成或 JEV 推理。', inputSchema: { baseUrl: z.string().min(1).max(2000) },
        annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false, idempotentHint: true }, _meta: { ui: { visibility: ['app'] } },
      }, async (selection, extra) => ({ content: [{ type: 'text', text: '模型目录与在线状态' }], structuredContent: await modelSettings.directory(selection, { signal: extra.signal }) }))
    }
    if (typeof control === 'function') registerAppTool(mcp, 'sentinel_set_chat_analysis', {
      title: '设置本聊天分析', description: '仅暂停或恢复当前聊天的监控分析，继续只读采集。',
      inputSchema: { paused: z.boolean(), expectedThreadId: z.string().min(1).max(128), revision: z.string().min(1).max(2000) },
      annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false, idempotentHint: true },
      _meta: { ui: { visibility: ['app'] } },
    }, async (change, extra) => {
      const view = await control(extra._meta, change)
      return { content: [{ type: 'text', text: view.summary }], structuredContent: view }
    })
    if (typeof requestNotifications === 'function') registerAppTool(mcp, 'sentinel_request_notification_permission', {
      title: '允许桌面通知', description: '在当前聊天身份核实后请求 GhostSentinel 自己的系统通知许可。',
      inputSchema: { expectedThreadId: z.string().min(1).max(128) },
      annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false, idempotentHint: true }, _meta: { ui: { visibility: ['app'] } },
    }, async (change, extra) => {
      const view = await requestNotifications(extra._meta, change)
      return { content: [{ type: 'text', text: view.summary }], structuredContent: view }
    })
    if (typeof notificationRecords === 'function') registerAppTool(mcp, 'sentinel_notification_records', {
      title: '本聊天通知记录', description: '只读核对当前聊天及所选执行的系统通知事实。',
      inputSchema: { expectedThreadId: z.string().min(1).max(128), executionKey: z.string().min(1).max(256).optional(), beforeSequence: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER).optional() },
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false, idempotentHint: true }, _meta: { ui: { visibility: ['app'] } },
    }, async (selection, extra) => {
      const result = await notificationRecords(extra._meta, selection)
      return { content: [{ type: 'text', text: '本聊天通知记录' }], structuredContent: result }
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
