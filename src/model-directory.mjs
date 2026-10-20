import { inferenceEndpoint } from './analysis/http.mjs'

const LIMIT = 262_144
export async function readModelDirectory(baseUrl, { signal } = {}) {
  const url = inferenceEndpoint({ baseUrl }, '/v1/models')
  const deadline = AbortSignal.timeout(5000)
  const result = { online: false, inferenceVerified: false, models: [], checkedAt: Date.now() }
  try {
    const response = await fetch(url, { method: 'GET', redirect: 'error', signal: signal ? AbortSignal.any([signal, deadline]) : deadline })
    result.online = true
    if (!response.ok) { await response.body?.cancel(); return { ...result, state: 'unconfirmed', error: { code: 'model_directory_http', status: response.status } } }
    const chunks = []
    let bytes = 0
    for await (const chunk of response.body) {
      bytes += chunk.length
      if (bytes > LIMIT) return { ...result, state: 'unconfirmed', error: { code: 'model_directory_limit' } }
      chunks.push(chunk)
    }
    let value
    try { value = JSON.parse(Buffer.concat(chunks).toString('utf8')) } catch { return { ...result, state: 'unconfirmed', error: { code: 'model_directory_invalid' } } }
    if (!Array.isArray(value?.data)) return { ...result, state: 'unconfirmed', error: { code: 'model_directory_invalid' } }
    const models = new Map()
    for (const item of value.data.slice(0, 256)) {
      if (typeof item?.id !== 'string' || !item.id.trim() || item.id.length > 2000 || /[\x00-\x1f\x7f]/.test(item.id)) continue
      const role = item.type === 'embedding' || item.source === 'embedding' ? 'embedding'
        : ['native', 'council'].includes(item.source) || item.type === 'decision' ? 'jev'
          : item.type === 'llm' || item.owned_by === 'ghostmodeldeck' && item.source === undefined ? 'summary' : 'unknown'
      models.set(item.id, { id: item.id, role: models.has(item.id) && models.get(item.id).role !== role ? 'unknown' : role })
    }
    return { ...result, state: 'online', models: [...models.values()], complete: value.data.length <= 256 }
  } catch {
    signal?.throwIfAborted()
    return { ...result, state: result.online ? 'unconfirmed' : 'offline', error: { code: deadline.aborted ? 'model_directory_timeout' : 'model_directory_unavailable' } }
  }
}
