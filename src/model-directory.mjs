import { inferenceEndpoint } from './analysis/http.mjs'

const LIMIT = 262_144
export async function readModelDirectory(baseUrl, { signal } = {}) {
  const url = inferenceEndpoint({ baseUrl }, '/v1/models')
  const deadline = AbortSignal.timeout(5000)
  const requestSignal = signal ? AbortSignal.any([signal, deadline]) : deadline
  const result = { online: false, inferenceVerified: false, models: [], checkedAt: Date.now() }
  let bytes = 0
  async function readJSON(response) {
    const chunks = []
    for await (const chunk of response.body) {
      bytes += chunk.length
      if (bytes > LIMIT) throw Object.assign(new Error('model directory exceeds byte budget'), { code: 'model_directory_limit' })
      chunks.push(chunk)
    }
    try { return JSON.parse(Buffer.concat(chunks).toString('utf8')) }
    catch { throw Object.assign(new Error('invalid model directory'), { code: 'model_directory_invalid' }) }
  }
  try {
    const response = await fetch(url, { method: 'GET', redirect: 'error', signal: requestSignal })
    result.online = true
    if (!response.ok) { await response.body?.cancel(); return { ...result, state: 'unconfirmed', error: { code: 'model_directory_http', status: response.status } } }
    const value = await readJSON(response)
    if (!Array.isArray(value?.data)) return { ...result, state: 'unconfirmed', error: { code: 'model_directory_invalid' } }
    const models = new Map(), gmdIds = new Set()
    function addRole(id, role) {
      const previous = models.get(id)
      const roles = new Set([...(previous?.roles ?? (previous ? [previous.role] : [])), role])
      roles.delete('unknown')
      if (roles.has('embedding')) models.set(id, { id, role: 'embedding' })
      else if (roles.has('summary') && roles.has('jev')) models.set(id, { id, role: 'multiple', roles: ['summary', 'jev'] })
      else models.set(id, { id, role: [...roles][0] ?? 'unknown' })
    }
    for (const item of value.data.slice(0, 256)) {
      if (typeof item?.id !== 'string' || !item.id.trim() || item.id.length > 2000 || /[\x00-\x1f\x7f]/.test(item.id)) continue
      const role = item.type === 'embedding' || item.source === 'embedding' ? 'embedding'
        : ['native', 'council'].includes(item.source) || item.type === 'decision' ? 'jev'
          : item.type === 'llm' || item.owned_by === 'ghostmodeldeck' && item.source === undefined ? 'summary' : 'unknown'
      addRole(item.id, role)
      if (item.type === 'llm') addRole(item.id, 'summary')
      if (item.owned_by === 'ghostmodeldeck' && role !== 'embedding') gmdIds.add(item.id)
    }
    if (gmdIds.size) {
      // GMD's source describes decision routing, not exclusive capability.
      // Its capacity directory explicitly lists loaded text/decision bindings.
      try {
        const response = await fetch(inferenceEndpoint({ baseUrl }, '/api/v1/models'), { method: 'GET', redirect: 'error', signal: requestSignal })
        if (response.ok) {
          const capacity = await readJSON(response)
          if (capacity?.source === 'ghost-model-deck' && Array.isArray(capacity.models)) for (const item of capacity.models.slice(0, 512)) {
            if (!gmdIds.has(item?.key) || item.source !== 'ghost-model-deck' || !Array.isArray(item.loaded_instances) || !item.loaded_instances.some(instance => instance?.id === item.key)) continue
            const role = item.type === 'llm' ? 'summary' : item.type === 'decision' ? 'jev' : item.type === 'embedding' ? 'embedding' : null
            if (role) addRole(item.key, role)
          }
        } else await response.body?.cancel()
      } catch { signal?.throwIfAborted() }
    }
    return { ...result, state: 'online', models: [...models.values()], complete: value.data.length <= 256 }
  } catch (error) {
    signal?.throwIfAborted()
    return { ...result, state: result.online ? 'unconfirmed' : 'offline', error: { code: ['model_directory_limit', 'model_directory_invalid'].includes(error.code) ? error.code : deadline.aborted ? 'model_directory_timeout' : 'model_directory_unavailable' } }
  }
}
