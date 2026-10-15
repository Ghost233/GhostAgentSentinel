import { isAbsolute, resolve, sep } from 'node:path'

const LIMIT = 256, PAGE_SIZE = 50, PAGES_PER_REFRESH = 4, TTL = 86400000
const counters = () => ({ main: 0, fork: 0, child: 0, remote: 0, cloud: 0, unknown: 0 })

export function classifyThread(thread, sessionsRoot) {
  if (!thread || typeof thread.id !== 'string' || !thread.id || thread.id.length > 128) return 'unknown'
  if (thread.parentThreadId || typeof thread.source === 'object' && thread.source?.subAgent) return 'child'
  const locations = [thread.cwd, thread.path, ...(Array.isArray(thread.environments) ? thread.environments.flatMap(item => [item?.environmentId, item?.cwd]) : [])].filter(value => typeof value === 'string')
  if (locations.some(value => /^(?:cloud|https?):\/\//.test(value))) return 'cloud'
  if (locations.some(value => /^(?:ssh|remote):\/\//.test(value))) return 'remote'
  if (thread.parentThreadId !== null || !['cli', 'vscode', 'appServer'].includes(thread.source) || thread.originator !== 'Codex Desktop' || typeof thread.cwd !== 'string' || !isAbsolute(thread.cwd) || typeof thread.path !== 'string' || !isAbsolute(thread.path)) return 'unknown'
  if (!resolve(thread.path).startsWith(resolve(sessionsRoot) + sep) || !thread.path.endsWith('.jsonl')) return 'unknown'
  // Environment IDs are opaque in the locked native schema. A selected
  // environment without independently proved locality is not local evidence.
  if (thread.environments != null && (!Array.isArray(thread.environments) || thread.environments.some(item => item?.environmentId !== 'local'))) return 'unknown'
  if (thread.forkedFromId != null && (typeof thread.forkedFromId !== 'string' || !thread.forkedFromId)) return 'unknown'
  return thread.forkedFromId ? 'fork' : 'main'
}

export function createDiscovery({ native, sessionsRoot, now = Date.now }) {
  const catalog = new Map()
  let cursor = null, archived = false, closed = false, pending = null, directoryComplete = false, updatedAt = null, state = 'pending', gaps = [], pagesRead = 0
  const coverage = () => {
    const counts = counters()
    for (const item of catalog.values()) counts[item.kind]++
    return { state, complete: false, directoryComplete, updatedAt, counts, gaps: [...new Set([...gaps, 'all-local-execution-coverage-unverified', ...(counts.fork ? ['fork-coverage-pending'] : []), ...(counts.unknown ? ['unclassified-directory-entries'] : [])])] }
  }
  async function refresh() {
    if (closed) return coverage()
    if (pending) return pending
    pending = (async () => {
      gaps = []; directoryComplete = false
      if (typeof native.listThreads !== 'function') { state = 'unsupported'; gaps = ['thread-directory-unavailable']; return coverage() }
      const at = now()
      for (const [id, item] of catalog) if (item.seenAt <= at - TTL) catalog.delete(id)
      // Revisit the head on every refresh, while carrying the older-page cursor.
      const headOnly = cursor !== null || archived
      let next = headOnly ? null : cursor, scope = headOnly ? false : archived, pages = 0
      const seenCursors = new Set()
      try {
        while (pages < PAGES_PER_REFRESH && !closed) {
          const response = await native.listThreads({ cursor: next, limit: PAGE_SIZE, archived: scope })
          pages++; pagesRead++
          if (closed) break
          if (!response || !Array.isArray(response.data) || response.data.length > PAGE_SIZE || response.nextCursor != null && (typeof response.nextCursor !== 'string' || !response.nextCursor || response.nextCursor.length > 4096)) throw new Error('invalid directory page')
          for (const thread of response.data) {
            if (typeof thread?.id !== 'string' || !thread.id || thread.id.length > 128) { gaps.push('directory-identity-invalid'); continue }
            const kind = classifyThread(thread, sessionsRoot)
            if (!catalog.has(thread.id) && catalog.size >= LIMIT) { gaps.push('directory-capacity-reached'); continue }
            catalog.set(thread.id, { id: thread.id, kind, seenAt: at })
          }
          if (closed) break
          if (headOnly && pages === 1) { next = cursor; scope = archived; continue }
          next = response.nextCursor ?? null
          if (!next && !scope) { cursor = null; archived = true; scope = true; continue }
          if (!next) { cursor = null; archived = false; directoryComplete = true; break }
          const key = String(scope) + ':' + next
          if (seenCursors.has(key)) { gaps.push('directory-cursor-repeated'); cursor = null; archived = false; break }
          seenCursors.add(key); cursor = next; archived = scope
        }
        state = 'ready'; updatedAt = at
        if (cursor || archived) gaps.push('directory-page-budget')
      } catch { state = 'unavailable'; gaps.push('directory-page-failed'); cursor = null; archived = false }
      return coverage()
    })().finally(() => { pending = null })
    return pending
  }
  return {
    refresh, snapshot: coverage,
    candidates: () => [...catalog.values()].filter(item => item.kind === 'main').map(item => item.id),
    has: id => catalog.has(id),
    kind: id => catalog.get(id)?.kind ?? null,
    diagnostics: () => ({ directoryEntries: catalog.size, directoryPending: pending ? 1 : 0, directoryPages: pagesRead, directoryCursor: cursor || archived ? 1 : 0 }),
    async close() { closed = true; await pending; catalog.clear(); cursor = null; archived = false; pagesRead = 0; gaps = []; state = 'closed' },
  }
}
