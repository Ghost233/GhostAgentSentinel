// Exact, body-free replay fences survive source GC on disk. Only live capture
// metadata occupies the bounded resident index; uninstall removes both kinds.
const COLLECTION = 'native-capture-progress', LIMIT = 10000, DAY = 86400000
const keyOf = (registrationId, nativeRecordId) => JSON.stringify([registrationId, nativeRecordId])
const error = code => Object.assign(new Error(code), { code })
const copy = value => structuredClone(value)

export function createNativeCaptureProgress({ db, seed = [] }) {
  const records = new Map()
  let totalRecords = 0
  const storedRecord = db.prepare('SELECT value FROM records WHERE collection=? AND key=?')
  const read = key => records.get(key) ?? (() => { const row = storedRecord.get(COLLECTION, key); return row ? JSON.parse(row.value) : undefined })()
  for (const row of db.prepare('SELECT key,value FROM records WHERE collection=?').iterate(COLLECTION)) {
    const record = JSON.parse(row.value)
    if (row.key !== record.key || record.key !== keyOf(record.registrationId, record.nativeRecordId) || typeof record.registrationId !== 'string' || !/^[a-f0-9]{64}$/.test(record.nativeRecordId ?? '') || (record.expired === true ? record.firstCapturedAt !== undefined || record.locator !== undefined : !Number.isSafeInteger(record.firstCapturedAt) || !record.locator)) throw error('native_capture_state_invalid')
    totalRecords++
    if (!record.expired) records.set(record.key, record)
  }
  const liveSize = changes => records.size + [...changes].reduce((count, [key, value]) => count + Number(!value.expired) - Number(records.has(key)), 0)
  const write = db.prepare('INSERT INTO records(collection,key,value) VALUES(?,?,?) ON CONFLICT(collection,key) DO UPDATE SET value=excluded.value')
  let nextExpiry = Infinity
  function expiry() { nextExpiry = Math.min(Infinity, ...[...records.values()].filter(record => !record.expired).map(record => record.firstCapturedAt + DAY)) }
  function commit(changes, initialize = false, staged = false) {
    if (!changes.size && !initialize) return () => {}
    if (liveSize(changes) > LIMIT) throw error('native_capture_capacity')
    const inserted = [...changes.keys()].filter(key => !read(key)).length
    db.exec('SAVEPOINT native_capture_progress')
    try {
      for (const [key, value] of changes) write.run(COLLECTION, key, JSON.stringify(value))
      if (initialize) write.run('settings', COLLECTION, '{"version":1}')
      db.exec('RELEASE native_capture_progress')
    } catch (failure) { db.exec('ROLLBACK TO native_capture_progress'); db.exec('RELEASE native_capture_progress'); throw failure }
    const apply = () => { for (const [key, value] of changes) { if (value.expired) records.delete(key); else records.set(key, copy(value)) }; totalRecords += inserted; expiry() }
    if (!staged) apply()
    return apply
  }
  if (records.size > LIMIT) throw error('native_capture_capacity')
  // Existing source ledgers contain only previously accepted capture metadata.
  // Upgrade never silently forgets those identities when their source expires.
  const initialized = db.prepare('SELECT value FROM records WHERE collection=? AND key=?').get('settings', COLLECTION)
  if (!initialized) {
    const initial = new Map()
    for (const row of seed) {
      const key = keyOf(row.registrationId, row.nativeRecordId), prior = initial.get(key) ?? read(key)
      if (prior?.expired || prior && prior.firstCapturedAt <= row.firstCapturedAt) continue
      initial.set(key, { key, ...copy(row) })
      if (liveSize(initial) > LIMIT) throw error('native_capture_capacity')
    }
    commit(initial, true)
  }
  expiry()
  function prepareClearRegistration(registrationId) {
    const count = db.prepare("SELECT count(*) AS n FROM records WHERE collection=? AND json_extract(value,'$.registrationId')=?").get(COLLECTION, registrationId).n
    return { removed: { nativeCaptureProgress: count }, stage() {
      db.prepare("DELETE FROM records WHERE collection=? AND json_extract(value,'$.registrationId')=?").run(COLLECTION, registrationId)
      return () => { for (const [key, record] of records) if (record.registrationId === registrationId) records.delete(key); totalRecords -= count; expiry() }
    } }
  }

  return {
    prepareClearRegistration,
    budget() { return { historyDeferred: LIMIT - records.size < 16 } },
    stageRetiredCopies(replacements) {
      const changes = new Map()
      for (const [key, locator] of replacements) {
        // Ingest has staged its progress SQL but has not published its memory.
        const row = storedRecord.get(COLLECTION, key), record = row && JSON.parse(row.value)
        if (!record || record.expired) continue
        changes.set(key, locator ? { ...record, locator: copy(locator) } : { key, registrationId: record.registrationId, nativeRecordId: record.nativeRecordId, expired: true })
      }
      // The caller owns the ingestion/upgrade savepoint. Admission has
      // already bounded its staged SQL; publishing follows that admission.
      for (const [key, value] of changes) write.run(COLLECTION, key, JSON.stringify(value))
      return () => { for (const [key, value] of changes) { if (value.expired) records.delete(key); else records.set(key, copy(value)) }; if (changes.size) expiry() }
    },
    project(registrationId, incoming, current, lookup, active) {
      const changes = new Map(), earlier = new Map(), output = [], expired = []; let existingProgress = false, resident = records.size
      const activeGenerations = new Set(active ? incoming.filter(event => event.taskId === active.taskId && event.taskLifecycleId === active.lifecycleId && !event.data?.history && !event.data?.inherited).map(event => event.data?.nativeGenerationKey) : [])
      for (const event of incoming) {
        const nativeRecordId = event.data?.nativeRecordId
        if (!nativeRecordId || event.capturedAt > current) { output.push(event); continue }
        const key = keyOf(registrationId, nativeRecordId), first = changes.get(key) ?? read(key)
        existingProgress ||= Boolean(first)
        if (first?.expired || first && first.firstCapturedAt <= current - DAY) { expired.push(event); continue }
        if (first) {
          const body = lookup(first.locator)
          // Same-batch duplicates have no persisted row yet. Their first body
          // remains in this one bounded incoming batch until validation ends.
          const original = body?.data?.nativeRecordId === nativeRecordId ? body : output.find(value => value.data?.nativeRecordId === nativeRecordId)
          if (!original) throw error('native_capture_body_unavailable')
          if (original.text !== event.text || Boolean(original.data?.truncated) !== Boolean(event.data?.truncated) || Boolean(original.data?.redacted) !== Boolean(event.data?.redacted)) throw error('hierarchy_context_invalid')
          const capturedAt = Math.min(first.firstCapturedAt, event.capturedAt)
          if (capturedAt < first.firstCapturedAt) earlier.set(nativeRecordId, capturedAt)
          changes.set(key, { ...first, firstCapturedAt: capturedAt })
          output.push({ ...event, capturedAt, evidence: original.evidence ?? event.evidence })
        } else {
          if (event.capturedAt <= current - DAY) { changes.set(key, { key, registrationId, nativeRecordId, expired: true }); expired.push(event); continue }
          // The preceding turn's closing record can share the same atomic
          // cursor advance as the active turn's first records.
          const closesPredecessor = event.kind === 'status' && ['task_complete', 'turn_aborted'].includes(event.data.nativeType)
          const priority = active && event.taskId === active.taskId && activeGenerations.has(event.data.nativeGenerationKey) && !event.data.history && !event.data.inherited && (event.taskLifecycleId === active.lifecycleId || closesPredecessor)
          if (resident >= LIMIT && !priority) throw error('native_capture_capacity')
          resident++
          changes.set(key, { key, registrationId, nativeRecordId, firstCapturedAt: event.capturedAt, locator: null })
          output.push(event)
        }
      }
      const projected = output.map(event => {
        const first = changes.get(keyOf(registrationId, event.data?.nativeRecordId))
        return first && !first.expired && event.capturedAt > first.firstCapturedAt ? { ...event, capturedAt: first.firstCapturedAt } : event
      })
      return { events: projected, expired, earlier, hasProgress: existingProgress || changes.size > 0, stage(accepted, keepsLiveContext) {
        const actual = new Map([...changes].filter(([, record]) => record.expired))
        for (const value of accepted) {
          const key = keyOf(registrationId, value.event.data?.nativeRecordId), proposed = changes.get(key)
          if (!proposed || value.event.capturedAt <= current - DAY) continue
          const known = read(key)
          if (known?.expired) continue
          actual.set(key, { ...proposed, firstCapturedAt: Math.min(proposed.firstCapturedAt, value.event.capturedAt, known?.firstCapturedAt ?? Infinity), locator: known?.locator ?? proposed.locator ?? copy(value.locator) })
        }
        const evicted = [], excess = liveSize(actual) - LIMIT
        if (excess > 0) {
          const candidates = [...records.values()].filter(record => !actual.has(record.key))
            .map(record => ({ record, liveContext: keepsLiveContext(lookup(record.locator)) }))
            .sort((a, b) => Number(a.liveContext) - Number(b.liveContext) || a.record.firstCapturedAt - b.record.firstCapturedAt || a.record.key.localeCompare(b.record.key))
          for (const { record: { key, registrationId, nativeRecordId } } of candidates.slice(0, excess)) {
            actual.set(key, { key, registrationId, nativeRecordId, expired: true })
            evicted.push(key)
          }
        }
        return { apply: commit(actual, false, true), evicted }
      } }
    },
    prune(current) {
      if (current < nextExpiry) return 0
      const expired = new Map()
      for (const [key, record] of records) if (!record.expired && record.firstCapturedAt <= current - DAY) expired.set(key, { key, registrationId: record.registrationId, nativeRecordId: record.nativeRecordId, expired: true })
      commit(expired)
      return expired.size
    },
    diagnostics(lookup) {
      const values = [...records.values()]
      return { records: totalRecords, limit: LIMIT, residentRecords: records.size, expired: totalRecords - records.size, locators: values.filter(record => record.locator).length, bodyGaps: values.filter(record => lookup(record.locator)?.data?.nativeRecordId !== record.nativeRecordId).length, expiredLocators: 0, expiredTimestamps: 0 }
    },
    clearRegistration(registrationId) {
      const plan = prepareClearRegistration(registrationId)
      let apply
      db.exec('SAVEPOINT native_capture_progress_clear')
      try { apply = plan.stage(); db.exec('RELEASE native_capture_progress_clear') }
      catch (failure) { db.exec('ROLLBACK TO native_capture_progress_clear'); db.exec('RELEASE native_capture_progress_clear'); throw failure }
      apply()
    },
  }
}
