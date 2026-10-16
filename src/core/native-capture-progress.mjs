// Body-free progress survives normal source GC. Capacity is finite and identities
// are never evicted to make room; only registration uninstall may remove them.
const COLLECTION = 'native-capture-progress', LIMIT = 10000, DAY = 86400000
const keyOf = (registrationId, nativeRecordId) => JSON.stringify([registrationId, nativeRecordId])
const error = code => Object.assign(new Error(code), { code })
const copy = value => structuredClone(value)

export function createNativeCaptureProgress({ db, seed = [] }) {
  const records = new Map(db.prepare('SELECT value FROM records WHERE collection=?').all(COLLECTION).map(row => { const value = JSON.parse(row.value); return [value.key, value] }))
  const write = db.prepare('INSERT INTO records(collection,key,value) VALUES(?,?,?) ON CONFLICT(collection,key) DO UPDATE SET value=excluded.value')
  const remove = db.prepare('DELETE FROM records WHERE collection=? AND key=?')
  let nextExpiry = Infinity
  function expiry() { nextExpiry = Math.min(Infinity, ...[...records.values()].filter(record => !record.expired).map(record => record.firstCapturedAt + DAY)) }
  function commit(changes, initialize = false, staged = false) {
    if (!changes.size && !initialize) return () => {}
    if (records.size + [...changes.keys()].filter(key => !records.has(key)).length > LIMIT) throw error('native_capture_capacity')
    db.exec('SAVEPOINT native_capture_progress')
    try {
      for (const [key, value] of changes) write.run(COLLECTION, key, JSON.stringify(value))
      if (initialize) write.run('settings', COLLECTION, '{"version":1}')
      db.exec('RELEASE native_capture_progress')
    } catch (failure) { db.exec('ROLLBACK TO native_capture_progress'); db.exec('RELEASE native_capture_progress'); throw failure }
    const apply = () => { for (const [key, value] of changes) records.set(key, copy(value)); expiry() }
    if (!staged) apply()
    return apply
  }
  for (const record of records.values()) if (record.key !== keyOf(record.registrationId, record.nativeRecordId) || typeof record.registrationId !== 'string' || !/^[a-f0-9]{64}$/.test(record.nativeRecordId ?? '') || (record.expired === true ? record.firstCapturedAt !== undefined || record.locator !== undefined : !Number.isSafeInteger(record.firstCapturedAt) || !record.locator)) throw error('native_capture_state_invalid')
  if (records.size > LIMIT) throw error('native_capture_capacity')
  // Existing source ledgers contain only previously accepted capture metadata.
  // Upgrade never silently forgets those identities when their source expires.
  const initialized = db.prepare('SELECT value FROM records WHERE collection=? AND key=?').get('settings', COLLECTION)
  if (!initialized) {
    const initial = new Map()
    for (const row of seed) {
      const key = keyOf(row.registrationId, row.nativeRecordId), prior = initial.get(key) ?? records.get(key)
      if (prior?.expired || prior && prior.firstCapturedAt <= row.firstCapturedAt) continue
      initial.set(key, { key, ...copy(row) })
      if (records.size + [...initial.keys()].filter(key => !records.has(key)).length > LIMIT) throw error('native_capture_capacity')
    }
    commit(initial, true)
  }
  expiry()

  return {
    project(registrationId, incoming, current, lookup) {
      const changes = new Map(), earlier = new Map(), output = []; let existingProgress = false
      for (const event of incoming) {
        const nativeRecordId = event.data?.nativeRecordId
        if (!nativeRecordId || event.capturedAt > current) { output.push(event); continue }
        const key = keyOf(registrationId, nativeRecordId), first = changes.get(key) ?? records.get(key)
        existingProgress ||= Boolean(first)
        if (first?.expired || first && first.firstCapturedAt <= current - DAY) continue
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
          if (records.size + [...changes.keys()].filter(value => !records.has(value)).length >= LIMIT) throw error('native_capture_capacity')
          if (event.capturedAt <= current - DAY) { changes.set(key, { key, registrationId, nativeRecordId, expired: true }); continue }
          changes.set(key, { key, registrationId, nativeRecordId, firstCapturedAt: event.capturedAt, locator: null })
          output.push(event)
        }
      }
      const projected = output.map(event => {
        const first = changes.get(keyOf(registrationId, event.data?.nativeRecordId))
        return first && !first.expired && event.capturedAt > first.firstCapturedAt ? { ...event, capturedAt: first.firstCapturedAt } : event
      })
      return { events: projected, earlier, hasProgress: existingProgress || changes.size > 0, stage(accepted) {
        const actual = new Map([...changes].filter(([, record]) => record.expired))
        for (const value of accepted) {
          const key = keyOf(registrationId, value.event.data?.nativeRecordId), proposed = changes.get(key)
          if (!proposed || value.event.capturedAt <= current - DAY) continue
          const known = records.get(key)
          if (known?.expired) continue
          actual.set(key, { ...proposed, firstCapturedAt: Math.min(proposed.firstCapturedAt, value.event.capturedAt, known?.firstCapturedAt ?? Infinity), locator: known?.locator ?? proposed.locator ?? copy(value.locator) })
        }
        return commit(actual, false, true)
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
      const values = [...records.values()], expired = values.filter(record => record.expired)
      return { records: records.size, limit: LIMIT, expired: expired.length, locators: values.filter(record => record.locator).length, bodyGaps: values.filter(record => !record.expired && lookup(record.locator)?.data?.nativeRecordId !== record.nativeRecordId).length, expiredLocators: expired.filter(record => record.locator !== undefined).length, expiredTimestamps: expired.filter(record => record.firstCapturedAt !== undefined).length }
    },
    clearRegistration(registrationId) {
      const selected = [...records].filter(([, record]) => record.registrationId === registrationId)
      db.exec('SAVEPOINT native_capture_progress_clear')
      try { for (const [key] of selected) remove.run(COLLECTION, key); db.exec('RELEASE native_capture_progress_clear') }
      catch (failure) { db.exec('ROLLBACK TO native_capture_progress_clear'); db.exec('RELEASE native_capture_progress_clear'); throw failure }
      for (const [key] of selected) records.delete(key)
      expiry()
    },
  }
}
