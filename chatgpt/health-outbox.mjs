import { DatabaseSync } from 'node:sqlite'
import { mkdir, chmod } from 'node:fs/promises'
import { chmodSync } from 'node:fs'
import { join } from 'node:path'
import { failureErrorInfo } from '../src/core/failure-receipts.mjs'

const DAY = 86_400_000, LIMIT = 256
const identifier = value => typeof value === 'string' && value.length > 0 && value.length <= 2000

// Only safe collection settlement metadata crosses a disconnected core boundary.
export async function openHealthOutbox({ dataDir, now = Date.now }) {
  await mkdir(dataDir, { recursive: true, mode: 0o700 })
  await chmod(dataDir, 0o700)
  const path = join(dataDir, 'health-outbox.sqlite'), db = new DatabaseSync(path)
  db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA secure_delete=ON; CREATE TABLE IF NOT EXISTS outcomes (sequence INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL, body TEXT NOT NULL); CREATE TABLE IF NOT EXISTS counters (key TEXT PRIMARY KEY, value INTEGER NOT NULL)')
  await chmod(path, 0o600)
  for (const suffix of ['-wal', '-shm']) try { chmodSync(path + suffix, 0o600) } catch (error) { if (error.code !== 'ENOENT') { db.close(); throw error } }
  let closed = false, flushing = null, closing = null
  const rows = () => db.prepare('SELECT sequence,scope,body FROM outcomes ORDER BY sequence').all()
  const erase = sequence => db.prepare('DELETE FROM outcomes WHERE sequence=?').run(sequence)
  function prune() {
    if (closed) return
    const old = rows().filter(row => JSON.parse(row.body).settledAt <= now() - DAY)
    for (const row of old) erase(row.sequence)
    if (old.length) db.prepare("INSERT INTO counters(key,value) VALUES('expired',?) ON CONFLICT(key) DO UPDATE SET value=value+excluded.value").run(old.length)
  }
  function push(value) {
    if (closed) throw new Error('health outbox closed')
    const channel = value.channel ?? 'default'
    if (!/^(default|directory|main:[a-f0-9]{64})$/.test(channel) || !identifier(value.sourceKey) || !identifier(value.roundId) || !['failed', 'succeeded'].includes(value.outcome) || !Number.isSafeInteger(value.settledAt) || value.settledAt < 0 || value.settledAt > now() || !(value.mainId === null && value.lifecycleId === null || identifier(value.mainId) && identifier(value.lifecycleId))) throw new Error('invalid collection settlement')
    prune()
    if (value.settledAt <= now() - DAY) return
    const body = { sourceKey: value.sourceKey, mainId: value.mainId, lifecycleId: value.lifecycleId, channel, roundId: value.roundId, outcome: value.outcome, settledAt: value.settledAt, ...(value.outcome === 'failed' ? { error: failureErrorInfo(value.error) } : {}) }
    const scope = JSON.stringify([body.sourceKey, body.mainId, body.lifecycleId, channel]), pending = rows()
    const previous = pending.findLast(row => row.scope === scope)
    if (previous) {
      const last = JSON.parse(previous.body)
      if (last.roundId === body.roundId || last.outcome === body.outcome && JSON.stringify(last.error) === JSON.stringify(body.error)) return
    }
    if (pending.length >= LIMIT) {
      db.prepare("INSERT INTO counters(key,value) VALUES('rejected',1) ON CONFLICT(key) DO UPDATE SET value=value+1").run()
      throw Object.assign(new Error('collection outcome retention budget exceeded'), { code: 'monitoring_outbox_capacity' })
    }
    db.prepare('INSERT INTO outcomes(scope,body) VALUES(?,?)').run(scope, JSON.stringify(body))
  }
  function flush(report) {
    if (closed) return Promise.resolve({ sent: 0 })
    if (flushing) return flushing
    flushing = (async () => {
      prune()
      let sent = 0
      for (const row of rows()) {
        try { await report(JSON.parse(row.body)) } catch { break }
        erase(row.sequence); sent++
      }
      return { sent }
    })().finally(() => { flushing = null })
    return flushing
  }
  return { push, flush,
    health({ sourceKey, mainId, lifecycleId }) {
      if (closed) return { state: 'unknown', failures: [] }
      prune()
      const values = rows().map(row => JSON.parse(row.body)).filter(value => value.sourceKey === sourceKey && value.mainId === mainId && value.lifecycleId === lifecycleId), channels = new Map()
      for (const value of values) channels.set(value.channel, value)
      const failures = [...channels.values()].filter(value => value.outcome === 'failed').map(value => ({ domain: 'collection', error: value.error, roundId: value.roundId, settledAt: value.settledAt }))
      return { state: failures.length ? 'failed' : values.some(value => value.outcome === 'failed') ? 'recovered' : 'unknown', failures: failures.slice(0, 6), hasMore: failures.length > 6 }
    },
    diagnostics() { prune(); return { closed, pending: closed ? 0 : rows().length, limit: LIMIT, expired: closed ? 0 : db.prepare("SELECT value FROM counters WHERE key='expired'").get()?.value ?? 0, rejected: closed ? 0 : db.prepare("SELECT value FROM counters WHERE key='rejected'").get()?.value ?? 0 } },
    close() { if (closing) return closing; closed = true; closing = (async () => { await flushing; db.close() })(); return closing },
  }
}
