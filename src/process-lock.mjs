import { closeSync, lstatSync, openSync } from 'node:fs'
import { DatabaseSync } from 'node:sqlite'
import { setTimeout as delay } from 'node:timers/promises'

export async function withProcessLock(path, action, { deadline = Date.now() + 2500, signal } = {}) {
  signal?.throwIfAborted()
  try {
    // Do not open and close another descriptor for an existing SQLite file.
    closeSync(openSync(path, 'wx', 0o600))
  } catch (error) { if (error.code !== 'EEXIST') throw error }
  const info = lstatSync(path)
  if (!info.isFile() || info.mode & 0o077 || info.uid !== process.getuid()) throw new Error('untrusted runtime ownership lock')
  const db = new DatabaseSync(path)
  try {
    for (;;) {
      signal?.throwIfAborted()
      try { db.exec('BEGIN IMMEDIATE'); break }
      catch (error) { if (error.errcode !== 5 || Date.now() >= deadline) throw error; await delay(25, undefined, { signal }) }
    }
    // SQLite releases the reservation even when a contender dies mid-recovery.
    return await action()
  } finally { db.close() }
}
