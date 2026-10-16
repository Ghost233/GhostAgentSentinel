import { constants } from 'node:fs'
import { lstat, open, rename, rm } from 'node:fs/promises'
import { randomUUID } from 'node:crypto'

const LIMIT = 16384
const FORK_LIMIT = 262144
const invalid = code => Object.assign(new Error(code), { code })
const identity = value => `${value.dev}:${value.ino}:${value.size}:${value.mtimeMs}`

export async function loadCursor(path, { fork = false } = {}) {
  const limit = fork === true ? FORK_LIMIT : LIMIT
  let before
  try { before = await lstat(path) } catch (error) { if (error.code === 'ENOENT') return null; throw invalid('cursor-unavailable') }
  if (!before.isFile() || before.mode & 0o077 || typeof process.getuid === 'function' && before.uid !== process.getuid()) throw invalid('cursor-file-unverified')
  if (before.size > limit) throw invalid('cursor-over-limit')
  let file
  try {
    file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW)
    const opened = await file.stat()
    if (identity(opened) !== identity(before)) throw invalid('cursor-file-changed')
    const bytes = Buffer.alloc(limit + 1), read = await file.read(bytes, 0, bytes.length, 0)
    const [after, current] = await Promise.all([file.stat(), lstat(path)])
    if (read.bytesRead > limit) throw invalid('cursor-over-limit')
    if (read.bytesRead !== opened.size || identity(after) !== identity(opened) || identity(current) !== identity(opened) || !current.isFile()) throw invalid('cursor-file-changed')
    const value = JSON.parse(bytes.subarray(0, read.bytesRead).toString('utf8'))
    if (value.format !== 2 || !value.reader || !Array.isArray(value.captures) || value.captures.length > 16 || !Number.isSafeInteger(value.captureThroughOffset) || value.captureThroughOffset < -1) throw invalid('cursor-format-invalid')
    const offsets = new Set()
    for (const capture of value.captures) {
      if (!Number.isSafeInteger(capture.offset) || capture.offset < 0 || capture.offset > value.captureThroughOffset || !Number.isSafeInteger(capture.capturedAt) || capture.capturedAt < 0 || offsets.has(capture.offset)) throw invalid('cursor-capture-invalid')
      offsets.add(capture.offset)
    }
    return value
  } catch (error) { if (error.code?.startsWith('cursor-')) throw error; throw invalid('cursor-unavailable') }
  finally { await file?.close() }
}

export async function saveCursor(path, reader, captured, captureThroughOffset, { fork = false } = {}) {
  const value = JSON.stringify({ format: 2, reader, captures: [...captured].map(([offset, capturedAt]) => ({ offset, capturedAt })), captureThroughOffset })
  if (Buffer.byteLength(value) > (fork === true ? FORK_LIMIT : LIMIT)) throw invalid('cursor-over-limit')
  const temporary = path + '.' + randomUUID() + '.tmp'
  let file
  try {
    file = await open(temporary, 'wx', 0o600)
    await file.writeFile(value)
    await file.close(); file = null
    await rename(temporary, path)
  } catch (error) {
    await file?.close()
    try { await rm(temporary, { force: true }) } catch (cleanup) { throw new AggregateError([error, cleanup], 'cursor write and cleanup failed') }
    throw error
  }
}
