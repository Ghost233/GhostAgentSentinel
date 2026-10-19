import { lstat, open, readdir, realpath } from 'node:fs/promises'
import { constants } from 'node:fs'
import { isAbsolute, join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { createHash } from 'node:crypto'

const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value)
const identity = info => JSON.stringify([info.dev, info.ino, info.mode, info.uid, info.nlink, info.size, info.mtimeMs, info.ctimeMs])
const fail = () => { throw new Error('GitHub payload integrity could not be confirmed') }

// Kept in the installed plugin, outside the writable extracted runtime. No
// cached JavaScript is imported until every declared file has been checked.
export async function verifyGitHubPayload({ root, manifestSHA256, nodeSHA256 }) {
  if (!isAbsolute(root ?? '') || !digest(manifestSHA256) || !digest(nodeSHA256)) fail()
  if (!(await lstat(root)).isDirectory() || (await lstat(root)).isSymbolicLink()) fail()
  root = await realpath(root)
  const buffer = Buffer.alloc(256 * 1024)
  async function file(path, expected, keep = false) {
    const before = await lstat(path)
    if (!before.isFile() || before.isSymbolicLink() || before.uid !== process.getuid() || before.nlink !== 1 || (before.mode & 0o7777) !== expected.mode || before.size !== expected.size) fail()
    const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW), hash = createHash('sha256'), chunks = []
    try {
      if (identity(await handle.stat()) !== identity(before)) fail()
      let position = 0
      while (true) {
        const { bytesRead } = await handle.read(buffer, 0, buffer.length, position)
        if (!bytesRead) break
        position += bytesRead
        if (position > expected.size) fail()
        const chunk = buffer.subarray(0, bytesRead); hash.update(chunk)
        if (keep) chunks.push(Buffer.from(chunk))
      }
      if (position !== expected.size || hash.digest('hex') !== expected.sha256 || identity(await handle.stat()) !== identity(before) || identity(await lstat(path)) !== identity(before)) fail()
      return keep ? Buffer.concat(chunks) : null
    } finally { await handle.close() }
  }
  const manifestPath = join(root, 'plugin-package.json'), manifestInfo = await lstat(manifestPath)
  if (manifestInfo.size < 1 || manifestInfo.size > 8 * 1024 * 1024) fail()
  const manifest = JSON.parse(await file(manifestPath, { size: manifestInfo.size, mode: 0o600, sha256: manifestSHA256 }, true))
  const entries = Object.entries(manifest.files ?? {})
  if (manifest.format !== 1 || manifest.kind !== 'ghost-sentinel-plugin-payload' || !entries.length || entries.length > 20000 || manifest.node?.sha256 !== nodeSHA256 || manifest.files['runtime/node']?.sha256 !== nodeSHA256) fail()
  let bytes = 0
  for (const [path, value] of entries) {
    if (!path || path.length > 1024 || /[\x00-\x1f\x7f\\]/.test(path) || path.startsWith('/') || path.split('/').some(part => !part || part === '.' || part === '..') || path === 'plugin-package.json' || !digest(value?.sha256) || !Number.isSafeInteger(value.size) || value.size < 0 || ![0o600, 0o700].includes(value.mode)) fail()
    if ((bytes += value.size) > 256 * 1024 * 1024) fail()
  }
  async function tree(directory, prefix = '', state = { count: 0, records: {}, files: [] }) {
    const before = await lstat(directory)
    if (!before.isDirectory() || before.isSymbolicLink() || before.uid !== process.getuid() || (before.mode & 0o7777) !== 0o700) fail()
    state.records[prefix] = identity(before)
    for (const name of (await readdir(directory)).sort()) {
      if (++state.count > 40000) fail()
      const path = join(directory, name), key = prefix ? prefix + '/' + name : name, info = await lstat(path)
      if (info.isSymbolicLink()) fail()
      if (info.isDirectory()) await tree(path, key, state)
      else if (info.isFile()) { state.records[key] = identity(info); state.files.push(key) }
      else fail()
    }
    if (identity(await lstat(directory)) !== identity(before)) fail()
    return state
  }
  const before = await tree(root), expected = [...entries.map(([path]) => path), 'plugin-package.json'].sort()
  if (JSON.stringify(before.files.sort()) !== JSON.stringify(expected)) fail()
  for (const [path, value] of entries) await file(join(root, path), value)
  const after = await tree(root)
  if (JSON.stringify(before.records) !== JSON.stringify(after.records)) fail()
  return { status: 'verified', version: manifest.version, manifestSHA256, launched: false }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const [root, manifestSHA256, nodeSHA256, channel] = process.argv.slice(2)
    if (![5, 6].includes(process.argv.length) || channel !== undefined && !['stable', 'test'].includes(channel)) fail()
    const result = await verifyGitHubPayload({ root, manifestSHA256, nodeSHA256 })
    if (channel) process.stdout.write(JSON.stringify({ ...result, channel }) + '\n')
  } catch { process.stderr.write('{"status":"failed","code":"github_payload_integrity_failed"}\n'); process.exitCode = 1 }
}
