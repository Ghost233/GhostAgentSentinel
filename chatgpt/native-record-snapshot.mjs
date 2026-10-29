import fs from 'node:fs/promises'
import { constants } from 'node:fs'
import { spawn, execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { join, dirname } from 'node:path'
import { tmpdir } from 'node:os'
import { fileURLToPath } from 'node:url'
import { createHash } from 'node:crypto'

const executable = fileURLToPath(new URL('../runtime/record-snapshot', import.meta.url))
const fail = () => Object.assign(new Error('record snapshot unavailable'), { code: 'record-snapshot-unavailable' })

export async function buildRecordSnapshot(source) {
  const directory = await fs.mkdtemp(join(tmpdir(), 'sentinel-snapshot-build-'))
  try {
    await fs.writeFile(join(directory, 'snapshot.c'), source, { mode: 0o600 })
    await promisify(execFile)('/usr/bin/clang', ['-O2', '-mmacosx-version-min=13.0', '-Wall', '-Wextra', '-Werror', join(directory, 'snapshot.c'), '-o', join(directory, 'snapshot')], { timeout: 30000, maxBuffer: 65536 })
    return await fs.readFile(join(directory, 'snapshot'))
  } finally { await fs.rm(directory, { recursive: true, force: true }) }
}

export async function createRecordSnapshot(source) {
  const directory = await fs.mkdtemp(join(tmpdir(), 'sentinel-record-snapshot-'))
  let folder, child, handle, timer, finished
  try {
    await fs.chmod(directory, 0o700)
    folder = await fs.open(directory, constants.O_RDONLY | constants.O_NOFOLLOW)
    child = spawn(executable, [], { stdio: ['pipe', 'pipe', 'ignore', source.fd, folder.fd] })
    finished = new Promise((resolve, reject) => { child.once('error', reject); child.once('close', code => code === 0 ? resolve() : reject(fail())) })
    finished.catch(() => {})
    timer = setTimeout(() => child.kill('SIGKILL'), 15000)
    const ready = new Promise((resolve, reject) => {
      let bytes = ''
      child.stdout.on('data', value => { bytes += value; if (bytes === 'ready\n') resolve(); else if (bytes.length >= 6) reject(fail()) })
      child.once('error', reject); child.once('close', () => reject(fail()))
    })
    await ready
    handle = await fs.open(join(directory, 'record'), constants.O_RDONLY | constants.O_NOFOLLOW)
    child.stdin.on('error', () => {})
    child.stdin.end('1')
    await finished
    const stat = await handle.stat()
    return { handle, stat }
  } catch (error) { await handle?.close(); throw Object.assign(fail(), { cause: error }) }
  finally {
    clearTimeout(timer)
    child?.stdin.destroy()
    if (finished) await finished.catch(() => {})
    await folder?.close()
    await fs.rm(directory, { recursive: true, force: true })
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url) && process.argv[2] === 'build') {
  const source = await fs.readFile(new URL('./native-record-snapshot.c', import.meta.url)), bytes = await buildRecordSnapshot(source)
  const hash = value => createHash('sha256').update(value).digest('hex')
  await fs.mkdir(dirname(executable), { recursive: true, mode: 0o700 })
  await fs.writeFile(executable, bytes, { mode: 0o700 })
  await fs.writeFile(executable + '.json', JSON.stringify({ format: 1, sourceSHA256: hash(source), builderSHA256: hash(await fs.readFile(fileURLToPath(import.meta.url))), binarySHA256: hash(bytes) }), { mode: 0o600 })
}
