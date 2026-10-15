import test, { before, after } from 'node:test'
import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { mkdtemp, mkdir, readFile, writeFile, cp, rm, realpath, access } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { createHash } from 'node:crypto'
import { setTimeout as delay } from 'node:timers/promises'

const execute = promisify(execFile)
const repo = fileURLToPath(new URL('../', import.meta.url))
const checker = fileURLToPath(new URL('../scripts/check-dsh-package.mjs', import.meta.url))
const manifest = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'))
const evidenceDirectory = process.env.SENTINEL_PACKED_TEST_EVIDENCE_DIR
let temporary, receiver, installed, tarball, pack, packagedFiles, runtimeProof, cleanupRecoveryUsed = false, sequence = 0
const commands = []
async function run(file, args, cwd = repo) {
  const env = { ...process.env }
  delete env.NODE_TEST_CONTEXT
  try {
    const result = await execute(file, args, { cwd, env, timeout: 30000, maxBuffer: 1024 * 1024 })
    commands.push({ file, args, cwd, exitCode: 0, stdout: result.stdout, stderr: result.stderr })
    return result
  } catch (error) {
    commands.push({ file, args, cwd, exitCode: error.code, signal: error.signal, stdout: error.stdout, stderr: error.stderr })
    throw error
  }
}
before(async () => {
  temporary = await realpath(await mkdtemp(join(tmpdir(), 'sentinel-packed-package-')))
  const destination = join(temporary, 'pack'), cache = join(temporary, 'npm-cache')
  receiver = join(temporary, 'receiver')
  await mkdir(destination); await mkdir(receiver)
  const flags = ['--ignore-scripts', '--offline', '--no-audit', '--no-fund', '--cache', cache]
  const packed = await run('npm', ['pack', ...flags, '--pack-destination', destination, '--json'])
  ;[pack] = JSON.parse(packed.stdout)
  tarball = join(destination, pack.filename)
  await writeFile(join(receiver, 'package.json'), JSON.stringify({ name: 'sentinel-owned-package-receiver', private: true, type: 'module' }))
  await run('npm', ['install', tarball, ...flags, '--package-lock=false'], receiver)
  installed = await realpath(join(receiver, 'node_modules', manifest.name))
  assert.ok(installed.startsWith(temporary + '/'), 'the installed package must be a physical copy outside the checkout')
  assert.ok(!installed.startsWith(repo))
  packagedFiles = Object.fromEntries(await Promise.all(pack.files.map(async ({ path }) => [path, createHash('sha256').update(await readFile(join(installed, path))).digest('hex')])))
})
after(async () => {
  const failures = []
  let coreReleased = true
  try {
    const directory = join(receiver, 'core-data')
    let runtime
    try { runtime = JSON.parse(await readFile(join(directory, 'runtime.json'), 'utf8')) } catch (error) { if (error.code !== 'ENOENT') throw error }
    if (runtime) {
      const alive = () => { try { process.kill(runtime.pid, 0); return true } catch (error) { if (error.code === 'ESRCH') return false; throw error } }
      if (alive()) {
        assert.equal(runtime.protocol, 1); assert.match(runtime.url, /^http:\/\/127\.0\.0\.1:\d+$/)
        assert.ok(Number.isSafeInteger(runtime.pid) && runtime.pid > 0 && runtime.pid !== process.pid)
        const response = await fetch(`${runtime.url}/v1/health`, { headers: { authorization: `Bearer ${runtime.token}` }, redirect: 'error', signal: AbortSignal.timeout(1000) })
        assert.equal(response.status, 200)
        const health = await response.json()
        assert.equal(health.protocol, 1); assert.equal(health.instanceId, runtime.instanceId)
        cleanupRecoveryUsed = true
        process.kill(runtime.pid, 'SIGTERM')
        const deadline = Date.now() + 5000
        while (alive()) { assert.ok(Date.now() < deadline, 'authenticated owned core failure cleanup deadline'); await delay(50) }
      }
    }
  } catch (error) { coreReleased = false; failures.push(error) }
  try {
    if (evidenceDirectory && temporary) {
      await mkdir(evidenceDirectory, { recursive: true })
      const tarballBytes = tarball ? await readFile(tarball) : null
      if (tarballBytes) await writeFile(join(evidenceDirectory, 'delivered-package.tgz'), tarballBytes)
      await writeFile(join(evidenceDirectory, 'pack-and-commands.json'), JSON.stringify({ checkedAt: new Date().toISOString(), node: process.version, manifest, package: pack,
        tarballSha256: tarballBytes ? createHash('sha256').update(tarballBytes).digest('hex') : null,
        temporary, receiver, installed, commands, packagedFiles, runtimeProof, coreReleased, cleanupRecoveryUsed }, null, 2) + '\n')
    }
  } catch (error) { failures.push(error) }
  try {
    if (temporary && coreReleased) { await rm(temporary, { recursive: true, force: true }); await assert.rejects(access(temporary), { code: 'ENOENT' }) }
  } catch (error) { failures.push(error) }
  if (cleanupRecoveryUsed) failures.push(new Error('forced owned core cleanup cannot count as normal package lifecycle acceptance'))
  if (failures.length) throw new AggregateError(failures, 'package validation teardown failed; original test errors remain separate')
})

test('an installed bundle with the wrong native plugin namespace cannot pass composition', async () => {
  const damaged = await damagedCopy()
  await writeFile(join(damaged, 'dsh/cordis.patch.yml'), '- insert:\n    - id: wrong-native-plugin\n      name: wrong-native-plugin\n      config: {}\n')
  await assert.rejects(run(process.execPath, [checker, damaged]), error => error.code === 1 && /wrong-native-plugin/.test(error.stderr))
})

test('an installed client missing its loader registration ID cannot pass the package check', async () => {
  const damaged = await damagedCopy()
  const path = join(damaged, manifest.exports['./client'])
  const original = await readFile(path, 'utf8')
  const missingId = original.replace(/\s+id: 'ghost-agent-sentinel',/, '')
  assert.notEqual(missingId, original, 'the regression must actually remove the registration ID')
  await writeFile(path, missingId)
  await assert.rejects(run(process.execPath, [checker, damaged]), error => error.code === 1 && /endsWith/.test(error.stderr))
})

test('a packed module resolved outside the checkout connects its native adapter and naturally releases its core', async () => {
  await run(process.execPath, [checker, installed])
  const bin = await realpath(join(receiver, 'node_modules/.bin/ghost-agent-sentinel'))
  assert.equal(bin, join(installed, 'src/daemon.mjs'))
  await cp(new URL('./packed-package-runtime.mjs', import.meta.url), join(receiver, 'runtime.mjs'))
  const loaderURL = new URL('./dsh-native-runtime.mjs', import.meta.url).href
  const result = await run(process.execPath, [join(receiver, 'runtime.mjs'), loaderURL, installed, join(receiver, 'core-data')], receiver)
  runtimeProof = JSON.parse(result.stdout.split('\n').find(line => line.startsWith('PACKED_RUNTIME_RESULT:')).slice('PACKED_RUNTIME_RESULT:'.length))
  assert.equal(runtimeProof.passed, true)
  assert.equal(runtimeProof.before.sourceStatus, 'connected')
  assert.equal(runtimeProof.before.tasks, 0)
  assert.equal(runtimeProof.after.corePidAlive, false)
  assert.equal(runtimeProof.after.runtimeRemoved, true)
  assert.equal(runtimeProof.after.coreLockRemoved, true)
  assert.equal(runtimeProof.after.startupLockRemoved, true)
  assert.deepEqual(runtimeProof.cleanupFailures, [])
  assert.equal(runtimeProof.failure, null)
  assert.equal(cleanupRecoveryUsed, false)
  assert.ok(!result.stdout.includes('"token"'), 'private connection credentials must not enter package receipts')
})
async function damagedCopy() {
  const directory = join(temporary, `damaged-${++sequence}`)
  await cp(installed, directory, { recursive: true })
  return directory
}

test('a delivered package missing a required runtime module cannot pass the package check', async () => {
  const damaged = await damagedCopy()
  await rm(join(damaged, 'dsh/remote.cjs'))
  await assert.rejects(run(process.execPath, [checker, damaged]), error => error.code === 1 && /remote\.cjs/.test(error.stderr))
})
