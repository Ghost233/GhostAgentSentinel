import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, rm, writeFile, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { execFile, spawn } from 'node:child_process'
import { promisify } from 'node:util'
import { setTimeout as delay } from 'node:timers/promises'

const run = promisify(execFile)
test('the compiled production Swift notification policy and action journal enforce their public native boundaries', { skip: process.platform !== 'darwin' && 'requires the macOS Swift toolchain' }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-swift-facts-')), binary = join(dir, 'facts'), cache = join(dir, 'cache')
  t.after(() => rm(dir, { recursive: true, force: true }))
  await mkdir(cache)
  await run('/usr/bin/xcrun', ['swiftc', '-swift-version', '5', '-parse-as-library', '-module-cache-path', cache, 'chatgpt/native/NotificationFacts.swift', 'test/helpers/notification-facts.swift', '-o', binary], { timeout: 60000, maxBuffer: 1048576 })
  for (const mode of ['inventory', 'journal', 'journal-uninstall', 'journal-boundaries', 'journal-concurrency', 'journal-errors']) await t.test(mode, async () => {
    const owned = join(dir, mode); await mkdir(owned, { mode: 0o700 })
    const result = await run(binary, [mode, owned], { timeout: 5000, maxBuffer: 65536 })
    assert.match(result.stdout, new RegExp('PASS ' + mode))
  })
  await t.test('cross-process journal maintenance and append share the same owned lock', async () => {
    const owned = join(dir, 'process-lock'); await mkdir(owned, { mode: 0o700 })
    await writeFile(join(owned, 'actions.json'), JSON.stringify({ schemaVersion: 1, historyGap: true, events: [{ observedAt: 0, requestId: 'expired' }] }), { mode: 0o600 })
    const child = mode => {
      const process = spawn(binary, [mode, owned], { stdio: ['pipe', 'pipe', 'pipe'], timeout: 5000 })
      let output = ''
      const ready = new Promise((resolve, reject) => { process.stdout.on('data', part => { output += part; if (output.includes(mode === 'hold-journal-lock' ? 'LOCKED' : 'WRITING')) resolve() }); process.once('error', reject); process.once('exit', code => { if (!output.includes(mode === 'hold-journal-lock' ? 'LOCKED' : 'WRITING')) reject(new Error('owned Swift helper exited before ready: ' + code)) }) })
      const done = new Promise((resolve, reject) => { process.once('exit', (code, signal) => resolve({ code, signal })); process.once('error', reject) })
      return { process, ready, done }
    }
    const holder = child('hold-journal-lock'); let writer
    try {
      await holder.ready; writer = child('journal-writer'); await writer.ready
      assert.equal(await Promise.race([writer.done.then(() => 'finished'), delay(50, 'blocked')]), 'blocked', 'a second process must not replace facts while maintenance holds the journal lock')
    } finally { holder.process.stdin.end('x'); assert.deepEqual(await holder.done, { code: 0, signal: null }); if (writer) assert.deepEqual(await writer.done, { code: 0, signal: null }) }
    const disk = JSON.parse(await readFile(join(owned, 'actions.json'), 'utf8'))
    assert.equal(disk.events.length, 1); assert.ok(disk.events[0].requestId.endsWith(':concurrent')); assert.equal(disk.historyGap, true)
  })
})
