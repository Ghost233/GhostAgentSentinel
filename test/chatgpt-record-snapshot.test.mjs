import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { createRecordSnapshot } from '../chatgpt/native-record-snapshot.mjs'

test('a source-fd clone is immutable, anonymous, read-only and separate from later live writes', async t => {
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-snapshot-test-')), path = join(dir, 'source')
  let source, snapshot
  t.after(async () => { await snapshot?.handle.close(); await source?.close(); await fs.rm(dir, { recursive: true, force: true }) })
  await fs.writeFile(path, 'original')
  source = await fs.open(path, 'r'); snapshot = await createRecordSnapshot(source)
  assert.equal((await snapshot.handle.stat()).nlink, 0, 'no snapshot pathname survives the handoff')
  await fs.writeFile(path, 'replaced and appended')
  assert.equal(await snapshot.handle.readFile('utf8'), 'original')
  await assert.rejects(snapshot.handle.write('do not write'), { code: 'EBADF' })
  assert.equal(await fs.readFile(path, 'utf8'), 'replaced and appended')
})

test('the clone helper refuses a non-file source', async t => {
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-snapshot-invalid-')), source = await fs.open(dir, 'r')
  t.after(async () => { await source.close(); await fs.rm(dir, { recursive: true, force: true }) })
  await assert.rejects(createRecordSnapshot(source), { code: 'record-snapshot-unavailable' })
  assert.deepEqual(await fs.readdir(dir), [])
})

test('a lost parent control pipe removes the clone without an acknowledgement', async t => {
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-snapshot-handoff-')), path = join(dir, 'source'), destination = join(dir, 'private')
  await fs.mkdir(destination, { mode: 0o700 }); await fs.writeFile(path, 'owned fixture')
  const source = await fs.open(path, 'r'), folder = await fs.open(destination, 'r')
  let child
  t.after(async () => { child?.kill(); await source.close(); await folder.close(); await fs.rm(dir, { recursive: true, force: true }) })
  child = spawn(fileURLToPath(new URL('../runtime/record-snapshot', import.meta.url)), [], { stdio: ['pipe', 'pipe', 'pipe', source.fd, folder.fd] })
  const finished = new Promise((resolve, reject) => { child.once('error', reject); child.once('close', resolve) })
  await new Promise((resolve, reject) => { child.stdout.once('data', bytes => bytes.toString() === 'ready\n' ? resolve() : reject(new Error('invalid handoff'))); child.once('error', reject) })
  assert.deepEqual(await fs.readdir(destination), ['record'])
  child.stdin.end()
  assert.equal(await finished, 0)
  assert.deepEqual(await fs.readdir(destination), [])
})
