import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { BoundedReader, supportedProfile } from '../chatgpt/bounded-reader.mjs'

test('missing source records expose only safe errno while unproved fork ancestry remains a qualification gap', async t => {
  const root = await mkdtemp(join(tmpdir(), 'sentinel-fork-io-'))
  const source = { path: join(root, 'missing-source.jsonl'), allowedRoot: root, threadId: 'main', parentId: null, originator: 'Codex Desktop', sourceKind: 'vscode', hostRunId: 'host-run', hostStartedAt: 1000, profile: supportedProfile, anchorTurnId: 'turn', forkedFromId: null }
  const missing = new BoundedReader(source), unprovedFork = new BoundedReader({ ...source, forkedFromId: 'original' })
  t.after(async () => { await missing.close(); await unprovedFork.close(); await rm(root, { recursive: true, force: true }) })
  const failed = await missing.pump()
  assert.equal(failed.gap, 'record-source-unavailable')
  assert.deepEqual(failed.error, { code: 'ENOENT' })
  assert.equal(JSON.stringify(failed.error).includes(root), false)
  assert.equal(Object.hasOwn(failed.error, 'message'), false)
  assert.equal(Object.hasOwn(failed.error, 'stack'), false)
  const ancestry = await unprovedFork.pump()
  assert.equal(ancestry.gap, 'fork-provenance-unverified')
  assert.equal(ancestry.readBytes, 0)
  assert.equal(ancestry.error, undefined)
})
