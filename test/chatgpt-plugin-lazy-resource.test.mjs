import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { BoundedReader, supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { resourceMaterial } from './fixtures/chatgpt-plugin-lazy/resource-material.mjs'

test('the real bridge resource fixture carries a verified producer and its synthetic current request reaches the material reader', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-lazy-resource-material-')), path = join(dir, 'main.jsonl'), at = Date.now()
  const reader = new BoundedReader({ path, allowedRoot: dir, threadId: 'resource-main', parentId: null, sourceKind: 'vscode', originator: 'Codex Desktop', cwd: dir, hostRunId: 'resource-host', hostStartedAt: at - 10000, profile: supportedProfile, anchorTurnId: 'resource-turn', nativeOriginVersion: 2 }, null, { mode: 'material' })
  t.after(async () => { await reader.close(); await rm(dir, { recursive: true, force: true }) })
  await writeFile(path, resourceMaterial({ at, cwd: dir, producerVersion: supportedProfile.cliVersion }))
  const result = await reader.pump({ anchorTurnId: 'resource-turn' })
  assert.equal(result.gap, null, 'the benchmark must exercise material admission, rather than silently benchmarking a rejected fixture')
  assert.equal(result.coverage, 'caught-up')
  assert.equal(result.effectivePhase, 'started')
  assert.deepEqual(result.materials.filter(value => value.kind === 'input').map(value => value.text), ['Read this synthetic resource fixture only'])
})
