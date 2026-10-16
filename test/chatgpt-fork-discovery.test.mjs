import test from 'node:test'
import assert from 'node:assert/strict'
import { createDiscovery } from '../chatgpt/discovery.mjs'

test('directory forks are independent collection candidates while child and opaque environment entries stay excluded', async () => {
  const base = { parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', cwd: '/same-install/project', path: '/same-install/sessions/rollout-main.jsonl', forkedFromId: null }
  const entries = [{ ...base, id: 'main' }, { ...base, id: 'fork', forkedFromId: 'main' }, { ...base, id: 'child', parentThreadId: 'main' }, { ...base, id: 'unknown', environments: [{ environmentId: 'opaque' }] }]
  const discovery = createDiscovery({ sessionsRoot: '/same-install/sessions', native: { async listThreads({ archived }) { return { data: archived ? [] : entries, nextCursor: null } } } })
  try { await discovery.refresh(); assert.deepEqual(discovery.candidates(), ['main', 'fork']); assert.deepEqual(discovery.snapshot().counts, { main: 1, fork: 1, child: 1, remote: 0, cloud: 0, unknown: 1 }) }
  finally { await discovery.close() }
  assert.equal(discovery.diagnostics().directoryEntries, 0)
})
