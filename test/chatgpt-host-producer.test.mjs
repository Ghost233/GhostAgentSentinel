import { withModelSummary } from './helpers/model-summary.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { zstdCompressSync } from 'node:zlib'
import { createHash } from 'node:crypto'
import { BoundedReader, supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { ForkHistoryReader } from '../chatgpt/fork-history.mjs'
import { supportsHostProfile } from '../chatgpt/storage-compatibility.mjs'
import { createServer } from 'node:http'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { configureInstallationModels } from './helpers/installation-model-settings.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { decodeJevRequest } from './helpers/jev-wire.mjs'

// Fixed shapes from the bounded 0.163 main-record sample; all text is synthetic.
const currentHost = Object.freeze({ appVersion: '26.1013.31256', appBuild: '20586', cliVersion: '0.163.0-alpha.10.2' })
const owner = '00000000-0000-4000-8000-000000000163'
const at = 1_800_000_000_000
const row = (ordinal, type, payload) => JSON.stringify({ ordinal, type, payload, timestamp: new Date(at).toISOString() }) + '\n'
const header = (id, producer) => ({ id, originator: 'Codex Desktop', source: 'vscode', cli_version: producer, cwd: '/synthetic-project', history_mode: 'paginated' })
const start = turn => ({ type: 'task_started', turn_id: turn, turn_attribution: { turn_id: turn, turn_trigger: 'composer' } })
const user = (thread, turn, text) => ({ type: 'item_completed', thread_id: thread, turn_id: turn, item: { type: 'UserMessage', id: 'typed-user', content: [{ type: 'text', text }] } })

async function fixture(t, { producer = currentHost.cliVersion, profile = currentHost, records = '', compressed = false } = {}) {
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-host-producer-')), plain = join(dir, 'rollout-' + owner + '.jsonl')
  const bytes = Buffer.from(row(0, 'session_meta', header(owner, producer)) + records)
  await fs.writeFile(plain, bytes); await fs.writeFile(plain + '.zst', zstdCompressSync(bytes))
  const source = { path: plain + (compressed ? '.zst' : ''), allowedRoot: dir, threadId: owner, parentId: null, sourceKind: 'vscode', originator: 'Codex Desktop', hostRunId: 'host-current', hostStartedAt: at - 10000, profile, anchorTurnId: 'turn', nativeOriginVersion: 2 }
  const readers = []
  const open = (changes = {}, cursor = null, options = {}) => { const reader = new BoundedReader({ ...source, ...changes }, cursor, { mode: 'material', ...options }); readers.push(reader); return reader }
  t.after(async () => { for (const reader of readers) await reader.close(); await fs.rm(dir, { recursive: true, force: true }) })
  return { source, open, bytes, plain }
}

async function collect(reader, budgetBytes = 1024) {
  const materials = [], lifecycle = []; let view
  for (let pump = 0; pump < 100; pump++) {
    view = await reader.pump({ budgetBytes })
    assert.ok(view.readBytes <= budgetBytes); assert.ok((view.decodedBytes ?? 0) <= 262144)
    materials.push(...(view.materials ?? [])); lifecycle.push(...view.recentLifecycle)
    if (['caught-up', 'gap'].includes(view.coverage)) break
  }
  return { view, materials, lifecycle }
}

test('the exact candidate Host reads fixed current-producer main shapes without promoting role-only messages to user requests', async t => {
  const records = row(1, 'event_msg', start('turn'))
    + row(2, 'response_item', { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'A role mirror is not authorization' }] })
    + row(3, 'event_msg', user(owner, 'turn', 'Inspect only this synthetic task'))
    + row(4, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item: { type: 'CommandExecution', id: 'command', aggregated_output: 'Synthetic command failed', exit_code: 7 } })
    + row(5, 'event_msg', { type: 'task_complete', turn_id: 'turn' })
  const f = await fixture(t, { records }), result = await collect(f.open())
  assert.equal(result.view.gap, null)
  assert.equal(result.view.effectivePhase, 'completed')
  assert.deepEqual(result.lifecycle.map(event => event.type), ['task_started', 'task_complete'])
  assert.deepEqual(result.materials.filter(event => event.kind === 'input').map(event => [event.text, event.data.inputOrigin]), [['Inspect only this synthetic task', 'user-entry']])
  assert.equal(result.materials.find(event => event.text.startsWith('A role mirror')).kind, 'status')
  const command = result.materials.find(event => event.kind === 'tool-result')
  assert.equal(command.text, 'Synthetic command failed'); assert.equal(command.data.exitCode, 7)
  assert.equal(command.data.callId, 'native-item:command')
})

test('an admitted Host does not admit an unknown or missing storage producer', async t => {
  for (const producer of ['0.163.0-alpha.10.3', '0.162.0-alpha.3', null]) await t.test(String(producer), async t => {
    const f = await fixture(t, { producer, records: row(1, 'event_msg', start('turn')) + row(2, 'event_msg', user(owner, 'turn', 'Must remain unread')) })
    const result = await collect(f.open())
    assert.equal(result.view.gap, 'record-producer-unverified')
    assert.equal(result.view.effectivePhase, 'unknown')
    assert.deepEqual(result.materials, []); assert.deepEqual(result.lifecycle, [])
  })
})

async function forkFixture(t, producer, { childProducer = currentHost.cliVersion, compressed = false } = {}) {
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-host-fork-')), sessionsRoot = join(dir, 'sessions'), archived = join(dir, 'archived_sessions')
  await fs.mkdir(sessionsRoot); await fs.mkdir(archived)
  const parent = '00000000-0000-4000-8000-000000000162', path = join(sessionsRoot, 'rollout-' + owner + '.jsonl'), parentPath = join(archived, 'rollout-' + parent + '.jsonl' + (compressed ? '.zst' : ''))
  const prefix = row(0, 'session_meta', header(parent, producer)) + row(1, 'event_msg', start('inherited-turn'))
    + row(2, 'event_msg', user(parent, 'inherited-turn', 'Keep the synthetic inherited limit'))
    + row(3, 'event_msg', { type: 'task_complete', turn_id: 'inherited-turn' })
  await fs.writeFile(parentPath, compressed ? zstdCompressSync(Buffer.from(prefix)) : prefix)
  await fs.writeFile(path, row(4, 'session_meta', { ...header(owner, childProducer), forked_from_id: parent, history_base: { thread_id: parent, end_ordinal_exclusive: 4, end_byte_offset: Buffer.byteLength(prefix) } })
    + row(5, 'event_msg', start('turn')) + row(6, 'event_msg', user(owner, 'turn', 'Inspect the independent synthetic fork')))
  const source = { path, allowedRoot: sessionsRoot, threadId: owner, parentId: null, sourceKind: 'vscode', originator: 'Codex Desktop', cwd: '/synthetic-project', hostRunId: 'host-current', hostStartedAt: at - 10000, profile: currentHost, anchorTurnId: 'turn', nativeOriginVersion: 2, forkedFromId: parent }
  const metadata = { id: parent, path: parentPath, cwd: '/synthetic-project', parentThreadId: null, source: 'vscode', originator: 'Codex Desktop', forkedFromId: null }
  const native = { async readThread(id) { return { thread: id === parent ? metadata : null } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } } }
  const reader = new ForkHistoryReader(source, null, { native, sessionsRoot })
  t.after(async () => { await reader.close(); await fs.rm(dir, { recursive: true, force: true }) })
  const collect = async () => {
    const materials = [], history = [], boundaries = []; let view
    for (let pump = 0; pump < 100; pump++) {
      view = await reader.pump({ budgetBytes: 4096 })
      assert.ok(view.readBytes <= 4096); assert.ok(view.decodedBytes <= 262144)
      materials.push(...view.materials); history.push(...view.historyMaterials); boundaries.push(...view.historyBoundaries)
      if (view.coverage === 'caught-up' || view.coverage === 'gap') break
    }
    return { view, materials, history, boundaries }
  }
  return { collect }
}

test('the candidate Host reads a mixed old-producer ancestor and current-producer suffix without changing either proof mode', async t => {
  const f = await forkFixture(t, supportedProfile.cliVersion), result = await f.collect()
  assert.equal(result.view.coverage, 'caught-up', JSON.stringify({ gap: result.view.gap, historyGap: result.view.historyGap }))
  assert.equal(result.view.forkProvenance.complete, true)
  assert.deepEqual(result.materials.map(event => event.text), ['Inspect the independent synthetic fork'])
  assert.deepEqual(result.history.map(event => [event.text, event.data.inherited, event.data.history]), [['Keep the synthetic inherited limit', true, true]])
  assert.equal(result.boundaries[0].data.forkReceipt.version, 1)
  assert.equal(Object.hasOwn(result.boundaries[0].data.forkReceipt, 'userProofMode'), false)
})

test('an unknown stored item stops at a hashed coverage gap without claiming user, activity, or native body identity', async t => {
  const f = await fixture(t, { records: row(1, 'event_msg', start('turn'))
    + row(2, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item: { type: 'SyntheticOpaqueItem', kind: 'opaque', query: 'private-unapproved-query', action: { text: 'private-unapproved-action' }, results: ['private-unapproved-result'] } })
    + row(3, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item: { type: 'Reasoning', summary: ['private-reasoning'] } }) })
  const result = await collect(f.open())
  const typeHash = createHash('sha256').update(JSON.stringify('SyntheticOpaqueItem')).digest('hex')
  assert.equal(result.view.gap, 'unsupported-item-type:' + typeHash)
  assert.equal(result.view.effectivePhase, 'unknown'); assert.deepEqual(result.materials, [])
  assert.doesNotMatch(JSON.stringify(result), /SyntheticOpaqueItem|private-unapproved|private-reasoning/)
})

test('an unknown typed item start cannot be skipped by a later known execution end', async t => {
  const f = await fixture(t, { records: row(1, 'event_msg', start('turn'))
    + row(2, 'event_msg', user(owner, 'turn', 'Inspect only the synthetic request'))
    + row(3, 'event_msg', { type: 'item_started', thread_id: owner, turn_id: 'turn', item: { type: 'SyntheticOpaqueItem', query: 'unverified-start-body' } })
    + row(4, 'event_msg', { type: 'task_complete', turn_id: 'turn' }) })
  const reader = f.open(), result = await collect(reader)
  const typeHash = createHash('sha256').update(JSON.stringify('SyntheticOpaqueItem')).digest('hex')
  assert.equal(result.view.gap, 'unsupported-item-type:' + typeHash)
  assert.equal(result.view.coverage, 'gap'); assert.equal(result.view.effectivePhase, 'unknown')
  assert.deepEqual(result.lifecycle.map(event => event.type), ['task_started'])
  assert.deepEqual(result.materials.map(event => event.kind), ['input'])
  assert.doesNotMatch(JSON.stringify(result), /SyntheticOpaqueItem|unverified-start-body/)
  const checkpoint = JSON.parse(JSON.stringify(reader.checkpoint())); await reader.close()
  const restored = await collect(f.open({}, checkpoint))
  assert.equal(restored.view.effectivePhase, 'unknown'); assert.equal(restored.view.gap, 'cursor-retains-gap')
  assert.deepEqual(restored.materials, []); assert.deepEqual(restored.lifecycle, [])
})

test('typed item starts retain missing-type gaps and do not invent activity for known reasoning or command items', async t => {
  for (const [name, item, rejected] of [['missing-item', undefined, true], ['missing-type', {}, true], ['unknown-type', { type: 'SyntheticOpaqueItem' }, true], ['known-reasoning', { type: 'Reasoning', summary: ['private-started-reasoning'] }, false], ['known-command', { type: 'CommandExecution', aggregated_output: 'uncompleted-command-is-not-a-result', exit_code: 0 }, false]]) await t.test(name, async t => {
    const f = await fixture(t, { records: row(1, 'event_msg', start('turn')) + row(2, 'event_msg', user(owner, 'turn', 'Inspect only the synthetic request'))
      + row(3, 'event_msg', { type: 'item_started', thread_id: owner, turn_id: 'turn', ...(item ? { item } : {}) })
      + row(4, 'event_msg', { type: 'item_started', thread_id: owner, turn_id: 'turn', item: { type: 'CommandExecution', aggregated_output: 'later-start-is-not-a-result', exit_code: 0 } })
      + row(5, 'event_msg', { type: 'task_complete', turn_id: 'turn' }) })
    const result = await collect(f.open())
    assert.deepEqual(result.materials.map(event => [event.kind, event.text]), [['input', 'Inspect only the synthetic request']])
    assert.doesNotMatch(JSON.stringify(result), /private-started-reasoning|uncompleted-command-is-not-a-result|later-start-is-not-a-result|SyntheticOpaqueItem/)
    if (rejected) {
      const typeHash = createHash('sha256').update(JSON.stringify(item?.type ?? null)).digest('hex')
      assert.equal(result.view.gap, 'unsupported-item-type:' + typeHash)
      assert.equal(result.view.effectivePhase, 'unknown'); assert.deepEqual(result.lifecycle.map(event => event.type), ['task_started'])
    } else {
      assert.equal(result.view.gap, null); assert.equal(result.view.effectivePhase, 'completed')
      assert.deepEqual(result.lifecycle.map(event => event.type), ['task_started', 'task_complete'])
    }
  })
})

test('foreign typed item starts remain excluded from the current thread and turn', async t => {
  for (const [thread, turn] of [['another-thread', 'turn'], [owner, 'another-turn']]) await t.test(thread + ':' + turn, async t => {
    const f = await fixture(t, { records: row(1, 'event_msg', start('turn')) + row(2, 'event_msg', user(owner, 'turn', 'Inspect only the synthetic request'))
      + row(3, 'event_msg', { type: 'item_started', thread_id: thread, turn_id: turn, item: { type: 'SyntheticOpaqueItem', query: 'foreign-started-body' } })
      + row(4, 'event_msg', { type: 'task_complete', turn_id: 'turn' }) })
    const result = await collect(f.open())
    assert.equal(result.view.gap, null); assert.equal(result.view.effectivePhase, 'completed')
    assert.deepEqual(result.materials.map(event => event.kind), ['input'])
    assert.doesNotMatch(JSON.stringify(result), /foreign-started-body|SyntheticOpaqueItem/)
  })
})

test('object-array tool output stops at an explicit shape gap without reserving its future body identity', async t => {
  const f = await fixture(t, { records: row(1, 'event_msg', start('turn'))
    + row(2, 'response_item', { type: 'custom_tool_call_output', call_id: 'array-call', output: [{ type: 'input_text', text: 'Unverified array text' }, { opaque: { authorization: 'Bearer PRIVATE-ARRAY-TOKEN' } }] })
    + row(3, 'response_item', { type: 'custom_tool_call_output', call_id: 'scalar-call', output: 'Verified fixed scalar text' }) })
  const result = await collect(f.open())
  assert.equal(result.view.gap, 'unsupported-tool-result-shape')
  assert.equal(result.view.effectivePhase, 'unknown'); assert.deepEqual(result.materials, [])
  const scalarFixture = await fixture(t, { records: row(1, 'event_msg', start('turn')) + row(2, 'response_item', { type: 'custom_tool_call_output', call_id: 'scalar-call', output: 'Verified fixed scalar text' }) })
  const scalar = (await collect(scalarFixture.open())).materials[0]
  assert.equal(scalar.kind, 'tool-result'); assert.equal(scalar.text, 'Verified fixed scalar text')
  assert.doesNotMatch(JSON.stringify(result), /Unverified array text|PRIVATE-ARRAY-TOKEN|opaque|authorization/)
})

test('Host admission requires exact string fields instead of coercing similar or unknown profiles', async t => {
  assert.equal(supportsHostProfile(supportedProfile), true)
  assert.equal(supportsHostProfile(currentHost), true)
  for (const profile of [null, {}, { ...currentHost, appBuild: 20586 }, { ...currentHost, appVersion: [currentHost.appVersion] }, { ...currentHost, appBuild: '20587' }, { ...currentHost, cliVersion: '0.163.0-alpha.10.3' }]) {
    const f = await fixture(t, { profile, records: row(1, 'event_msg', start('turn')) })
    assert.equal(supportsHostProfile(profile), false)
    const result = await collect(f.open())
    assert.equal(result.view.gap, 'unsupported-host-version'); assert.equal(result.view.readBytes, 0)
    assert.deepEqual(result.materials, []); assert.deepEqual(result.lifecycle, [])
  }
})

test('candidate plain and zstd reads keep both byte budgets, native identities, and checkpoint progress', async t => {
  const records = row(1, 'event_msg', start('turn'))
    + row(2, 'response_item', { type: 'reasoning', encrypted_content: 'hidden-reasoning'.repeat(30000) })
    + row(3, 'event_msg', user(owner, 'turn', 'A bounded synthetic request'))
    + row(4, 'event_msg', { type: 'task_complete', turn_id: 'turn' })
  const f = await fixture(t, { records }), plain = f.open(), compressed = f.open({ path: f.plain + '.zst' })
  const a = await collect(plain, 65536), b = await collect(compressed, 65536)
  assert.equal(a.view.coverage, 'caught-up'); assert.equal(b.view.coverage, 'caught-up')
  assert.equal(a.view.effectivePhase, 'completed'); assert.equal(b.view.effectivePhase, 'completed')
  assert.deepEqual(b.materials.map(event => [event.data.nativeRecordId, event.text]), a.materials.map(event => [event.data.nativeRecordId, event.text]))
  assert.deepEqual(b.lifecycle.map(event => event.nativeRecordId), a.lifecycle.map(event => event.nativeRecordId))
  const checkpoint = JSON.parse(JSON.stringify(compressed.checkpoint())); await compressed.close()
  const resumed = f.open({ path: f.plain + '.zst' }, checkpoint), restored = await collect(resumed, 65536)
  assert.equal(restored.view.coverage, 'caught-up'); assert.equal(resumed.checkpoint().offset, checkpoint.offset)
  assert.deepEqual(restored.materials, []); assert.deepEqual(restored.lifecycle, [])
  assert.doesNotMatch(JSON.stringify(restored) + JSON.stringify(checkpoint), /hidden-reasoning/)
})

test('old producer progress is retained within one Host profile and explicitly refuses cross-profile migration', async t => {
  const f = await fixture(t, { producer: supportedProfile.cliVersion, profile: supportedProfile, records: row(1, 'event_msg', start('turn')) + row(2, 'event_msg', user(owner, 'turn', 'Old producer request')) })
  const old = f.open(), first = await collect(old), checkpoint = JSON.parse(JSON.stringify(old.checkpoint())), retained = structuredClone(checkpoint)
  assert.equal(first.view.coverage, 'caught-up'); assert.ok(checkpoint.offset > 0); await old.close()
  const restored = await collect(f.open({}, checkpoint))
  assert.equal(restored.view.coverage, 'caught-up'); assert.deepEqual(restored.materials, []); assert.deepEqual(restored.lifecycle, [])
  const changed = await collect(f.open({ profile: currentHost }, checkpoint))
  assert.equal(changed.view.gap, 'cursor-invalid'); assert.equal(changed.view.readBytes, 0)
  assert.deepEqual(changed.materials, []); assert.deepEqual(changed.lifecycle, [])
  assert.deepEqual(checkpoint, retained, 'the rejected checkpoint is not rewritten or migrated')
  const independentlyVerified = await collect(f.open({ profile: currentHost }))
  assert.equal(independentlyVerified.view.coverage, 'caught-up')
  assert.deepEqual(independentlyVerified.materials.map(event => event.data.nativeRecordId), first.materials.map(event => event.data.nativeRecordId))
})

test('a live reader does not accept a different admitted Host or a changed profile reference', async t => {
  const f = await fixture(t, { profile: { ...currentHost }, records: row(1, 'event_msg', start('turn')) }), explicit = f.open()
  await collect(explicit)
  const other = await explicit.pump({ profile: supportedProfile })
  assert.equal(other.gap, 'unsupported-host-version'); assert.equal(other.readBytes, 0)
  const mutable = f.open(); await collect(mutable)
  Object.assign(f.source.profile, supportedProfile)
  const mutated = await mutable.pump()
  assert.equal(mutated.gap, 'unsupported-host-version'); assert.equal(mutated.readBytes, 0)
})

test('a metadata-only native checkpoint revalidates its real producer without requiring a material header in the cursor', async t => {
  const f = await fixture(t, { records: row(1, 'event_msg', start('turn')) }), initial = f.open({}, null, { mode: 'metadata' })
  assert.equal((await collect(initial)).view.coverage, 'caught-up')
  const checkpoint = JSON.parse(JSON.stringify(initial.checkpoint())); assert.equal(checkpoint.header, undefined); await initial.close()
  const resumed = await collect(f.open({}, checkpoint, { mode: 'metadata' }))
  assert.equal(resumed.view.gap, null); assert.equal(resumed.view.effectivePhase, 'started')
  assert.deepEqual(resumed.lifecycle, [])
})

test('new and old producers retain modern receipts while both exact legacy ancestor producers retain their legacy proof mode', async t => {
  for (const [producer, childProducer, compressed] of [[currentHost.cliVersion, supportedProfile.cliVersion, true], ['0.162.0-alpha.2', currentHost.cliVersion, false], ['0.160.1', currentHost.cliVersion, true]]) await t.test(producer, async t => {
    const f = await forkFixture(t, producer, { childProducer, compressed }), result = await f.collect()
    assert.equal(result.view.coverage, 'caught-up', JSON.stringify({ gap: result.view.gap, historyGap: result.view.historyGap }))
    assert.equal(result.view.forkProvenance.complete, true)
    const receipt = result.boundaries[0].data.forkReceipt
    if (producer === currentHost.cliVersion) { assert.equal(receipt.version, 1); assert.equal(Object.hasOwn(receipt, 'userProofMode'), false) }
    else { assert.equal(receipt.version, 2); assert.equal(receipt.userProofMode, 'legacy-unverified') }
    assert.equal(receipt.users, 1); assert.equal(receipt.unconfirmed, 0)
    assert.ok(result.history.every(event => event.data.history === true && event.data.inherited === true))
  })
})

test('an unknown ancestor producer never releases inherited bodies or marks the fork complete', async t => {
  const f = await forkFixture(t, '0.163.0-alpha.999'), result = await f.collect()
  assert.notEqual(result.view.forkProvenance.complete, true)
  assert.deepEqual(result.history, []); assert.deepEqual(result.boundaries, [])
  assert.deepEqual(result.materials.map(event => event.text), ['Inspect the independent synthetic fork'])
})

test('candidate storage cannot bypass an acknowledged header or prefix fence in either encoding', async t => {
  for (const compressed of [false, true]) for (const mutation of ['header', 'body']) await t.test([compressed ? 'zstd' : 'plain', mutation].join('-'), async t => {
    const f = await fixture(t, { compressed, records: row(1, 'event_msg', start('turn')) + row(2, 'response_item', { type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'Original fixed output' }] }) })
    const initial = f.open(), result = await collect(initial)
    assert.equal(result.view.coverage, 'caught-up')
    const proof = result.view.nativeOrigins[0], fence = { key: proof.key, headerHash: proof.headerHash, prefix: proof.next, revision: 1 }
    await initial.close()
    const bytes = Buffer.from(f.bytes.toString().replace(mutation === 'header' ? '/synthetic-project' : 'Original fixed output', mutation === 'header' ? '/synthetic-rewrite' : 'Modified fixed output'))
    await fs.writeFile(f.source.path, compressed ? zstdCompressSync(bytes) : bytes)
    const changed = await collect(f.open({}, null, { origins: { async read() { return fence } } }))
    assert.equal(changed.view.gap, mutation === 'header' ? 'native_origin_header_changed' : 'native_origin_prefix_changed')
    assert.deepEqual(changed.materials, []); assert.deepEqual(changed.lifecycle, [])
  })
})

test('candidate identity and ordinal checks remain independent of Host admission', async t => {
  const f = await fixture(t, { records: row(1, 'event_msg', start('turn')) + row(8, 'event_msg', user(owner, 'turn', 'Bad ordinal request')) })
  const foreign = await collect(f.open({ threadId: '00000000-0000-4000-8000-000000000999' }))
  assert.equal(foreign.view.effectivePhase, 'unknown'); assert.deepEqual(foreign.materials, []); assert.deepEqual(foreign.lifecycle, [])
  const discontinuous = await collect(f.open({}, null, { forkSuffix: true }))
  assert.equal(discontinuous.view.gap, 'record-ordinal-unverified')
  assert.deepEqual(discontinuous.materials, []); assert.equal(discontinuous.view.effectivePhase, 'unknown')
})

async function publicFixture(t) {
  let clock = at
  t.mock.method(Date, 'now', () => clock)
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-current-material-http-')), dataDir = join(dir, 'core'), path = join(dir, 'rollout-' + owner + '.jsonl')
  await fs.mkdir(dataDir)
  // This is a bounded local wire fixture, not an actual model process or route.
  const wire = createServer(async (req, res) => {
    let body = ''; for await (const chunk of req) body += chunk
    res.setHeader('content-type', 'application/json')
    if (req.url === '/v1/chat/completions') res.end(JSON.stringify({ model: 'fixed-extractor', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(JSON.parse(body), { selectedOptionIds: [], gaps: [] })) } }] }))
    else {
      const request = decodeJevRequest(JSON.parse(body))
      res.end(JSON.stringify({ model: 'fixed-judge', answers: Object.fromEntries(Object.keys(request.questions).map(id => [id, { type: 'choice', choice: 'clear' }])) }))
    }
  })
  let server, core, adapter
  t.after(async () => { await adapter?.close(); await core?.close(); await server?.close(); wire.closeAllConnections(); await new Promise(resolve => wire.close(resolve)); await fs.rm(dir, { recursive: true, force: true }) })
  await new Promise(resolve => wire.listen(0, '127.0.0.1', resolve))
  const baseUrl = 'http://127.0.0.1:' + wire.address().port
  await fs.writeFile(path, row(0, 'session_meta', header(owner, currentHost.cliVersion)) + row(1, 'event_msg', start('turn'))
    + row(2, 'event_msg', user(owner, 'turn', 'Read only this synthetic log'))
    + row(3, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item: { type: 'AgentMessage', id: 'agent-output', content: [{ type: 'Text', text: 'The synthetic log has been inspected' }] } }))
  server = await startServer({ dataDir, now: () => clock, cooldownMs: 0 }); core = await connectCore({ dataDir })
  await configureInstallationModels(core, 'chatgpt:candidate-material-http', { llm: { baseUrl, model: 'fixed-extractor' }, jev: { baseUrl, model: 'fixed-judge' } })
  // The actual adapter remains on its old admitted Host. The stored producer is
  // current: this tests the candidate storage policy without enabling a new Host.
  const native = { async sampleHost() { return { available: true, hostRunId: 'public-fixture', hostStartedAt: at - 10000, profile: supportedProfile } }, async readThread(id) { return id === owner ? { thread: { id, path, cwd: dir, parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop' }, anchorTurnId: 'turn' } : null }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async close() {} }
  const reopen = async () => { await adapter?.close(); adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'candidate-material-http', token: 'a'.repeat(64), pollMs: 0, collectMaterials: true, nativeOriginVersion: 2, now: () => clock }) }
  await reopen()
  const read = async () => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'a'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: owner } } }) })
    assert.equal(response.status, 200); const result = await response.json(); assert.equal(result.result.isError, undefined)
    return result.result.structuredContent
  }
  return { core, path, reopen, read, advance() { clock += 1000 } }
}

test('strict material gaps reach the real Core HTTP chain and cannot let a known end preserve an old green result or refresh capture identities', async t => {
  for (const kind of ['unknown-item', 'array-output', 'unknown-started-item', 'missing-started-type']) await t.test(kind, async t => {
    const f = await publicFixture(t)
    await f.read()
    let original
    for (let attempt = 0; attempt < 100; attempt++) {
      original = (await f.core.snapshot()).tasks[0]
      if (original?.gs.light === 'green') break
      await new Promise(resolve => setTimeout(resolve, 2))
    }
    assert.equal(original.gs.light, 'green'); assert.equal(original.currentRequest, 'Read only this synthetic log')
    assert.equal(original.coverage.complete, true)
    const captures = original.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence])
    assert.equal(captures.length, 3)
    const retained = (await f.core.diagnostics()).retained.nativeCaptureProgress.records
    f.advance()
    const unsupported = kind === 'array-output'
      ? row(4, 'response_item', { type: 'custom_tool_call_output', call_id: 'unverified', output: [{ type: 'input_text', text: 'Do not retain this body' }, { type: 'input_image', image_url: 'unverified-image-body' }] })
      : row(4, 'event_msg', { type: kind === 'unknown-item' ? 'item_completed' : 'item_started', thread_id: owner, turn_id: 'turn', item: { ...(kind === 'missing-started-type' ? {} : { type: 'SyntheticOpaqueItem' }), query: 'Do not retain this body' } })
    await fs.appendFile(f.path, unsupported + row(5, 'event_msg', { type: 'task_complete', turn_id: 'turn' }))
    let view
    for (let attempt = 0; attempt < 20; attempt++) { view = await f.read(); if (view.observation.gap?.startsWith('unsupported-')) break }
    assert.equal(view.signal, 'gray'); assert.notEqual(view.observation.coverage, 'caught-up')
    assert.match(view.observation.gap, kind === 'array-output' ? /^unsupported-tool-result-shape$/ : /^unsupported-item-type:[a-f0-9]{64}$/)
    let after = (await f.core.snapshot()).tasks[0]
    assert.equal(after.status, 'unknown'); assert.equal(after.coverage.complete, false); assert.notEqual(after.gs.light, 'green')
    assert.equal(after.lastActivity, original.lastActivity)
    assert.deepEqual(after.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), captures)
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, retained)
    assert.doesNotMatch(JSON.stringify(after.events), /Do not retain|SyntheticOpaqueItem|task_complete/)
    await f.reopen(); view = await f.read(); after = (await f.core.snapshot()).tasks[0]
    assert.equal(view.signal, 'gray'); assert.equal(after.coverage.complete, false)
    assert.deepEqual(after.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), captures)
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, retained)
    assert.equal(after.events.some(event => event.data.nativeType === 'task_complete'), false)
  })
})
