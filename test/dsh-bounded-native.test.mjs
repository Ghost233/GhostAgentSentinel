import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import zlib from 'node:zlib'
import { syncBuiltinESMExports } from 'node:module'
import { createHash } from 'node:crypto'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { req } from './dsh-native-runtime.mjs'
import { createAdapter } from '../dsh/host.mjs'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

// Only this isolated test process controls delivery of its own completed native IO.
const nativeOpen = fs.open, nativeDecode = zlib.zstdDecompress
const io = { suffix: '', mode: null, enabled: false, entered: 0, nativeFinished: 0, delivered: 0, actualBytes: 0, actualClosed: 0, opened: 0, handles: [], gate: Promise.resolve(), release: undefined }
fs.open = async (...args) => {
  const file = await nativeOpen(...args)
  if (!io.enabled || args[1] !== 'r' || !String(args[0]).endsWith(io.suffix)) return file
  io.opened++; io.handles.push(file)
  const read = file.read.bind(file), close = file.close.bind(file)
  file.read = async (...readArgs) => {
    if (io.mode !== 'read') return read(...readArgs)
    io.entered++
    const result = await read(...readArgs)
    io.nativeFinished++; io.actualBytes += result.bytesRead
    await io.gate; io.delivered++
    return result
  }
  file.close = async () => { await close(); io.actualClosed++ }
  return file
}
zlib.zstdDecompress = (...args) => {
  if (!io.enabled || io.mode !== 'decode') return nativeDecode(...args)
  io.entered++
  return nativeDecode(args[0], args[1], (error, value) => {
    io.nativeFinished++; io.actualBytes += value?.length ?? 0
    void io.gate.then(() => { io.delivered++; args[2](error, value) })
  })
}
syncBuiltinESMExports()
const boundedPagingSupported = ['observeSessionPage', 'releaseObservationPageCursor'].every(name => typeof req('@deepseek-ai/dsh-session-query').default.prototype[name] === 'function')
const extendedPaging = { skip: boundedPagingSupported ? false : 'unsupported: stock SDK has no bounded cold page / cursor-release API; extended SDK scenario retained' }
const { Context } = req('@deepseek-ai/cordis')
const { SESSION_FORMAT_VERSION } = req('@deepseek-ai/dsh-session')
const { createUserMessage, freezeMessage } = req('@deepseek-ai/dsh-llm')
const jsonlModule = req('@deepseek-ai/dsh-session-persistence-jsonl')
const Jsonl = jsonlModule.default ?? jsonlModule
const modules = ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop', 'dsh-fs-local', 'dsh-working-directory', 'dsh-subagent', 'dsh-session-query'].map(name => { const module = req(`@deepseek-ai/${name}`); return module.default ?? module })
const zero = { activeReads: 0, openFiles: 0, activeDecodes: 0, cursors: 0, retainedBytes: 0 }
const until = async (predicate, label) => {
  const deadline = performance.now() + 4000
  while (!await predicate()) { if (performance.now() >= deadline) assert.fail(typeof label === 'function' ? label() : label); await new Promise(resolve => setTimeout(resolve, 5)) }
}
const reports = []
const ioRecord = () => Object.fromEntries(['entered', 'nativeFinished', 'delivered', 'actualBytes', 'actualClosed', 'opened'].map(key => [key, io[key]]))

function turnLog(text, { offset = 0, turn = 1, at = Date.now() - 2000 } = {}) {
  return [
    { type: 'turn/start', seq: offset, time: at, data: { turn } },
    { type: 'user/message', seq: offset + 1, time: at + 1, data: createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text }] }), surfaceOp: 'append' },
    { type: 'step/start', seq: offset + 2, time: at + 2, data: { turn, step: 1 } },
    { type: 'assistant/message', seq: offset + 3, time: at + 3, data: { turn, step: 1,
      message: freezeMessage({ id: `assistant-${offset}`, role: 'assistant', content: [{ type: 'text', text: 'self-owned native result' }], source: { kind: 'model', provider: 'fixture', model: 'fixture' } }),
      stream: [
        { type: 'chunk', time: at + 2, chunk: { type: 'block-start', index: 0, blockType: 'text' } },
        { type: 'text-chunks', time0: at + 2, index: 0, dt: [], texts: ['self-owned native result'] },
        { type: 'chunk', time: at + 3, chunk: { type: 'block-end', index: 0, block: { type: 'text', text: 'self-owned native result' } } },
        { type: 'chunk', time: at + 3, chunk: { type: 'finish', reason: { kind: 'stop' } } },
      ] }, surfaceOp: 'append' },
    { type: 'step/end', seq: offset + 4, time: at + 4, data: { turn, step: 1 } },
    { type: 'turn/end', seq: offset + 5, time: at + 5, data: { turn, reason: { kind: 'completed' } } },
  ]
}

async function fixture(t, { compression = 'zstd', expectedShutdown = 'closed', closeTimeoutMs = 5000 } = {}) {
  const storageDir = await fs.mkdtemp(join(tmpdir(), 'sentinel-built-pages-storage-'))
  const dataDir = await fs.mkdtemp(join(tmpdir(), 'sentinel-built-pages-core-')), ctx = new Context(), writer = new Context()
  let server, parent, adapter, keeper, keeperKey
  const record = { scope: 'Built native SDK JSONL/public Query; production Adapter/collector; authenticated public Core HTTP; self-owned contexts/storage only, no installed Host or actual model', compression, producers: [], pages: [], pageRequests: [], pageErrors: [], resources: [], cleanup: [] }
  reports.push(record)
  t.after(async () => {
    io.release?.(); io.enabled = false
    if (adapter) { record.finalDiagnostics = adapter.diagnostics(); record.finalSnapshot = await adapter.snapshot().catch(error => ({ error: error.message })) }
    const failures = []
    for (const release of [async () => { if (adapter) { const result = await adapter.close(); record.shutdown = result; assert.equal(result.status, expectedShutdown, JSON.stringify(result)) } }, () => parent?.dispose(), () => ctx.fiber.dispose(), () => writer.fiber.dispose(), async () => { if (keeperKey) await keeper.unregisterSource(keeperKey, 'self-owned fixture complete'); await keeper?.close() }, () => server?.close(), () => fs.rm(storageDir, { recursive: true, force: true }), () => fs.rm(dataDir, { recursive: true, force: true })]) {
      try { await release() } catch (error) { failures.push(error); record.cleanup.push(error.message) }
    }
    if (failures.length) throw new AggregateError(failures, 'built page integration cleanup failed')
  })
  await ctx.plugin(req('@deepseek-ai/dsh-llm').default)
  for (const module of modules) await ctx.plugin(module, { agents: [] })
  parent = await ctx.agents.create({ sessionId: 'built-page-root', agentOptions: { provider: 'unused', model: 'unused' } })
  await writer.plugin(Jsonl, { root: storageDir, compression })
  await ctx.plugin(Jsonl, { root: storageDir, compression })
  const publicPage = ctx.sessionQuery.observeSessionPage?.bind(ctx.sessionQuery)
  if (publicPage) assert.equal(ctx.sessionPersistence.observationPageSupported, true)
  if (publicPage) ctx.sessionQuery.observeSessionPage = async (id, request) => {
    record.pageRequests.push({ id, budget: request.budget, resources: { ...ctx.sessionPersistence.observationPageResources } })
    try { return await publicPage(id, request) }
    catch (error) { record.pageErrors.push({ id, code: error.code ?? null, stage: error.stage ?? null, message: error.message, usage: error.usage ?? null }); throw error }
  }
  server = await startServer({ dataDir })
  keeper = await connectCore({ dataDir })
  keeperKey = await keeper.registerSource({ registrationId: 'built-page-keeper', hostRunId: 'other-host', name: 'Other independent source' })
  await keeper.ingest(keeperKey, { tasks: [{ id: 'other-task', lifecycleId: 'other-life', title: 'Other source task', status: 'running' }] })
  async function store(id, events, inheritedEventCount) {
    const header = { version: SESSION_FORMAT_VERSION, id, createdAt: Date.now() - 3000, isSeeded: inheritedEventCount !== undefined, parentSession: parent.agent.session.id, origin: 'subagent' }
    record.producers.push({ id, header, inheritedEventCount: inheritedEventCount ?? 0, events: { count: events.length,
      sha256: createHash('sha256').update(JSON.stringify(events)).digest('hex'), seqRange: [events[0]?.seq ?? null, events.at(-1)?.seq ?? null], times: [events[0]?.time ?? null, events.at(-1)?.time ?? null],
      messages: events.filter(event => event.type === 'user/message').map(event => ({ id: event.data.id, seq: event.seq, text: event.data.content.filter(block => block.type === 'text').map(block => block.text).join('\n') })) } })
    const handle = await writer.sessionPersistence.create(header, inheritedEventCount === undefined ? undefined : { inheritedEventCount })
    try { for (let index = 0; index < events.length; index += 100) { await handle.append(events.slice(index, index + 100)); await handle.flush() } }
    finally { await handle.close() }
    const metadata = await writer.sessionPersistence.stat(id)
    record.resources.push({ stage: `stored:${id}`, metadata: { sizeBytes: metadata.sizeBytes, eventCount: metadata.eventCount ?? null }, sdk: { ...ctx.sessionPersistence.observationPageResources } })
    return metadata
  }
  const start = () => {
    adapter = createAdapter(ctx, { dataDir, registrationId: 'built-page-source' }, { flushMs: 5, heartbeatMs: 25, closeTimeoutMs })
    return adapter
  }
  const catalog = id => parent.agent.session.append('subagent/catalog', { version: 0, childId: id, childCreatedAt: Date.now(), mode: 'one-shot' })
  return { ctx, writer, parent, record, store, start, catalog, keeper, keeperKey, get adapter() { return adapter } }
}

test('built compressed cold pages and nested seeded rewind reach public Core without promoting ancestor material or restoring an Agent', extendedPaging, async t => {
  let releaseSeed
  t.after(() => releaseSeed?.())
  const f = await fixture(t), at = Date.now() - 2000
  const ordinary = [...turnLog('OWN-COLD-REQUEST', { at }), ...Array.from({ length: 600 }, (_, index) => ({ type: 'session/title', seq: index + 6, time: at + 6 + index, data: { title: `bounded native title ${index}` } })),
    { type: 'subagent/catalog', seq: 606, time: at + 606, data: { version: 2, childId: 'normal-owned-external', mode: 'external', childCreatedAt: at + 606 } }]
  const inherited = [...turnLog('INHERITED-ROOT-REQUEST-MUST-NOT-BECOME-CHILD', { at }),
    { type: 'session/end-seed', seq: 6, time: at + 6, data: { inherited: true } },
    { type: 'session/title', seq: 7, time: at + 7, data: { title: 'ancestor-one' } },
    { type: 'subagent/catalog', seq: 8, time: at + 8, data: { version: 2, childId: 'ancestor-foreign-external', mode: 'external', childCreatedAt: at + 8 } },
    { type: 'session/title', seq: 9, time: at + 9, data: { title: 'ancestor-three' } },
    { type: 'session/end-seed', seq: 10, time: at + 10, data: { inherited: true } },
    ...turnLog('OWN-NESTED-REQUEST', { offset: 11, turn: 2, at: at + 11 }),
    { type: 'subagent/catalog', seq: 17, time: at + 17, data: { version: 2, childId: 'nested-owned-external', mode: 'external', childCreatedAt: at + 17 } }]
  await f.store('cold-normal', ordinary)
  await f.store('cold-nested', inherited, 10)
  let prepares = 0, legacyReads = 0, leases = 0, disposed = 0
  const prepare = f.ctx.sessions.prepare.bind(f.ctx.sessions)
  f.ctx.sessions.prepare = (...args) => { prepares++; return prepare(...args) }
  f.ctx.sessionQuery.observeSession = async () => { legacyReads++; throw new Error('bounded integration must not call legacy full observeSession') }
  const observePage = f.ctx.sessionQuery.observeSessionPage.bind(f.ctx.sessionQuery)
  f.ctx.sessionQuery.observeSessionPage = async (id, request) => {
    const actualRequest = id === 'cold-nested' ? { ...request, budget: { ...request.budget, maxEvents: Math.min(7, request.budget.maxEvents) } } : request
    const page = await observePage(id, actualRequest)
    leases++
    f.record.pages.push({ id, budget: actualRequest.budget, cut: page.inheritedEventCount, seqs: page.events.map(event => event.seq), usage: page.usage, hasMore: page.hasMore, complete: page.complete,
      validation: page.validation, gaps: page.gaps, sdk: { ...f.ctx.sessionPersistence.observationPageResources } })
    const dispose = page[Symbol.dispose].bind(page)
    let released = false
    page[Symbol.dispose] = () => { if (!released) { disposed++; released = true }; return dispose() }
    if (id === 'cold-nested' && page.inheritedEventCount === null && !releaseSeed) await new Promise(resolve => { releaseSeed = resolve })
    return page
  }
  const service = f.start()
  await until(() => service.connected, 'production adapter connected')
  f.catalog('cold-normal'); f.catalog('cold-nested')
  await until(() => releaseSeed, () => `real seeded validation page held before final cut: ${JSON.stringify({ pageRequests: f.record.pageRequests, pageErrors: f.record.pageErrors, reader: service.diagnostics().hierarchyReader })}`)
  const first = await service.snapshot()
  assert.ok(!JSON.stringify(first).includes('INHERITED-ROOT-REQUEST-MUST-NOT-BECOME-CHILD'))
  assert.ok(!first.tasks.some(task => task.id === 'ancestor-foreign-external'))
  assert.ok(f.record.pages.filter(page => page.id === 'cold-nested').every(page => page.cut === null && page.seqs.length === 0))
  releaseSeed()
  let snapshot
  await until(async () => { snapshot = await service.snapshot(); return snapshot.tasks.some(task => task.id === 'nested-owned-external') && snapshot.tasks.some(task => task.id === 'normal-owned-external') && !service.diagnostics().hierarchyReader.pending }, 'real bounded cold branches accepted through public Core')
  const nested = snapshot.tasks.find(task => task.sessionId === 'cold-nested'), normal = snapshot.tasks.find(task => task.sessionId === 'cold-normal')
  assert.ok(nested.events.some(event => event.kind === 'input' && event.text === 'OWN-NESTED-REQUEST' && event.capturedAt === at + 12))
  assert.ok(normal.events.some(event => event.kind === 'input' && event.text === 'OWN-COLD-REQUEST' && event.capturedAt === at + 1))
  assert.ok(!JSON.stringify(snapshot).includes('INHERITED-ROOT-REQUEST-MUST-NOT-BECOME-CHILD'))
  assert.ok(!snapshot.tasks.some(task => task.id === 'ancestor-foreign-external'))
  assert.ok(snapshot.hierarchyContext.scopes.find(scope => scope.sessionId === 'cold-nested').coverage.branchGaps.some(gap => gap.reason.includes('whole-session relationships')))
  assert.equal(f.ctx.agents.get('cold-nested'), undefined); assert.equal(f.ctx.sessions.get('cold-nested'), undefined)
  assert.equal(f.ctx.agents.get('cold-normal'), undefined); assert.equal(f.ctx.sessions.get('cold-normal'), undefined)
  assert.equal(prepares, 0); assert.equal(legacyReads, 0); assert.equal(disposed, leases)
  assert.deepEqual(f.ctx.sessionPersistence.observationPageResources, zero)
  const reads = f.record.pages.length
  await Promise.all(Array.from({ length: 20 }, () => service.snapshot()))
  await new Promise(resolve => setTimeout(resolve, 40))
  assert.equal(f.record.pages.length, reads, 'views and heartbeats share the original bounded directory read')
  const nestedPages = f.record.pages.filter(page => page.id === 'cold-nested')
  assert.ok(nestedPages.filter(page => page.seqs.length).every(page => page.cut === 10))
  assert.deepEqual(nestedPages.flatMap(page => page.seqs), Array.from({ length: 18 }, (_, index) => index))
  assert.ok(f.record.pages.every(page => page.usage.physicalBytes <= page.budget.maxPhysicalBytes && page.usage.decodedBytes <= page.budget.maxDecodedBytes && page.usage.events <= page.budget.maxEvents))
  assert.equal((await f.keeper.snapshot()).sources.find(source => source.key === f.keeperKey).status, 'connected')
  f.record.resources.push({ stage: 'all-cold-pages-settled', sdk: { ...f.ctx.sessionPersistence.observationPageResources }, reader: service.diagnostics().hierarchyReader, prepares, legacyReads, leases, disposed })
})

for (const metadataKnown of [true, false]) test(`built legacy SDK refuses cold observation before IO with ${metadataKnown ? 'known small compressed' : 'unknown'} metadata`, async t => {
  const f = await fixture(t), id = 'legacy-cold', at = Date.now() - 2000
  const events = [...turnLog('LEGACY-COLD-BODY-MUST-STAY-UNREAD', { at }),
    { type: 'session/title', seq: 6, time: at + 6, data: { title: 'x'.repeat(524288) } },
    { type: 'subagent/catalog', seq: 7, time: at + 7, data: { version: 2, childId: 'legacy-unread-descendant', mode: 'external', childCreatedAt: at + 7 } }]
  const metadata = await f.store(id, events)
  assert.ok(metadata.sizeBytes < 262144, 'real compressed physical metadata falls below the reader byte budget')
  assert.ok(Buffer.byteLength(JSON.stringify(events)) > 262144, 'physical metadata does not constrain decoded material')
  const calls = { observe: 0, stat: 0, open: 0, read: 0, prepare: 0 }
  f.record.resources.push({ stage: 'legacy-before-IO', metadataKnown, calls })
  const query = f.ctx.sessionQuery, persistence = f.ctx.sessionPersistence
  const observe = query.observeSession.bind(query), stat = persistence.stat.bind(persistence), open = persistence.open.bind(persistence), prepare = f.ctx.sessions.prepare.bind(f.ctx.sessions)
  query.observeSessionPage = undefined
  query.observeSession = (...args) => { calls.observe++; return observe(...args) }
  persistence.stat = async (...args) => { calls.stat++; const value = await stat(...args); return metadataKnown || !value ? value : { header: value.header, revision: value.revision } }
  persistence.open = async (...args) => { calls.open++; const handle = await open(...args), read = handle.read.bind(handle); handle.read = (...readArgs) => { calls.read++; return read(...readArgs) }; return handle }
  f.ctx.sessions.prepare = (...args) => { calls.prepare++; return prepare(...args) }
  const service = f.start()
  await until(() => service.connected, 'legacy source connected')
  f.catalog(id)
  let snapshot
  await until(() => service.diagnostics().hierarchyReader.cached >= 2 && !service.diagnostics().hierarchyReader.pending, 'legacy branch attempt settled')
  snapshot = await service.snapshot()
  assert.deepEqual(calls, { observe: 0, stat: 0, open: 0, read: 0, prepare: 0 })
  assert.equal(snapshot.tasks.find(task => task.sessionId === id).status, 'unknown')
  assert.ok(snapshot.hierarchyContext.scopes.find(scope => scope.sessionId === id).coverage.branchGaps.some(gap => gap.reason.includes('有预算的只读会话分页')))
  assert.ok(!snapshot.tasks.some(task => task.id === 'legacy-unread-descendant'))
  assert.ok(!JSON.stringify(snapshot).includes('LEGACY-COLD-BODY-MUST-STAY-UNREAD'))
  assert.equal(f.ctx.agents.get(id), undefined); assert.equal(f.ctx.sessions.get(id), undefined)
  if (persistence.observationPageResources !== undefined) assert.deepEqual(persistence.observationPageResources, zero)
  else assert.equal(typeof query.observeSessionPage, 'undefined', 'stock SDK cannot report an extended paging resource counter')
  assert.equal(service.diagnostics().hierarchyReader.activeLeases, 0)
  assert.equal(service.diagnostics().hierarchyReader.pendingColdReads, 0)
  for (let index = 0; index < 20; index++) await service.snapshot()
  assert.deepEqual(calls, { observe: 0, stat: 0, open: 0, read: 0, prepare: 0 })
  assert.equal((await f.keeper.snapshot()).sources.find(source => source.key === f.keeperKey).status, 'connected')
  assert.equal((await service.close()).status, 'closed')
  assert.equal(service.diagnostics().adapter.activeRequests, 0)
  assert.equal(service.diagnostics().hierarchyReader.pending, 0)
  assert.equal(f.parent.agent.status, 'idle', 'unsupported observation never controls its native actor')
})

test('a built seeded read that exceeds its deadline remains unfinished without delivering a cut or disconnecting its source', extendedPaging, async t => {
  const f = await fixture(t, { compression: 'none' }), id = 'cold-seed-deadline', at = Date.now() - 2000
  await f.store(id, [...turnLog('UNCONFIRMED-ANCESTOR', { at }),
    { type: 'session/end-seed', seq: 6, time: at + 6, data: { inherited: true } },
    ...turnLog('UNREAD-OWN-REQUEST', { offset: 7, turn: 2, at: at + 7 })], 6)
  let signal, deliveredPages = 0
  const query = f.ctx.sessionQuery.observeSessionPage.bind(f.ctx.sessionQuery)
  f.ctx.sessionQuery.observeSessionPage = async (sessionId, request) => { if (sessionId === id) signal = request.signal; const page = await query(sessionId, request); deliveredPages++; return page }
  const service = f.start()
  await until(() => service.connected, 'deadline source connected')
  Object.assign(io, { mode: 'read', suffix: `/${id}/session.v4.jsonl`, enabled: true, entered: 0, nativeFinished: 0, delivered: 0, actualBytes: 0, actualClosed: 0, opened: 0, handles: [] })
  io.gate = new Promise(resolve => { io.release = resolve })
  f.catalog(id)
  await until(() => io.nativeFinished === 1 && signal?.aborted, 'actual seeded IO completion delivery held beyond the public read deadline')
  assert.equal(deliveredPages, 0)
  assert.equal(service.diagnostics().hierarchyReader.pendingColdReads, 1)
  assert.equal(f.ctx.sessionPersistence.observationPageResources.activeReads, 1)
  assert.equal(f.ctx.sessionPersistence.observationPageResources.openFiles, 1)
  f.record.resources.push({ stage: 'seed-deadline-native-complete-undelivered', io: ioRecord(), sdk: { ...f.ctx.sessionPersistence.observationPageResources } })
  io.release(); io.enabled = false
  await until(() => !service.diagnostics().hierarchyReader.pending, 'expired seeded read actually settled')
  assert.equal(deliveredPages, 0)
  assert.ok(f.record.pageErrors.some(error => error.id === id && error.code === 'SESSION_OBSERVATION_PAGE_ABORTED' && error.stage === 'read' && error.usage.physicalBytes > 0))
  const snapshot = await service.snapshot()
  assert.ok(snapshot.adapter.gaps.some(gap => gap.includes('只读观察已取消')))
  assert.equal(snapshot.hierarchyContext.sources[0].coverage.complete, false)
  assert.ok(!JSON.stringify(snapshot).includes('UNCONFIRMED-ANCESTOR'))
  assert.ok(!JSON.stringify(snapshot).includes('UNREAD-OWN-REQUEST'))
  assert.equal(service.connected, true)
  assert.equal((await f.keeper.snapshot()).sources.find(source => source.key === f.keeperKey).status, 'connected')
  assert.equal(f.ctx.agents.get(id), undefined); assert.equal(f.ctx.sessions.get(id), undefined)
  assert.deepEqual(f.ctx.sessionPersistence.observationPageResources, zero)
  assert.equal(io.actualClosed, 1)
  await assert.rejects(io.handles[0].stat(), { code: 'EBADF' })
  f.record.resources.push({ stage: 'seed-deadline-settled', io: ioRecord(), sdk: { ...f.ctx.sessionPersistence.observationPageResources }, reader: service.diagnostics().hierarchyReader })
})

for (const mode of ['read', 'decode']) test(`built ${mode} cancellation keeps the actual operation visible until native completion delivery and releases only the observer`, extendedPaging, async t => {
  const compression = mode === 'read' ? 'none' : 'zstd', f = await fixture(t, { compression, expectedShutdown: 'incomplete', closeTimeoutMs: 25 })
  await f.store(`cold-${mode}`, turnLog('owned-cancellation-request'))
  const service = f.start()
  await until(() => service.connected, 'initial adapter baseline connected')
  Object.assign(io, { mode, suffix: `/cold-${mode}/session.v4.jsonl${compression === 'zstd' ? '.zstd' : ''}`, enabled: true, entered: 0, nativeFinished: 0, delivered: 0, actualBytes: 0, actualClosed: 0, opened: 0, handles: [] })
  io.gate = new Promise(resolve => { io.release = resolve })
  f.catalog(`cold-${mode}`)
  await until(() => io.nativeFinished === 1, 'actual native operation completed with callback delivery held')
  assert.ok(io.actualBytes > 0)
  assert.equal(io.delivered, 0)
  assert.equal(io.opened, 1); assert.equal(io.actualClosed, 0)
  const before = { ...f.ctx.sessionPersistence.observationPageResources }
  assert.equal(before.activeReads, 1); assert.equal(before.openFiles, 1)
  assert.equal(before.activeDecodes, mode === 'decode' ? 1 : 0)
  f.record.resources.push({ stage: 'native-complete-before-delivery', io: ioRecord(), sdk: before })
  const shutdown = await service.close()
  assert.equal(shutdown.status, 'incomplete'); assert.equal(shutdown.timedOut, true)
  assert.equal(service.diagnostics().hierarchyReader.pendingColdReads, 1)
  assert.equal(f.ctx.sessionPersistence.observationPageResources.activeReads, 1)
  assert.equal(f.ctx.sessionPersistence.observationPageResources.openFiles, 1)
  const other = await f.keeper.snapshot()
  assert.equal(other.sources.find(source => source.key === f.keeperKey).status, 'connected')
  let toolCalls = 0
  f.ctx.tools.register({ name: 'continue_after_observer_stop', description: 'Self-owned side-effect-free native continuation', parameters: {},
    output: { schema: { type: 'string' }, render: (_args, value) => [{ type: 'text', text: value }] }, execute() { toolCalls++; return 'native-task-continues' } })
  const signal = new AbortController().signal
  const result = await f.ctx.tools.execute({ callId: `continue-${mode}`, name: 'continue_after_observer_stop', arguments: {}, agent: f.parent.agent, signal })
  assert.equal(result.value, 'native-task-continues'); assert.equal(toolCalls, 1); assert.equal(signal.aborted, false)
  f.record.resources.push({ stage: 'source-closed-operation-unsettled', io: ioRecord(), sdk: { ...f.ctx.sessionPersistence.observationPageResources }, shutdown, otherConnected: true, nativeToolCalls: toolCalls })
  io.release(); io.enabled = false
  await until(() => service.diagnostics().hierarchyReader.pending === 0, 'cancelled native operation actually delivered and settled')
  assert.deepEqual(f.ctx.sessionPersistence.observationPageResources, zero)
  assert.equal(service.diagnostics().hierarchyReader.activeLeases, 0)
  assert.equal(service.diagnostics().hierarchyReader.cursorReleaseFailures, 0)
  assert.equal(io.delivered, 1)
  assert.equal(io.actualClosed, 1)
  await assert.rejects(io.handles[0].stat(), { code: 'EBADF' }, 'actual FileHandle refuses access after SDK close')
  assert.ok(!(await f.keeper.snapshot()).tasks.some(task => task.sessionId === `cold-${mode}`))
  f.record.resources.push({ stage: 'actual-settlement-resources-zero', io: ioRecord(), sdk: { ...f.ctx.sessionPersistence.observationPageResources }, reader: service.diagnostics().hierarchyReader })
})

test.after(async () => {
  io.release?.(); io.enabled = false
  fs.open = nativeOpen; zlib.zstdDecompress = nativeDecode; syncBuiltinESMExports()
  if (process.env.SENTINEL_BUILT_PAGE_REPORT) {
    const sdkPaths = ['@deepseek-ai/dsh-session', '@deepseek-ai/dsh-session-query', '@deepseek-ai/dsh-session-persistence', '@deepseek-ai/dsh-session-persistence-jsonl', '@deepseek-ai/dsh-subagent']
    const artifacts = Object.fromEntries(await Promise.all(sdkPaths.map(async name => { const path = req.resolve(name); return [path, createHash('sha256').update(await fs.readFile(path)).digest('hex')] })))
    await fs.writeFile(process.env.SENTINEL_BUILT_PAGE_REPORT, JSON.stringify({ runtime: process.version, pid: process.pid, artifacts, reports, completedAt: new Date().toISOString() }, null, 2))
  }
})
