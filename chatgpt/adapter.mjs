import { createHash } from 'node:crypto'
import { mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { BoundedReader, supportedProfile } from './bounded-reader.mjs'
import { startPanelServer } from './panel-server.mjs'
import { loadCursor, saveCursor } from './cursor-store.mjs'

const fingerprint = value => createHash('sha256').update(JSON.stringify(value)).digest('hex')
const profileKey = value => JSON.stringify([value?.appVersion, value?.appBuild, value?.cliVersion])
const phaseStatus = phase => phase === 'started' ? 'running' : ['completed', 'aborted'].includes(phase) ? 'idle' : 'unknown'
const gapView = (reason, threadId = null) => ({ identity: { threadId }, signal: 'gray', summary: '无法确认当前观察', observation: { phase: 'unknown', status: 'unknown', gap: reason }, analysis: { state: 'not-connected', summary: '六项分析尚未接入' } })

function requestIdentity(meta) {
  const values = ['thread_id', 'threadId'].filter(key => Object.hasOwn(meta ?? {}, key)).map(key => meta[key])
  if (!values.length || values.some(value => typeof value !== 'string' || !value.trim() || value.length > 128)) return null
  return new Set(values).size === 1 ? values[0] : null
}

// Native is the read-only host IO boundary; all qualification and lifecycle
// decisions remain in this adapter and the shared core.
export async function startChatGPTAdapter({ core, native, sessionsRoot, dataDir, installationId, token, pollMs = 1500, port = 0, now = Date.now }) {
  if (!core || !native || !sessionsRoot || !dataDir || typeof installationId !== 'string' || !installationId.trim()) throw new Error('adapter installation and source IO are required')
  if (!Number.isSafeInteger(pollMs) || pollMs < 0) throw new Error('invalid polling interval')
  await mkdir(join(dataDir, 'cursors'), { recursive: true, mode: 0o700 })
  let closed = false, timer = null, refreshPending = null, source = null, sourceEpoch = 0, registration = null
  const threads = new Map(), operations = new Set()
  const track = promise => { operations.add(promise); promise.then(() => operations.delete(promise), () => operations.delete(promise)); return promise }
  const capability = async () => (await core.health()).features?.includes('source-observation/1') === true

  async function stop(entry, reason, revokeTask = true) {
    entry.generation++
    const reader = entry.reader
    entry.reader = null; entry.view = gapView(reason)
    await reader?.close()
    if (revokeTask && source && entry.lifecycleId && entry.hostRunId === source.hostRunId) {
      await core.ingest(source.key, { tasks: [{ id: entry.id, sessionId: entry.id, lifecycleId: entry.lifecycleId, title: 'ChatGPT 主会话', status: 'unknown', gaps: [reason] }] })
    }
  }

  async function stopSource(reason) {
    sourceEpoch++
    const previous = source
    source = null
    await Promise.all([...threads.values()].map(entry => stop(entry, reason, false)))
    if (previous) await core.unregisterSource(previous.key, reason)
  }

  async function fail(entry, gap, scope = 'task') {
    if (scope === 'source') await stopSource(gap)
    else await stop(entry, gap)
    return entry.view
  }

  async function sourceFor(host, epoch) {
    if (source) return source.hostRunId === host.hostRunId ? source : null
    if (!registration) {
      registration = (async () => {
        const key = await core.registerSource({ registrationId: 'chatgpt:' + installationId, hostRunId: host.hostRunId, name: 'ChatGPT · 本机执行域', mode: 'observation-only', capabilities: ['main-lifecycle/1'] })
        if (closed || epoch !== sourceEpoch) { await core.unregisterSource(key, 'source-generation-superseded'); return null }
        source = { key, hostRunId: host.hostRunId }
        return source
      })().finally(() => { registration = null })
    }
    const registered = await registration
    return registered?.hostRunId === host.hostRunId ? registered : null
  }

  async function qualify(id, expected = null) {
    const metadata = await native.readThread(id)
    const thread = metadata?.thread
    if (!thread || thread.id !== id || thread.parentThreadId !== null || !['vscode', 'cli', 'appServer'].includes(thread.source) || thread.originator !== 'Codex Desktop') return { gap: 'main-identity-unverified' }
    if (thread.forkedFromId) return { gap: 'fork-provenance-unverified' }
    if (typeof thread.path !== 'string' || typeof thread.cwd !== 'string') return { gap: 'main-source-unverified' }
    const host = await native.sampleHost()
    if (!host?.available) return { gap: host?.reason ?? 'source-unavailable', scope: 'source' }
    if (profileKey(host.profile) !== profileKey(supportedProfile)) return { gap: 'unsupported-host-version', scope: 'source' }
    if (typeof host.hostRunId !== 'string' || !host.hostRunId || !Number.isSafeInteger(host.hostStartedAt)) return { gap: 'source-generation-unverified', scope: 'source' }
    const enabled = await native.readEnablement(thread.cwd)
    if (enabled?.user !== 'enabled' || enabled?.effective !== 'enabled') return { gap: enabled?.user === 'disabled' || enabled?.effective === 'disabled' ? 'plugin-disabled' : 'plugin-enablement-unconfirmed', scope: enabled?.user !== 'enabled' ? 'source' : 'task' }
    const source = { path: thread.path, allowedRoot: sessionsRoot, threadId: id, parentId: null, sourceKind: thread.source, originator: thread.originator, forkedFromId: null, anchorTurnId: metadata.anchorTurnId ?? null, hostRunId: host.hostRunId, hostStartedAt: host.hostStartedAt, profile: host.profile }
    if (expected && fingerprint([source.path, source.hostRunId, source.hostStartedAt, source.profile]) !== fingerprint([expected.path, expected.hostRunId, expected.hostStartedAt, expected.profile])) return { gap: 'source-generation-changed', scope: source.hostRunId !== expected.hostRunId ? 'source' : 'task' }
    return { source }
  }

  async function update(entry) {
    try {
      if (closed) return gapView('adapter-closed')
      if (!await capability()) return await fail(entry, 'core-capability-unavailable', 'source')
      const epoch = sourceEpoch
      const before = await qualify(entry.id, entry.reader?.source)
      if (before.gap) return await fail(entry, before.gap, before.scope)
      if (entry.blocked) {
        if (entry.blocked.scope === fingerprint(before.source)) { entry.view = gapView(entry.blocked.reason); return entry.view }
        entry.blocked = null
      }
      const generation = entry.generation
      if (closed) return gapView('adapter-closed')
      if (!entry.reader) {
        let cursor = null
        try { cursor = await loadCursor(join(dataDir, 'cursors', entry.cursorKey + '.json')) } catch (error) { await stop(entry, error.code ?? 'cursor-unavailable'); return entry.view }
        if (cursor) { entry.captured = new Map(cursor.captures.map(item => [item.offset, item.capturedAt])); entry.captureThroughOffset = cursor.captureThroughOffset }
        entry.reader = new BoundedReader(before.source, cursor?.reader)
      }
      if (closed) return gapView('adapter-closed')
      const previousCheckpoint = entry.reader.checkpoint()
      const observed = await entry.reader.pump()
      const after = await qualify(entry.id, before.source)
      if (closed || epoch !== sourceEpoch || generation !== entry.generation) return gapView('observation-superseded')
      if (after.gap || observed.coverage === 'gap') {
        if (!after.gap && ['start-predates-host', 'host-start-time-overlap'].includes(observed.gap)) entry.blocked = { scope: fingerprint(before.source), reason: observed.gap }
        return await fail(entry, after.gap ?? observed.gap, after.scope)
      }
      const connection = await sourceFor(before.source, epoch)
      if (!connection) return await fail(entry, 'source-generation-changed', 'source')
      const phase = observed.effectivePhase, lifecycleId = observed.observed.turnId ?? 'unobserved:' + before.source.hostRunId
      const status = phaseStatus(phase)
      const previousLifecycle = entry.lifecycleId, previousPhase = entry.view.observation.phase
      entry.lifecycleId = lifecycleId; entry.hostRunId = before.source.hostRunId
      const lifecycleEvents = observed.recentLifecycle.filter(event => event.turnId === lifecycleId)
      for (const event of lifecycleEvents) if (!entry.captured.has(event.recordStart)) {
        if (event.recordStart <= entry.captureThroughOffset) return await fail(entry, 'cursor-capture-history-unavailable')
        rememberCapture(entry, event.recordStart)
      }
      entry.captureThroughOffset = Math.max(entry.captureThroughOffset, observed.committedOffset - 1)
      const checkpoint = entry.reader.checkpoint(), target = join(dataDir, 'cursors', entry.cursorKey + '.json')
      // Persist first-capture identities before sending. A lost response replays
      // from the previous position with the same timestamps after recovery.
      await saveCursor(target, { ...previousCheckpoint, fileId: previousCheckpoint.fileId ?? checkpoint.fileId, attachSize: previousCheckpoint.attachSize ?? checkpoint.attachSize, knownSize: checkpoint.knownSize }, entry.captured, entry.captureThroughOffset)
      await core.ingest(connection.key, { tasks: [...(previousLifecycle && previousLifecycle !== lifecycleId ? [{ id: entry.id, sessionId: entry.id, lifecycleId: previousLifecycle, title: 'ChatGPT 主会话', status: ['completed', 'aborted'].includes(previousPhase) ? 'idle' : 'unknown', gaps: ['completed', 'aborted'].includes(previousPhase) ? [] : ['execution-superseded'] }] : []), { id: entry.id, sessionId: entry.id, lifecycleId, title: 'ChatGPT 主会话', status }], events: lifecycleEvents.map(event => ({ id: 'lifecycle:' + event.recordStart, taskId: entry.id, taskLifecycleId: event.turnId, kind: 'status', capturedAt: entry.captured.get(event.recordStart), occurredAt: event.at, text: event.type, evidence: 'chatgpt-record:' + entry.cursorKey + ':' + event.recordStart, data: { status: phase === 'unknown' ? 'unknown' : phaseStatus(event.type === 'task_started' ? 'started' : event.type === 'task_complete' ? 'completed' : 'aborted'), nativeType: event.type, runId: event.turnId } })) })
      if (closed || epoch !== sourceEpoch || generation !== entry.generation) { await stop(entry, 'observation-superseded'); return entry.view }
      await saveCursor(target, checkpoint, entry.captured, entry.captureThroughOffset)
      const snapshot = await core.snapshot()
      if (closed) return gapView('adapter-closed')
      if (epoch !== sourceEpoch || generation !== entry.generation || source?.key !== connection.key) return entry.view
      const task = snapshot.tasks.find(task => task.sourceKey === connection.key && task.id === entry.id && task.lifecycleId === lifecycleId)
      if (!task || task.analysis.state !== 'observation-only') { await stop(entry, 'core-capability-unconfirmed'); return entry.view }
      entry.view = { identity: { threadId: entry.id, cursorKey: entry.cursorKey }, signal: 'gray', summary: phase === 'started' ? '已观察到执行开始 · 尚无分析结论' : phase === 'completed' ? '本轮已结束 · 尚无分析结论' : phase === 'aborted' ? '本轮已中断 · 尚无分析结论' : '当前执行尚未确认', observation: { phase, status: task.status, coverage: observed.coverage, gap: observed.gap, readBytes: observed.readBytes, observedAt: observed.observed.at }, analysis: { state: 'not-connected', summary: '六项分析尚未接入' } }
      return entry.view
    } catch { await stop(entry, 'observation-unavailable').catch(() => {}); return entry.view }
  }

  function rememberCapture(entry, offset) { const at = now(); if (!Number.isSafeInteger(at) || at < 0) throw new Error('invalid capture clock'); entry.captured.set(offset, at); if (entry.captured.size > 16) entry.captured.delete(entry.captured.keys().next().value); return at }
  function run(entry) { if (!entry.pending) { entry.pending = track(update(entry)).finally(() => { entry.pending = null }); } return entry.pending }
  async function read(meta) {
    const id = requestIdentity(meta)
    if (!id) return gapView('request-identity-unverified')
    if (closed) return gapView('adapter-closed')
    let entry = threads.get(id)
    if (!entry && threads.size >= 256) return gapView('observation-capacity-reached')
    if (!entry) { entry = { id, cursorKey: fingerprint([installationId, id]), generation: 0, captured: new Map(), captureThroughOffset: -1, view: gapView('not-observed', id) }; threads.set(id, entry) }
    const view = await run(entry)
    if (!view.identity.threadId && !entry.reader && !entry.lifecycleId && !entry.blocked) threads.delete(id)
    return view
  }
  async function refresh() { if (closed) return; if (refreshPending) return refreshPending; refreshPending = (async () => { for (const entry of threads.values()) if (!closed) await run(entry) })().finally(() => { refreshPending = null }); return refreshPending }
  const panel = await startPanelServer({ read, token, port })
  const schedule = () => { if (!closed && pollMs) timer = setTimeout(async () => { timer = null; await refresh(); schedule() }, pollMs) }
  schedule()
  return { url: panel.url, read, refresh, diagnostics: () => ({ closed, trackedThreads: threads.size, openReaders: [...threads.values()].filter(entry => entry.reader?.handle).length, pendingOperations: operations.size, refreshTimers: timer === null ? 0 : 1, ...panel.diagnostics() }), async close() { if (closed) return; closed = true; clearTimeout(timer); timer = null; await panel.close(); await Promise.allSettled([...operations]); await stopSource('adapter-closed').catch(() => {}); threads.clear(); await native.close?.() } }
}
