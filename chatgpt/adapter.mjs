import { createHash } from 'node:crypto'
import { mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { BoundedReader, supportedProfile } from './bounded-reader.mjs'
import { startPanelServer } from './panel-server.mjs'
import { loadCursor, saveCursor } from './cursor-store.mjs'
import { classifyThread, createDiscovery } from './discovery.mjs'

const fingerprint = value => createHash('sha256').update(JSON.stringify(value)).digest('hex')
const profileKey = value => JSON.stringify([value?.appVersion, value?.appBuild, value?.cliVersion])
const phaseStatus = phase => phase === 'started' ? 'running' : ['completed', 'aborted'].includes(phase) ? 'idle' : 'unknown'
const lifecycleGapView = (reason, threadId = null) => ({ identity: { threadId }, signal: 'gray', summary: '无法确认当前观察', observation: { phase: 'unknown', status: 'unknown', gap: reason }, analysis: { state: 'not-connected', summary: '六项分析尚未接入' } })

function requestIdentity(meta) {
  const values = ['thread_id', 'threadId'].filter(key => Object.hasOwn(meta ?? {}, key)).map(key => meta[key])
  if (!values.length || values.some(value => typeof value !== 'string' || !value.trim() || value.length > 128)) return null
  return new Set(values).size === 1 ? values[0] : null
}

function materialContext(snapshot, sourceKey, batch) {
  const key = task => JSON.stringify([task.id, task.lifecycleId])
  const tasks = new Map(snapshot.tasks.filter(task => task.sourceKey === sourceKey).map(task => [key(task), task]))
  for (const task of batch.tasks) tasks.set(key(task), { ...tasks.get(key(task)), ...task })
  const events = new Map([...tasks.values()].flatMap(task => task.events ?? []).map(event => [JSON.stringify([event.taskId, event.taskLifecycleId, event.id]), event]))
  for (const event of batch.events) { const id = JSON.stringify([event.taskId, event.taskLifecycleId, event.id]); if (!events.has(id)) events.set(id, event) }
  const byTask = new Map()
  for (const event of events.values()) { const id = JSON.stringify([event.taskId, event.taskLifecycleId]); if (!byTask.has(id)) byTask.set(id, []); byTask.get(id).push(event) }
  const activations = [], instructionBindings = [], sessions = new Set()
  const reference = event => ({ taskId: event.taskId, lifecycleId: event.taskLifecycleId, eventId: event.id })
  for (const task of tasks.values()) {
    sessions.add(task.id)
    const own = byTask.get(key(task)) ?? []
    const native = event => event.data?.sourceKind === 'chatgpt-native' && event.data?.sourceForm === 'event_msg' && event.data?.runId === task.lifecycleId
    const start = own.find(event => native(event) && event.data.nativeType === 'task_started')
    const end = own.findLast(event => native(event) && ['task_complete', 'turn_aborted'].includes(event.data.nativeType))
    const state = task.status === 'running' && start ? 'running' : ['idle', 'disposed'].includes(task.status) && end ? 'ended' : 'unknown'
    const proof = state === 'ended' ? end : start ?? end
    activations.push({ taskId: task.id, lifecycleId: task.lifecycleId, sessionId: task.id, runId: proof ? task.lifecycleId : null, state, stateRefs: proof ? [reference(proof)] : [] })
    if (!start || !['composer', 'edit_user_message'].includes(start.data.turnTrigger)) continue
    for (const input of own.filter(event => event.kind === 'input' && event.data?.nativeType === 'item_completed/UserMessage' && event.data.runId === task.lifecycleId && event.data.inputOrigin === 'user-entry' && event.data.inherited === false && !event.data.history)) {
      instructionBindings.push({ taskId: task.id, lifecycleId: task.lifecycleId, inputRef: reference(input), phase: 'consumed', origin: 'user-entry', originRefs: [reference(start), reference(input)], ancestorLimitRefs: [] })
    }
  }
  const previous = snapshot.sources.find(source => source.key === sourceKey)?.hierarchyContext
  return { schemaVersion: 1, revision: (previous?.revision ?? 0) + 1, sessions: [...sessions].map(sessionId => ({ sessionId })), activations, instructionBindings, relations: [], coverage: { complete: true, branchGaps: [] } }
}

function materialView(view, task) {
  const states = { queued: '检查排队中', analyzing: '正在检查', unavailable: '本地摘取模型或 JEV 未配置', failed: '本轮分析失败', unknown: '当前请求或分析证据不足', cooldown: '本轮六项检查已更新', idle: '最近检查结果' }
  const issues = task.alerts.filter(alert => alert.status === 'attention').map(alert => ({ checkId: alert.checkId, reason: alert.reason.slice(0, 240), evidence: (alert.evidenceIds ?? []).map(id => task.events.find(event => event.id === id)).filter(Boolean).slice(0, 2).map(event => ({ text: event.text.slice(0, 120), reference: event.evidence ?? event.id })) }))
  return { ...view, signal: task.gs?.light ?? 'gray', summary: (task.currentRequest ? task.gs?.reason ?? view.summary : '当前用户请求尚未核实，检查结论不可判断').slice(0, 240),
    observation: { ...view.observation, gap: view.observation.gap ?? (!task.currentRequest ? 'current-user-request-unverified' : null) },
    analysis: { connected: true, state: task.analysis.state, summary: states[task.analysis.state] ?? '本轮尚无有效检查结论', checks: task.analysis.checks, issues,
      resultDisposition: task.analysis.resultDisposition, waitMs: task.analysis.waitMs, lagMs: task.analysis.lagMs, error: task.analysis.error, coverage: task.analysis.coverage ?? task.coverage } }
}

// Native is the read-only host IO boundary; all qualification and lifecycle
// decisions remain in this adapter and the shared core.
export async function startChatGPTAdapter({ core, native, sessionsRoot, dataDir, installationId, token, pollMs = 1500, port = 0, now = Date.now, collectMaterials = false }) {
  if (!core || !native || !sessionsRoot || !dataDir || typeof installationId !== 'string' || !installationId.trim()) throw new Error('adapter installation and source IO are required')
  if (!Number.isSafeInteger(pollMs) || pollMs < 0) throw new Error('invalid polling interval')
  if (typeof collectMaterials !== 'boolean') throw new Error('invalid material collection mode')
  const gapView = (reason, threadId = null) => { const view = lifecycleGapView(reason, threadId); if (collectMaterials) view.analysis = { connected: true, state: 'unknown', summary: '当前观察不可用，检查结论不可判断', checks: [] }; return view }
  await mkdir(join(dataDir, 'cursors'), { recursive: true, mode: 0o700 })
  let closed = false, timer = null, refreshPending = null, source = null, sourceEpoch = 0, registration = null
  const threads = new Map(), operations = new Set()
  const discovery = createDiscovery({ native, sessionsRoot, now })
  let queue = Promise.resolve(), queuedBatches = 0, rotation = 0
  const track = promise => { operations.add(promise); promise.then(() => operations.delete(promise), () => operations.delete(promise)); return promise }
  const capability = async () => { const features = (await core.health()).features ?? []; return features.includes('source-observation/1') && (!collectMaterials || features.includes('chatgpt-main/1') && features.includes('hierarchy-context/1')) }

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
    for (const [id, entry] of threads) if (!entry.lifecycleId && !entry.blocked) threads.delete(id)
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
        const key = await core.registerSource({ registrationId: 'chatgpt:' + installationId, hostRunId: host.hostRunId, name: 'ChatGPT · 本机执行域', ...(collectMaterials ? { policy: 'chatgpt-main/1', capabilities: ['main-lifecycle/1', 'main-material/1'] } : { mode: 'observation-only', capabilities: ['main-lifecycle/1'] }) })
        if (closed || epoch !== sourceEpoch) { await core.unregisterSource(key, 'source-generation-superseded'); return null }
        source = { key, hostRunId: host.hostRunId }
        return source
      })().finally(() => { registration = null })
    }
    const registered = await registration
    return registered?.hostRunId === host.hostRunId ? registered : null
  }

  const hostGap = host => !host?.available ? host?.reason ?? 'source-unavailable' : profileKey(host.profile) !== profileKey(supportedProfile) ? 'unsupported-host-version' : typeof host.hostRunId !== 'string' || !host.hostRunId || !Number.isSafeInteger(host.hostStartedAt) ? 'source-generation-unverified' : null
  async function projectSetting(context, cwd) {
    if (!context.projects.has(cwd)) context.projects.set(cwd, Promise.resolve().then(() => native.readEnablement(cwd)).catch(() => ({ user: 'unknown', effective: 'unknown' })))
    return context.projects.get(cwd)
  }
  function entryFor(id) {
    let entry = threads.get(id)
    if (!entry && threads.size < 256) { entry = { id, cursorKey: fingerprint(collectMaterials ? [installationId, id, 'material/1'] : [installationId, id]), generation: 0, captured: new Map(), captureThroughOffset: -1, lastViewedAt: now(), view: gapView('not-observed', id) }; threads.set(id, entry) }
    return entry
  }
  async function qualify(id, expected = null, context) {
    const metadata = await native.readThread(id)
    const thread = metadata?.thread
    const kind = classifyThread(thread, sessionsRoot)
    if (!thread || thread.id !== id || kind !== 'main') return { gap: kind === 'fork' ? 'fork-provenance-unverified' : 'main-identity-unverified' }
    const host = context.host
    const enabled = await projectSetting(context, thread.cwd)
    if (enabled?.user !== 'enabled' || enabled?.effective !== 'enabled') return { gap: enabled?.user === 'disabled' || enabled?.effective === 'disabled' ? 'plugin-disabled' : 'plugin-enablement-unconfirmed', scope: enabled?.user !== 'enabled' ? 'source' : 'task' }
    const source = { path: thread.path, allowedRoot: sessionsRoot, threadId: id, parentId: null, sourceKind: thread.source, originator: thread.originator, cwd: thread.cwd, forkedFromId: null, anchorTurnId: metadata.anchorTurnId ?? null, hostRunId: host.hostRunId, hostStartedAt: host.hostStartedAt, profile: host.profile }
    if (expected && fingerprint([source.path, source.cwd, source.sourceKind, source.originator, source.hostRunId, source.hostStartedAt, source.profile]) !== fingerprint([expected.path, expected.cwd, expected.sourceKind, expected.originator, expected.hostRunId, expected.hostStartedAt, expected.profile])) return { gap: 'source-generation-changed', scope: source.hostRunId !== expected.hostRunId ? 'source' : 'task' }
    return { source }
  }

  async function prepare(entry, context, epoch) {
    try {
      const before = await qualify(entry.id, entry.reader?.source, context)
      if (before.gap) { await fail(entry, before.gap, before.scope); return null }
      if (entry.blocked) {
        if (entry.blocked.scope === fingerprint(before.source)) { entry.view = gapView(entry.blocked.reason); return null }
        entry.blocked = null
      }
      const generation = entry.generation
      if (closed || epoch !== sourceEpoch) return null
      if (!entry.reader) {
        let cursor = null
        try { cursor = await loadCursor(join(dataDir, 'cursors', entry.cursorKey + '.json')) } catch (error) { await stop(entry, error.code ?? 'cursor-unavailable'); return null }
        if (cursor) { entry.captured = new Map(cursor.captures.map(item => [item.offset, item.capturedAt])); entry.captureThroughOffset = cursor.captureThroughOffset }
        entry.reader = new BoundedReader(before.source, cursor?.reader, { mode: collectMaterials ? 'material' : 'metadata' })
      }
      if (closed) return null
      const previousCheckpoint = entry.reader.checkpoint(), observed = await entry.reader.pump()
      if (observed.coverage === 'gap') {
        if (['start-predates-host', 'host-start-time-overlap'].includes(observed.gap)) entry.blocked = { scope: fingerprint(before.source), reason: observed.gap }
        await fail(entry, observed.gap); return null
      }
      return { source: before.source, previousCheckpoint, observed, epoch, generation }
    } catch { await stop(entry, 'observation-unavailable').catch(() => {}); return null }
  }

  async function publish(entry, prepared) {
    try {
      const { observed, epoch, generation } = prepared
      const connection = await sourceFor(prepared.source, epoch)
      if (!connection) return await fail(entry, 'source-generation-changed', 'source')
      const phase = observed.effectivePhase, lifecycleId = observed.observed.turnId ?? 'unobserved:' + prepared.source.hostRunId
      const status = phaseStatus(phase)
      const previousLifecycle = entry.lifecycleId, previousPhase = entry.view.observation.phase
      entry.lifecycleId = lifecycleId; entry.hostRunId = prepared.source.hostRunId
      const lifecycleEvents = collectMaterials ? observed.recentLifecycle : observed.recentLifecycle.filter(event => event.turnId === lifecycleId)
      const materials = collectMaterials ? observed.materials : []
      for (const event of [...lifecycleEvents, ...materials].sort((a, b) => a.recordStart - b.recordStart)) if (!entry.captured.has(event.recordStart)) {
        if (event.recordStart <= entry.captureThroughOffset) return await fail(entry, 'cursor-capture-history-unavailable')
        rememberCapture(entry, event.recordStart)
      }
      entry.captureThroughOffset = Math.max(entry.captureThroughOffset, observed.committedOffset - 1)
      const checkpoint = entry.reader.checkpoint(), target = join(dataDir, 'cursors', entry.cursorKey + '.json')
      // Persist first-capture identities before sending. A lost response replays
      // from the previous position with the same timestamps after recovery.
      await saveCursor(target, { ...prepared.previousCheckpoint, fileId: prepared.previousCheckpoint.fileId ?? checkpoint.fileId, attachSize: prepared.previousCheckpoint.attachSize ?? checkpoint.attachSize, knownSize: checkpoint.knownSize }, entry.captured, entry.captureThroughOffset)
      const batch = { tasks: [...(previousLifecycle && previousLifecycle !== lifecycleId ? [{ id: entry.id, sessionId: entry.id, lifecycleId: previousLifecycle, title: 'ChatGPT 主会话', status: ['completed', 'aborted'].includes(previousPhase) ? 'idle' : 'unknown', gaps: ['completed', 'aborted'].includes(previousPhase) ? [] : ['execution-superseded'] }] : []), { id: entry.id, sessionId: entry.id, lifecycleId, title: 'ChatGPT 主会话', status }], events: lifecycleEvents.map(event => ({ id: (collectMaterials ? 'material-lifecycle:' : 'lifecycle:') + event.recordStart, taskId: entry.id, taskLifecycleId: event.turnId, kind: 'status', capturedAt: entry.captured.get(event.recordStart), occurredAt: event.at, text: event.type, evidence: 'chatgpt-record:' + entry.cursorKey + ':' + event.recordStart, data: { status: !collectMaterials && phase === 'unknown' ? 'unknown' : phaseStatus(event.type === 'task_started' ? 'started' : event.type === 'task_complete' ? 'completed' : 'aborted'), nativeType: event.type, runId: event.turnId, ...(collectMaterials ? { sourceKind: 'chatgpt-native', sourceForm: 'event_msg', ...(event.turnTrigger ? { turnTrigger: event.turnTrigger } : {}) } : {}) } })) }
      if (collectMaterials) {
        batch.events.push(...materials.map(event => ({ id: 'material:' + event.recordStart, taskId: entry.id, taskLifecycleId: event.turnId, kind: event.kind, capturedAt: entry.captured.get(event.recordStart), occurredAt: event.at, text: event.text, data: event.data, evidence: 'chatgpt-record:' + entry.cursorKey + ':' + event.recordStart })))
        batch.events.sort((a, b) => Number(a.id.split(':')[1]) - Number(b.id.split(':')[1]))
        const before = await core.snapshot()
        if (closed || epoch !== sourceEpoch || generation !== entry.generation) return entry.view
        const outputs = new Map(before.tasks.filter(task => task.sourceKey === connection.key).flatMap(task => task.events).filter(event => event.kind === 'output' && event.data?.messageId).map(event => [JSON.stringify([event.taskId, event.taskLifecycleId, event.data.messageId]), event]))
        batch.events = batch.events.filter(event => {
          if (event.kind !== 'output' || !event.data?.messageId) return true
          const key = JSON.stringify([event.taskId, event.taskLifecycleId, event.data.messageId]), original = outputs.get(key)
          if (!original) { outputs.set(key, event); return true }
          if (original.id === event.id) return true
          if (original.text === event.text && Boolean(original.data?.truncated) === Boolean(event.data.truncated) && Boolean(original.data?.redacted) === Boolean(event.data.redacted)) return false
          event.kind = 'gap'; event.text = '同一原生输出身份的正文不一致，采集覆盖未确认'; event.data = { ...event.data, truncated: true }
          return true
        })
        const descriptions = new Map(batch.tasks.map(task => [task.lifecycleId, task]))
        const known = before.tasks.filter(task => task.sourceKey === connection.key && task.id === entry.id)
        for (const turnId of new Set(batch.events.map(event => event.taskLifecycleId))) if (!descriptions.has(turnId)) descriptions.set(turnId, { id: entry.id, sessionId: entry.id, lifecycleId: turnId, title: 'ChatGPT 主会话', status: 'unknown' })
        for (const event of [...known.flatMap(task => task.events), ...batch.events]) if (event.taskLifecycleId !== lifecycleId && event.data?.sourceKind === 'chatgpt-native' && ['task_complete', 'turn_aborted'].includes(event.data?.nativeType)) descriptions.set(event.taskLifecycleId, { id: entry.id, sessionId: entry.id, lifecycleId: event.taskLifecycleId, title: 'ChatGPT 主会话', status: 'idle' })
        batch.tasks = [...descriptions.values()]
        batch.hierarchyContext = materialContext(before, connection.key, batch)
      }
      await core.ingest(connection.key, batch)
      if (closed || epoch !== sourceEpoch || generation !== entry.generation) { await stop(entry, 'observation-superseded'); return entry.view }
      await saveCursor(target, checkpoint, entry.captured, entry.captureThroughOffset)
      const snapshot = await core.snapshot()
      if (closed) return gapView('adapter-closed')
      if (epoch !== sourceEpoch || generation !== entry.generation || source?.key !== connection.key) return entry.view
      const task = snapshot.tasks.find(task => task.sourceKey === connection.key && task.id === entry.id && task.lifecycleId === lifecycleId)
      if (!task || !collectMaterials && task.analysis.state !== 'observation-only') { await stop(entry, 'core-capability-unconfirmed'); return entry.view }
      entry.view = { identity: { threadId: entry.id, cursorKey: entry.cursorKey }, signal: 'gray', summary: phase === 'started' ? '已观察到执行开始 · 尚无分析结论' : phase === 'completed' ? '本轮已结束 · 尚无分析结论' : phase === 'aborted' ? '本轮已中断 · 尚无分析结论' : '当前执行尚未确认', observation: { phase, status: task.status, coverage: observed.coverage, gap: observed.gap, readBytes: observed.readBytes, observedAt: observed.observed.at }, analysis: { state: 'not-connected', summary: '六项分析尚未接入' } }
      if (collectMaterials) entry.view = materialView(entry.view, task)
      return entry.view
    } catch { await stop(entry, 'observation-unavailable').catch(() => {}); return entry.view }
  }

  function rememberCapture(entry, offset) { const at = now(); if (!Number.isSafeInteger(at) || at < 0) throw new Error('invalid capture clock'); entry.captured.set(offset, at); if (entry.captured.size > 16) entry.captured.delete(entry.captured.keys().next().value); return at }
  function enqueue(action) {
    if (closed || queuedBatches >= 64) return null
    queuedBatches++
    const pending = queue.then(() => closed ? undefined : action())
    queue = pending.catch(() => {})
    return track(pending.finally(() => { queuedBatches-- }))
  }
  function decorate(view) { return { ...view, discovery: { ...discovery.snapshot(), tracked: threads.size, connected: source ? 1 : 0, currentIncluded: Boolean(view.identity.threadId && threads.has(view.identity.threadId)) } } }

  async function collect(requested = [], discover = false) {
    const epoch = sourceEpoch
    try {
      if (!await capability()) { await stopSource('core-capability-unavailable'); return }
      const host = await native.sampleHost(), user = await native.readEnablement(null)
      const gap = hostGap(host) ?? (user?.user !== 'enabled' || user?.effective !== 'enabled' ? user?.user === 'disabled' || user?.effective === 'disabled' ? 'plugin-disabled' : 'plugin-enablement-unconfirmed' : null)
      if (gap) { await stopSource(gap); return }
      if (discover) await discovery.refresh()
      if (closed || epoch !== sourceEpoch) return
      for (const [id, entry] of threads) {
        const kind = discovery.kind(id)
        if (kind && kind !== 'main') await stop(entry, kind === 'fork' ? 'fork-provenance-unverified' : 'directory-object-excluded')
        if (!discovery.has(id) && now() - entry.lastViewedAt >= 86400000) { try { await stop(entry, 'directory-identity-expired') } finally { threads.delete(id) } }
      }
      let ids = requested
      if (discover) {
        const candidates = discovery.candidates()
        for (const id of threads.keys()) if (!discovery.has(id) && !candidates.includes(id)) candidates.push(id)
        ids = candidates.length ? Array.from({ length: Math.min(20, candidates.length) }, (_, index) => candidates[(rotation + index) % candidates.length]) : []
        rotation = candidates.length ? (rotation + ids.length) % candidates.length : 0
      }
      const prepared = new Map(), before = { host, projects: new Map() }
      for (const id of ids) {
        if (closed || epoch !== sourceEpoch) break
        const entry = entryFor(id)
        if (entry) {
          const value = await prepare(entry, before, epoch)
          if (value) prepared.set(entry, value)
          else if (!entry.reader && !entry.lifecycleId && !entry.blocked) threads.delete(id)
        }
      }
      if (closed || epoch !== sourceEpoch) return
      // A separate fresh sample follows every reader operation in the batch.
      const afterHost = await native.sampleHost(), afterUser = await native.readEnablement(null)
      const afterGap = hostGap(afterHost) ?? (afterHost.hostRunId !== host.hostRunId || afterHost.hostStartedAt !== host.hostStartedAt ? 'source-generation-changed' : null) ?? (afterUser?.user !== 'enabled' || afterUser?.effective !== 'enabled' ? afterUser?.user === 'disabled' || afterUser?.effective === 'disabled' ? 'plugin-disabled' : 'plugin-enablement-unconfirmed' : null)
      if (afterGap) { await stopSource(afterGap); return }
      const after = { host: afterHost, projects: new Map() }, valid = new Map()
      for (const [entry, value] of prepared) {
        if (closed || epoch !== sourceEpoch) break
        const proof = await qualify(entry.id, value.source, after)
        if (proof.gap) await fail(entry, proof.gap, proof.scope)
        else valid.set(entry, value)
      }
      for (const [entry, value] of valid) {
        if (closed || epoch !== sourceEpoch || value.generation !== entry.generation) break
        await publish(entry, value)
      }
      if (source && !closed && epoch === sourceEpoch) await core.heartbeat(source.key)
    } catch { await stopSource('observation-unavailable').catch(() => {}) }
  }
  async function read(meta) {
    const id = requestIdentity(meta)
    if (!id) return decorate(gapView('request-identity-unverified'))
    if (closed) return decorate(gapView('adapter-closed'))
    const entry = entryFor(id)
    if (!entry) return decorate(gapView('observation-capacity-reached'))
    entry.lastViewedAt = now()
    if (queuedBatches) {
      const user = await native.readEnablement(null).catch(() => ({ user: 'unknown', effective: 'unknown' }))
      if (closed) return decorate(gapView('adapter-closed'))
      if (user?.user !== 'enabled' || user?.effective !== 'enabled') { await stopSource(user?.user === 'disabled' || user?.effective === 'disabled' ? 'plugin-disabled' : 'plugin-enablement-unconfirmed'); return decorate(entry.view) }
    }
    const pending = enqueue(() => collect([id]))
    if (!pending) return decorate(gapView(closed ? 'adapter-closed' : 'collection-queue-capacity'))
    await pending
    return decorate(closed ? gapView('adapter-closed') : entry.view)
  }
  async function refresh() { if (closed) return; if (refreshPending) return refreshPending; const pending = enqueue(() => collect([], true)); if (!pending) return; refreshPending = pending.finally(() => { refreshPending = null }); return refreshPending }
  const panel = await startPanelServer({ read, token, port })
  const schedule = () => { if (!closed && pollMs) timer = setTimeout(async () => { timer = null; await refresh(); schedule() }, pollMs) }
  if (pollMs && typeof native.listThreads === 'function') void refresh()
  schedule()
  return { url: panel.url, read, refresh, discovery: () => decorate(gapView('not-observed')).discovery, diagnostics: () => ({ closed, trackedThreads: threads.size, openReaders: [...threads.values()].filter(entry => entry.reader?.handle).length, pendingOperations: operations.size, refreshTimers: timer === null ? 0 : 1, queuedBatches, ...discovery.diagnostics(), ...panel.diagnostics() }), async close() { if (closed) return; closed = true; clearTimeout(timer); timer = null; const directoryClosing = discovery.close(); await native.close?.(); await panel.close(); await Promise.allSettled([...operations]); await directoryClosing; await stopSource('adapter-closed').catch(() => {}); threads.clear() } }
}
