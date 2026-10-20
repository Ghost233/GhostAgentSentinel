import { createHash, randomUUID } from 'node:crypto'
import { mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { BoundedReader } from './bounded-reader.mjs'
import { supportsHostProfile } from './storage-compatibility.mjs'
import { ForkHistoryReader } from './fork-history.mjs'
import { startPanelServer } from './panel-server.mjs'
import { loadCursor, saveCursor } from './cursor-store.mjs'
import { classifyThread, createDiscovery, nativeStorageRoot } from './discovery.mjs'
import { openHealthOutbox } from './health-outbox.mjs'
import { readModelDirectory } from './model-directory.mjs'
import { projectInstallationDiagnostics } from '../src/installation-diagnostics.mjs'
import { failureErrorInfo } from '../src/core/failure-receipts.mjs'
import { createNotificationDelivery } from './notifications.mjs'
import { CHATGPT_INSTALLATION_PURGE_FEATURE, CHATGPT_RETENTION_MAINTENANCE_FEATURE } from '../src/protocol.mjs'
import { FORK_STORAGE_FEATURE, FORK_LINEAGE_FEATURE, FORK_LEGACY_FEATURE, FORK_COMPRESSION_FEATURE } from '../src/fork-proof.mjs'

const fingerprint = value => createHash('sha256').update(JSON.stringify(value)).digest('hex')
const phaseStatus = phase => phase === 'started' ? 'running' : ['completed', 'aborted'].includes(phase) ? 'idle' : 'unknown'
const nativeEvidence = (entry, event, offset) => event.data?.nativeRecordId || event.nativeRecordId ? 'chatgpt-native-record:' + (event.data?.nativeRecordId ?? event.nativeRecordId) : 'chatgpt-record:' + entry.cursorKey + ':' + offset
const collectionFailures = new Set(['record-source-unavailable', 'record-identity-changed', 'record-file-replaced', 'record-file-truncated', 'record-rewrite-suspected', 'cursor-anchor-changed', 'record-malformed-or-over-limit', 'invalid-lifecycle-record', 'cursor-invalid', 'cursor-retains-gap', 'unsupported-record-type', 'unsupported-event-type', 'compressed-data-invalid', 'native_origin_prefix_changed', 'native_origin_header_changed'])
const combineHealth = (...values) => {
  const failed = values.filter(value => value?.state === 'failed')
  const failures = new Map(failed.flatMap(value => value.failures ?? []).map(failure => [failure.id ?? JSON.stringify([failure.domain, failure.channel, failure.roundId, failure.error]), failure]))
  return { state: failed.length ? 'failed' : values.some(value => value?.state === 'recovered') ? 'recovered' : 'unknown', failures: [...failures.values()].slice(0, 6), orderingGap: values.some(value => value?.orderingGap === true) }
}
const lifecycleGapView = (reason, threadId = null) => ({ identity: { threadId }, signal: 'gray', summary: '无法确认当前观察', observation: { phase: 'unknown', status: 'unknown', gap: reason }, analysis: { state: 'not-connected', summary: '六项分析尚未接入' } })

function requestIdentity(meta) {
  const values = ['thread_id', 'threadId'].filter(key => Object.hasOwn(meta ?? {}, key)).map(key => meta[key])
  if (!values.length || values.some(value => typeof value !== 'string' || !value.trim() || value.length > 128)) return null
  return new Set(values).size === 1 ? values[0] : null
}

function materialContext(snapshot, sourceKey, batch, inheritance = null) {
  const previous = snapshot.sources.find(source => source.key === sourceKey)?.hierarchyContext
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
    const native = event => event.data?.sourceKind === 'chatgpt-native' && event.data?.sourceForm === 'event_msg' && event.data?.runId === task.lifecycleId && !event.data?.history && !event.data?.inherited
    const start = own.find(event => native(event) && event.data.nativeType === 'task_started')
    const end = own.findLast(event => native(event) && ['task_complete', 'turn_aborted'].includes(event.data.nativeType))
    const state = task.status === 'running' && start ? 'running' : ['idle', 'disposed'].includes(task.status) && end ? 'ended' : 'unknown'
    const proof = state === 'ended' ? end : start ?? end
    activations.push({ taskId: task.id, lifecycleId: task.lifecycleId, sessionId: task.id, runId: proof ? task.lifecycleId : null, state, stateRefs: proof ? [reference(proof)] : [] })
    if (!start || !['composer', 'edit_user_message'].includes(start.data.turnTrigger)) continue
    for (const input of own.filter(event => event.kind === 'input' && event.data?.nativeType === 'item_completed/UserMessage' && event.data.runId === task.lifecycleId && event.data.inputOrigin === 'user-entry' && event.data.inherited === false && !event.data.history)) {
      let forkContext
      if (input.data.forkHistoryPending === true) {
        const retained = inheritance?.taskId !== task.id ? previous?.instructionBindings.find(binding => binding.taskId === task.id && binding.lifecycleId === task.lifecycleId && binding.inputRef.eventId === input.id && binding.forkContext?.generation === input.data.forkGeneration)?.forkContext : null
        const receipt = inheritance?.taskId === task.id && inheritance.complete ? [...events.values()].find(event => event.taskId === task.id && event.data?.historyOwnerThreadId === task.id && event.data?.forkReceipt?.ownerGeneration === input.data.forkGeneration) : null
        const receiptId = retained?.receiptId ?? receipt?.data.forkReceipt.id ?? (inheritance?.taskId === task.id && inheritance.complete && inheritance.generation === input.data.forkGeneration ? inheritance.receiptId : null)
        forkContext = { generation: input.data.forkGeneration, ...(receiptId ? { receiptId } : {}) }
      }
      instructionBindings.push({ taskId: task.id, lifecycleId: task.lifecycleId, inputRef: reference(input), phase: 'consumed', origin: 'user-entry', originRefs: [reference(start), reference(input)], ancestorLimitRefs: [], ...(forkContext ? { forkContext } : {}) })
    }
  }
  return { schemaVersion: 1, revision: (previous?.revision ?? 0) + 1, sessions: [...sessions].map(sessionId => ({ sessionId })), activations, instructionBindings, relations: [], coverage: { complete: true, branchGaps: [] } }
}

function materialView(view, task, { historical = false, snapshot } = {}) {
  const states = { queued: '检查排队中', analyzing: '正在检查', unavailable: '本地摘取模型或 JEV 未配置', failed: '本轮分析失败', unknown: '当前请求或分析证据不足', cooldown: '本轮六项检查已更新', idle: '最近检查结果' }
  const findings = historical ? task.analysis.checks.filter(check => check.status === 'attention').map(check => ({ ...check, checkId: check.id })) : task.alerts.filter(alert => alert.status === 'attention')
  const issues = findings.map(alert => ({ checkId: alert.checkId, reason: alert.reason.slice(0, 240), notificationStatus: !historical && ['pending', 'claimed', 'submitted', 'failed', 'cancelled', 'expired'].includes(alert.notification?.status) ? alert.notification.status : null, evidence: (alert.evidenceIds ?? []).map(id => task.events.find(event => event.id === id)).filter(Boolean).slice(0, 2).map(event => ({ text: event.text.slice(0, 120), reference: event.evidence ?? event.id })) }))
  const own = task.events.filter(event => event.taskLifecycleId === task.lifecycleId && !event.data?.history && !event.data?.inherited)
  const inputRef = task.instructionContext?.binding?.inputRef
  const request = own.find(event => event.kind === 'input' && event.id === inputRef?.eventId && event.taskId === inputRef.taskId && event.taskLifecycleId === inputRef.lifecycleId && event.authorizationEligible)
  const currentFacts = !historical && request ? { request: { text: task.currentRequest, evidenceIds: [request.id] }, activity: { ...task.activity, evidenceIds: (task.activity?.evidenceIds ?? []).filter(id => own.some(event => event.id === id)) } } : null
  const forkBinding = task.instructionContext?.binding?.forkContext
  const inheritance = forkBinding ? { state: forkBinding.receiptId ? 'complete' : 'pending', gaps: [...new Set([...(task.instructionContext.gaps ?? []), ...(!historical ? view.inheritance?.gaps ?? [] : [])])] } : null
  const revalidating = !historical && view.observation?.recordRevalidating === true
  const generated = task.analysis.summary?.generated && (historical || ['current', 'reused'].includes(task.analysis.resultDisposition)) && !['failed', 'unavailable', 'interrupted', 'stale'].includes(task.analysis.state) ? task.analysis.summary.generated : null
  const generatedView = value => value ? { text: value.text, evidence: value.evidenceRefs.slice(0, 2).map(reference => {
    const owner = (snapshot?.tasks ?? [task]).find(candidate => candidate.sourceKey === task.sourceKey && candidate.id === reference.taskId && candidate.lifecycleId === reference.lifecycleId)
    const event = owner?.events.find(event => event.id === reference.eventId && event.taskId === reference.taskId && event.taskLifecycleId === reference.lifecycleId)
    return { text: event?.text?.slice(0, 240) ?? '引用材料当前不可读取', reference: event?.evidence ?? JSON.stringify(reference) }
  }) } : null
  const prior = task.analysis.lastSuccessfulSummary, readAt = snapshot?.generatedAt ?? Date.now()
  const lastSuccessfulSummary = prior && prior.completedAt <= readAt && prior.expiresAt > readAt && (!prior.configScope || prior.configScope.registrationId === task.analysis.currentConfigScope?.registrationId)
    ? { ...prior, generated: generatedView(prior.generated) } : null
  return { ...view, ...(currentFacts ? { currentFacts } : {}), ...(!historical && inheritance ? { inheritance } : {}), signal: task.conversationMonitoring?.paused || revalidating ? 'gray' : task.gs?.light ?? 'gray', summary: (task.conversationMonitoring?.paused ? '本聊天分析已暂停，继续只读采集' : revalidating ? '当前记录重验中' : task.currentRequest ? task.gs?.reason ?? view.summary : '当前用户请求尚未核实，检查结论不可判断').slice(0, 240),
    ...(task.conversationMonitoring ? { monitoring: task.conversationMonitoring } : {}),
    health: task.monitoringHealth,
    observation: { ...view.observation, gap: view.observation.gap ?? (!task.currentRequest ? 'current-user-request-unverified' : null) },
    analysis: { connected: true, state: task.analysis.state, summary: states[task.analysis.state] ?? '本轮尚无有效检查结论', checks: task.analysis.checks, issues,
      generated: generatedView(generated), lastSuccessfulSummary, configScope: task.analysis.configScope, currentConfigScope: task.analysis.currentConfigScope,
      resultDisposition: task.analysis.resultDisposition, completedAt: task.analysis.completedAt, through: task.analysis.through, waitMs: task.analysis.waitMs, lagMs: task.analysis.lagMs, error: task.analysis.error, coverage: task.analysis.coverage ?? task.coverage } }
}

// Native is the read-only host IO boundary; all qualification and lifecycle
// decisions remain in this adapter and the shared core.
export async function startChatGPTAdapter({ core, native, notificationIO, runtimeStatus, displayName = 'Sentinel', sessionsRoot, dataDir, installationId, token, pollMs = 1500, port = 0, now = Date.now, collectMaterials = false, nativeOriginVersion = 1, initialEnrollmentBaseline, onQualificationLost }) {
  if (!core || !native || !sessionsRoot || !dataDir || typeof installationId !== 'string' || !installationId.trim()) throw new Error('adapter installation and source IO are required')
  if (!['Sentinel', 'GhostSentinel', 'GhostSentinel test'].includes(displayName)) throw new Error('invalid adapter display name')
  if (!Number.isSafeInteger(pollMs) || pollMs < 0) throw new Error('invalid polling interval')
  if (![1, 2].includes(nativeOriginVersion)) throw new Error('invalid native origin version')
  if (typeof collectMaterials !== 'boolean') throw new Error('invalid material collection mode')
  if (notificationIO && !collectMaterials) throw new Error('native notifications require material monitoring')
  if (onQualificationLost !== undefined && typeof onQualificationLost !== 'function') throw new Error('invalid qualification loss handler')
  if (initialEnrollmentBaseline !== undefined && (!initialEnrollmentBaseline || typeof initialEnrollmentBaseline.initialize !== 'boolean' || !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(initialEnrollmentBaseline.id ?? ''))) throw new Error('invalid initial enrollment baseline')
  let baselineInitializationAvailable = initialEnrollmentBaseline?.initialize === true, baselineCoreInstanceId = null
  const gapView = (reason, threadId = null) => { const view = lifecycleGapView(reason, threadId); if (reason === 'unsupported-host-version') view.summary = '当前 App 版本尚未适配，材料采集已停止'; if (collectMaterials) view.analysis = { connected: true, state: 'unknown', summary: '当前观察不可用，检查结论不可判断', checks: [] }; return view }
  await mkdir(join(dataDir, 'cursors'), { recursive: true, mode: 0o700 })
  const healthOutbox = collectMaterials ? await openHealthOutbox({ dataDir, now }) : null
  let closed = false, closing = null, timer = null, refreshPending = null, source = null, sourceEpoch = 0, registration = null
  let sourceHealth = null, outboxError = null, coreInstanceId = null, availabilityFailure = null
  let notificationPending = null, notificationError = null
  let qualifiedHost = null
  const threads = new Map(), operations = new Set()
  const settingsLifetime = new AbortController()
  const delivery = notificationIO ? createNotificationDelivery({ core, native: notificationIO, installationId, qualify: qualifyNotification, now }) : null
  const storageVersion = collectMaterials && nativeOriginVersion === 2 ? 2 : 1
  const discovery = createDiscovery({ native, sessionsRoot, nativeOriginVersion: storageVersion, now })
  let queue = Promise.resolve(), queuedBatches = 0, rotation = 0
  const track = promise => { operations.add(promise); promise.then(() => operations.delete(promise), () => operations.delete(promise)); return promise }
  const capability = async () => {
    const health = await core.health(), features = health.features
    const required = collectMaterials ? [...(nativeOriginVersion === 2 ? [FORK_COMPRESSION_FEATURE] : []), 'source-observation/1', 'chatgpt-main/1', 'chatgpt-fork-context/1', FORK_STORAGE_FEATURE, FORK_LINEAGE_FEATURE, FORK_LEGACY_FEATURE, 'hierarchy-context/1', 'conversation-monitoring/1', 'monitoring-failures/1'] : ['source-observation/1']
    required.push(CHATGPT_INSTALLATION_PURGE_FEATURE, CHATGPT_RETENTION_MAINTENANCE_FEATURE)
    if (delivery || initialEnrollmentBaseline) required.push('monitoring-initial-enrollment-rollup/1')
    if (delivery) required.push('monitoring-notifications/1')
    const available = Array.isArray(features) && required.every(feature => features.includes(feature))
    if (available && baselineCoreInstanceId === null) baselineCoreInstanceId = health.instanceId
    else if (available && baselineCoreInstanceId !== health.instanceId) baselineInitializationAvailable = false
    return { available, instanceId: health.instanceId }
  }

  async function qualifyNotification(notice) {
    const epoch = sourceEpoch, { host, gap } = await qualification()
    const facts = { hostRunId: host?.hostRunId ?? null, appRunning: !closed && epoch === sourceEpoch && !hostGap(host), pluginEnabled: !closed && epoch === sourceEpoch && !gap, projectEnabled: null }
    if (!facts.appRunning || !facts.pluginEnabled || host.hostRunId !== notice.hostRunId || notice.mainId === null) return facts
    const proof = await qualify(notice.mainId, null, { host, projects: new Map() })
    return { ...facts, projectEnabled: proof.source ? true : proof.gap === 'plugin-disabled' ? false : null }
  }
  function refreshNotifications(facts) {
    if (!delivery || closed) return
    const pending = delivery.refresh(facts)
    if (pending === notificationPending) return
    notificationPending = pending
    track(pending).then(result => { notificationError = result.errorCode ?? null; if (notificationPending === pending) notificationPending = null }, () => { notificationError = 'native_unavailable'; if (notificationPending === pending) notificationPending = null })
  }

  async function collectionOutcome(entry, outcome, error, roundId, channel = entry ? 'main:' + fingerprint(entry.id) : 'directory') {
    if (!healthOutbox || !source || closed) return
    const scope = entry?.lifecycleVerified && entry.hostRunId === source.hostRunId ? { mainId: entry.id, lifecycleId: entry.lifecycleId } : entry?.qualifiedHostRunId === source.hostRunId ? { mainId: entry.id, lifecycleId: null } : { mainId: null, lifecycleId: null }
    const value = { sourceKey: source.key, ...scope, channel, roundId, outcome, settledAt: now(), ...(scope.mainId !== null ? { enrollmentPhase: scope.lifecycleId !== null ? 'established' : entry?.enrollmentPhase ?? 'unknown' } : {}), ...(error ? { error: failureErrorInfo({ code: error.code ?? 'collection_failed', codeHash: error.codeHash, stage: error.stage ?? 'collection', status: error.status, details: error.details }) } : {}) }
    try {
      healthOutbox.push(value)
      if (entry && outcome === 'succeeded') entry.enrollmentPhase = 'established'
      if (entry && outcome === 'succeeded' && scope.mainId !== null) {
        if (scope.lifecycleId !== null) healthOutbox.push({ ...value, lifecycleId: null })
        const sourceRecovery = { ...value, mainId: null, lifecycleId: null }
        delete sourceRecovery.enrollmentPhase
        healthOutbox.push(sourceRecovery)
      }
      if (healthOutbox.diagnostics().pending < 256) outboxError = null
    } catch (failure) { outboxError = { state: 'failed', failures: [{ domain: 'collection', error: failureErrorInfo(failure) }] } }
    if (typeof core.recordCollectionOutcome === 'function') await healthOutbox.flush(value => core.recordCollectionOutcome(value))
  }
  async function updateHealth(entry = null, snapshot = null) {
    if (!healthOutbox || !source || closed) return
    const owner = source, epoch = sourceEpoch, key = owner.key
    snapshot ??= await core.snapshot().catch(() => null)
    if (closed || epoch !== sourceEpoch || source !== owner) return
    sourceHealth = combineHealth(snapshot?.sources.find(value => value.key === key)?.monitoringHealth, healthOutbox.health({ sourceKey: key, mainId: null, lifecycleId: null }), outboxError)
    if (entry) {
      const task = snapshot?.tasks.find(task => task.sourceKey === key && task.id === entry.id && task.lifecycleId === entry.lifecycleId)
      const receipts = entry.qualifiedHostRunId === owner.hostRunId ? await core.failureReceipts({ sourceKey: key, mainId: entry.id, lifecycleId: null }) : null
      if (closed || epoch !== sourceEpoch || source !== owner) return
      const unanchored = entry.qualifiedHostRunId === owner.hostRunId ? receipts?.health ?? { state: 'unknown', failures: [], orderingGap: true } : null
      entry.health = combineHealth(task?.monitoringHealth, unanchored, healthOutbox.health({ sourceKey: key, mainId: entry.id, lifecycleId: entry.lifecycleId ?? null }))
      entry.view.health = combineHealth(entry.health, sourceHealth)
    }
  }

  async function stop(entry, reason, revokeTask = true) {
    entry.generation++
    const reader = entry.reader
    entry.reader = null; entry.view = { ...gapView(reason), ...(entry.health ? { health: entry.health } : {}) }
    await reader?.close()
    if (revokeTask && source && entry.lifecycleId && entry.hostRunId === source.hostRunId) {
      await core.ingest(source.key, { tasks: [{ id: entry.id, sessionId: entry.id, lifecycleId: entry.lifecycleId, title: 'ChatGPT 主会话', status: 'unknown', gaps: [reason] }] })
    }
  }

  async function stopSource(reason) {
    sourceEpoch++
    const previous = source
    source = null
    const metadataClosing = native.disconnectMetadata?.()
    const unregistering = previous && onQualificationLost ? core.unregisterSource(previous.key, reason) : null
    unregistering?.catch(() => {})
    if (previous && delivery) {
      const facts = { sourceKey: previous.key, hostRunId: previous.hostRunId, appRunning: null, pluginEnabled: reason === 'plugin-disabled' ? false : null }
      await core.setNativeNotificationEligibility(facts).catch(() => { notificationError = 'native_unavailable' })
      refreshNotifications(facts)
    }
    if (reason !== 'observation-unavailable') { sourceHealth = null; outboxError = null; availabilityFailure = null; for (const entry of threads.values()) entry.health = null }
    await Promise.all([unregistering, metadataClosing, ...[...threads.values()].map(entry => stop(entry, reason, false))])
    for (const [id, entry] of threads) if (!entry.lifecycleId && !entry.blocked) threads.delete(id)
    if (previous && !onQualificationLost) await core.unregisterSource(previous.key, reason)
  }

  async function fail(entry, gap, scope = 'task') {
    if (scope === 'source' && onQualificationLost && ['plugin-disabled', 'plugin-enablement-unconfirmed'].includes(gap)) await qualificationLost(gap)
    if (scope === 'source') await stopSource(gap)
    else await stop(entry, gap)
    return entry.view
  }

  async function sourceFor(host, epoch) {
    if (closed || epoch !== sourceEpoch) return null
    if (source) return source.hostRunId === host.hostRunId ? source : null
    if (!registration) {
      registration = (async () => {
        const enrollment = initialEnrollmentBaseline ? { id: initialEnrollmentBaseline.id, initialize: baselineInitializationAvailable } : undefined
        baselineInitializationAvailable = false
        const key = await core.registerSource({ ...(enrollment ? { initialEnrollmentBaseline: enrollment } : {}), registrationId: 'chatgpt:' + installationId, hostRunId: host.hostRunId, name: (displayName === 'Sentinel' ? '' : displayName + ' · ') + 'ChatGPT · 本机执行域', ...(collectMaterials ? { policy: 'chatgpt-main/1', ...(nativeOriginVersion === 2 ? { nativeOriginVersion: 2 } : {}), capabilities: ['main-lifecycle/1', 'main-material/1'] } : { mode: 'observation-only', capabilities: ['main-lifecycle/1'] }) })
        if (closed || epoch !== sourceEpoch) { await core.unregisterSource(key, 'source-generation-superseded'); return null }
        source = { key, hostRunId: host.hostRunId }
        return source
      })().finally(() => { registration = null })
    }
    const registered = await registration
    return registered?.hostRunId === host.hostRunId ? registered : null
  }

  const hostGap = host => host?.available !== true ? host?.reason ?? 'source-unavailable' : typeof host.hostRunId !== 'string' || !host.hostRunId || !Number.isSafeInteger(host.hostStartedAt) ? 'source-generation-unverified' : null
  let qualificationLoss, qualificationError
  function qualificationLost(reason) {
    if (!onQualificationLost || closed) return qualificationLoss
    return qualificationLoss ??= (async () => {
      // Await only endpoint withdrawal. Full close waits for this operation.
      try { await onQualificationLost(reason) } catch (error) { qualificationError = error }
      void close(reason).catch(() => {})
    })()
  }
  async function qualification(expected = null) {
    let host, user
    try { host = await native.sampleHost() }
    catch (error) { if (!onQualificationLost) throw error; const gap = 'host-verification-unavailable'; await qualificationLost(gap); return { host: null, gap } }
    const previous = expected ?? qualifiedHost
    const gap = hostGap(host) ?? (previous && (host.hostRunId !== previous.hostRunId || host.hostStartedAt !== previous.hostStartedAt) ? 'source-generation-changed' : null)
    if (gap) { await qualificationLost(gap); return { host, gap } }
    try { user = await native.readEnablement(null) }
    catch (error) { if (!onQualificationLost) throw error; const gap = 'plugin-enablement-unconfirmed'; await qualificationLost(gap); return { host, gap } }
    const enabledGap = user?.user !== 'enabled' || user?.effective !== 'enabled' ? user?.user === 'disabled' || user?.effective === 'disabled' ? 'plugin-disabled' : 'plugin-enablement-unconfirmed' : null
    if (enabledGap) { await qualificationLost(enabledGap); return { host, gap: enabledGap } }
    if (onQualificationLost && !qualifiedHost) qualifiedHost = { hostRunId: host.hostRunId, hostStartedAt: host.hostStartedAt }
    return { host, gap: supportsHostProfile(host.profile) ? null : 'unsupported-host-version' }
  }
  async function projectSetting(context, cwd) {
    if (!context.projects.has(cwd)) context.projects.set(cwd, Promise.resolve().then(() => native.readEnablement(cwd)).then(setting => ({ setting }), () => ({ unavailable: true })))
    return context.projects.get(cwd)
  }
  function entryFor(id) {
    let entry = threads.get(id)
    if (!entry && threads.size < 256) { entry = { id, cursorKey: fingerprint(collectMaterials ? [installationId, id, 'material/1'] : [installationId, id]), generation: 0, captured: new Map(), captureThroughOffset: -1, deliveryThrough: { current: -1, history: -1 }, lastViewedAt: now(), view: gapView('not-observed', id), enrollmentPhase: 'unknown' }; threads.set(id, entry) }
    return entry
  }
  async function qualify(id, expected = null, context) {
    const metadata = await native.readThread(id)
    const thread = metadata?.thread
    const kind = classifyThread(thread, sessionsRoot, storageVersion)
    if (!thread || thread.id !== id || kind !== 'main' && !(collectMaterials && kind === 'fork')) return { gap: kind === 'fork' ? 'fork-provenance-unverified' : 'main-identity-unverified' }
    const host = context.host
    const project = await projectSetting(context, thread.cwd)
    if (project.unavailable) return { gap: 'plugin-enablement-unconfirmed', scope: 'task' }
    const enabled = project.setting
    if (enabled?.user !== 'enabled' || enabled?.effective !== 'enabled') return { gap: enabled?.user === 'disabled' || enabled?.effective === 'disabled' ? 'plugin-disabled' : 'plugin-enablement-unconfirmed', scope: enabled?.user !== 'enabled' ? 'source' : 'task' }
    const source = { ...(collectMaterials && nativeOriginVersion === 2 ? { nativeOriginVersion: 2 } : {}), path: thread.path, allowedRoot: nativeStorageRoot(thread.path, sessionsRoot, storageVersion), threadId: id, parentId: null, sourceKind: thread.source, originator: thread.originator, cwd: thread.cwd, forkedFromId: thread.forkedFromId ?? null, anchorTurnId: metadata.anchorTurnId ?? null, hostRunId: host.hostRunId, hostStartedAt: host.hostStartedAt, profile: host.profile }
    if (expected && fingerprint([source.path, source.cwd, source.sourceKind, source.originator, source.forkedFromId, source.hostRunId, source.hostStartedAt, source.profile]) !== fingerprint([expected.path, expected.cwd, expected.sourceKind, expected.originator, expected.forkedFromId, expected.hostRunId, expected.hostStartedAt, expected.profile])) return { gap: 'source-generation-changed', scope: source.hostRunId !== expected.hostRunId ? 'source' : 'task' }
    return { source }
  }

  async function prepare(entry, context, epoch) {
    const roundId = randomUUID()
    entry.qualifiedHostRunId = null
    try {
      const before = await qualify(entry.id, entry.reader?.source, context)
      if (before.gap) { await fail(entry, before.gap, before.scope); return null }
      entry.qualifiedHostRunId = before.source.hostRunId
      const originSource = collectMaterials && nativeOriginVersion === 2 ? await sourceFor(before.source, epoch) : null
      const origins = originSource ? { read: key => core.nativeOriginFence(originSource.key, key) } : null
      entry.fork = before.source.forkedFromId
      if (entry.blocked) {
        if (entry.blocked.scope === fingerprint(before.source)) { entry.view = gapView(entry.blocked.reason); return null }
        entry.blocked = null
      }
      const generation = entry.generation
      if (closed || epoch !== sourceEpoch) return null
      if (!entry.reader) {
        const cursorKey = fingerprint(collectMaterials ? [installationId, entry.id, before.source.forkedFromId ? 'fork-material/' + nativeOriginVersion : 'material/' + nativeOriginVersion] : [installationId, entry.id])
        if (entry.cursorKey !== cursorKey) { entry.cursorKey = cursorKey; entry.captured.clear(); entry.captureThroughOffset = -1; entry.deliveryThrough = { current: -1, history: -1 } }
        let cursor = null
        try { cursor = await loadCursor(join(dataDir, 'cursors', entry.cursorKey + '.json'), { fork: Boolean(before.source.forkedFromId) }) } catch (error) { await collectionOutcome(entry, 'failed', error, roundId); await stop(entry, error.code ?? 'cursor-unavailable'); await updateHealth(entry); return null }
        if (!cursor && collectMaterials && nativeOriginVersion === 2) {
          const oldKey = fingerprint([installationId, entry.id, before.source.forkedFromId ? 'fork-material/1' : 'material/1'])
          if (await loadCursor(join(dataDir, 'cursors', oldKey + '.json'), { fork: Boolean(before.source.forkedFromId) })) { await stop(entry, 'origin-upgrade-unverified'); return null }
        }
        entry.enrollmentPhase = cursor ? 'established' : entry.enrollmentPhase === 'established' ? 'established' : initialEnrollmentBaseline ? 'initial-unanchored' : 'unknown'
        if (cursor) { entry.captured = new Map(cursor.captures.map(item => [item.offset, item.capturedAt])); entry.captureThroughOffset = cursor.captureThroughOffset; entry.deliveryThrough = cursor.deliveryThrough ?? { current: cursor.captureThroughOffset, history: cursor.captureThroughOffset } }
        entry.reader = before.source.forkedFromId ? new ForkHistoryReader(before.source, cursor?.reader, { native, sessionsRoot, mode: 'material', origins }) : new BoundedReader(before.source, cursor?.reader, { mode: collectMaterials ? 'material' : 'metadata', origins })
      }
      if (closed) return null
      const previousCheckpoint = entry.reader.checkpoint(), observed = await entry.reader.pump({ anchorTurnId: before.source.anchorTurnId })
      if (observed.coldRead) { entry.view = { ...gapView('historical-execution-unconfirmed', entry.id), observation: { phase: 'unknown', status: 'unknown', coverage: 'catching-up', gap: 'historical-execution-unconfirmed', coldReadStage: observed.coldRead.phase, metadataOffset: observed.coldRead.offset, readBytes: observed.readBytes, decodedBytes: observed.decodedBytes } }; return null }
      if (observed.coverage === 'gap') {
        const code = observed.gap?.split(':')[0]
        if (observed.error || collectionFailures.has(code)) await collectionOutcome(entry, 'failed', observed.error ?? { code }, roundId)
        if (['start-predates-host', 'host-start-time-overlap'].includes(observed.gap)) entry.blocked = { scope: fingerprint(before.source), reason: observed.gap }
        await fail(entry, observed.gap); await updateHealth(entry); return null
      }
      return { source: before.source, previousCheckpoint, observed, epoch, generation, roundId }
    } catch (error) { if (closed || epoch !== sourceEpoch) return null; await collectionOutcome(entry, 'failed', error, roundId); await stop(entry, error.code?.startsWith('native_origin_') ? error.code : 'observation-unavailable').catch(() => {}); await updateHealth(entry); return null }
  }

  async function publish(entry, prepared) {
    try {
      const { observed, epoch, generation } = prepared
      const connection = await sourceFor(prepared.source, epoch)
      if (!connection) return await fail(entry, 'source-generation-changed', 'source')
      const fork = Boolean(prepared.source.forkedFromId), offset = event => fork ? event.captureOffset : event.recordStart
      const phase = observed.effectivePhase, lifecycleId = observed.observed.turnId ?? (fork ? prepared.source.anchorTurnId : null) ?? 'unobserved:' + prepared.source.hostRunId
      const status = phaseStatus(phase)
      const previousLifecycle = entry.lifecycleId, previousPhase = entry.view.observation.phase
      entry.lifecycleId = lifecycleId; entry.lifecycleVerified = observed.observed.anchored === true; entry.hostRunId = prepared.source.hostRunId; entry.sourceKey = connection.key
      if (previousLifecycle !== lifecycleId) entry.health = null
      const lifecycleEvents = collectMaterials ? observed.recentLifecycle : observed.recentLifecycle.filter(event => event.turnId === lifecycleId)
      const materials = collectMaterials ? observed.materials : []
      const historyLifecycle = observed.historyLifecycle ?? [], historyMaterials = observed.historyMaterials ?? [], boundaries = observed.historyBoundaries ?? []
      for (const event of [...lifecycleEvents, ...materials, ...historyLifecycle, ...historyMaterials, ...boundaries].sort((a, b) => offset(a) - offset(b))) if (!entry.captured.has(offset(event))) {
        const channel = event.history === true || event.data?.history === true ? 'history' : 'current'
        if (offset(event) <= (fork ? entry.deliveryThrough[channel] : entry.captureThroughOffset)) return await fail(entry, 'cursor-capture-history-unavailable')
        rememberCapture(entry, offset(event))
      }
      entry.captureThroughOffset = Math.max(entry.captureThroughOffset, fork ? observed.captureThroughOffset : observed.committedOffset - 1)
      if (fork) entry.deliveryThrough = observed.deliveryThrough
      const checkpoint = entry.reader.checkpoint(), target = join(dataDir, 'cursors', entry.cursorKey + '.json')
      // Persist first-capture identities before sending. A lost response replays
      // from the previous position with the same timestamps after recovery.
      await saveCursor(target, fork ? entry.reader.replayCheckpoint(prepared.previousCheckpoint) : { ...prepared.previousCheckpoint, fileId: prepared.previousCheckpoint.fileId ?? checkpoint.fileId, attachSize: prepared.previousCheckpoint.attachSize ?? checkpoint.attachSize, knownSize: checkpoint.knownSize }, entry.captured, entry.captureThroughOffset, { fork, deliveryThrough: entry.deliveryThrough })
      const batch = { ...(collectMaterials && nativeOriginVersion === 2 ? { nativeOrigins: observed.nativeOrigins ?? [] } : {}), tasks: [...(previousLifecycle && previousLifecycle !== lifecycleId ? [{ id: entry.id, sessionId: entry.id, lifecycleId: previousLifecycle, title: 'ChatGPT 主会话', status: ['completed', 'aborted'].includes(previousPhase) ? 'idle' : 'unknown', gaps: ['completed', 'aborted'].includes(previousPhase) ? [] : ['execution-superseded'] }] : []), { id: entry.id, sessionId: entry.id, lifecycleId, title: 'ChatGPT 主会话', status }], events: lifecycleEvents.map(event => ({ id: (collectMaterials ? 'material-lifecycle:' : 'lifecycle:') + offset(event), taskId: entry.id, taskLifecycleId: event.turnId, kind: 'status', capturedAt: entry.captured.get(offset(event)), occurredAt: event.at, text: event.type, evidence: nativeEvidence(entry, event, offset(event)), data: { status: event.type === 'task_started' && observed.unconfirmedStarts?.includes(event.turnId) ? 'unknown' : observed.historicalTerminal?.turnId === event.turnId ? 'idle' : !collectMaterials && phase === 'unknown' ? 'unknown' : phaseStatus(event.type === 'task_started' ? 'started' : event.type === 'task_complete' ? 'completed' : 'aborted'), nativeType: event.type, runId: event.turnId, ...(collectMaterials ? { sourceKind: 'chatgpt-native', sourceForm: 'event_msg', nativeRecordId: event.nativeRecordId, ...(nativeOriginVersion === 2 ? { nativeGenerationKey: event.nativeGenerationKey, nativeRecordStart: event.nativeRecordStart, nativeRecordEnd: event.nativeRecordEnd } : {}), ...(event.turnTrigger ? { turnTrigger: event.turnTrigger } : {}) } : {}) } })) }
      if (collectMaterials) {
        batch.events.push(...materials.map(event => ({ id: 'material:' + offset(event), taskId: entry.id, taskLifecycleId: event.turnId, kind: event.kind, capturedAt: entry.captured.get(offset(event)), occurredAt: event.at, text: event.text, data: event.data, evidence: nativeEvidence(entry, event, offset(event)) })))
        const retainedEvent = (event, prefix, data) => ({ id: prefix + offset(event), taskId: entry.id, taskLifecycleId: lifecycleId, kind: event.kind ?? 'status', capturedAt: entry.captured.get(offset(event)), occurredAt: event.at, text: event.text ?? event.type, evidence: nativeEvidence(entry, event, offset(event)), data: { ...data, runId: lifecycleId, history: true, inherited: true } })
        batch.events.push(...historyLifecycle.map(event => retainedEvent(event, 'material-lifecycle:', { sourceKind: 'chatgpt-native', sourceForm: 'event_msg', nativeType: event.type, originRunId: event.originRunId ?? event.turnId, sourceThreadId: event.sourceThreadId, nativeRecordId: event.nativeRecordId, ...(nativeOriginVersion === 2 ? { nativeGenerationKey: event.nativeGenerationKey, nativeRecordStart: event.nativeRecordStart, nativeRecordEnd: event.nativeRecordEnd } : {}), forkThreadId: entry.id, recordOrdinal: event.recordOrdinal, ...(event.historyGeneration ? { historyGeneration: event.historyGeneration } : {}), ...(event.turnTrigger ? { turnTrigger: event.turnTrigger } : {}) })), ...historyMaterials.map(event => retainedEvent(event, 'material:', { ...event.data, originRunId: event.data.originRunId ?? event.turnId })), ...boundaries.map(event => retainedEvent(event, 'material-boundary:', event.data)))
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
        for (const turnId of observed.unconfirmedStarts ?? []) descriptions.set(turnId, { ...descriptions.get(turnId), id: entry.id, sessionId: entry.id, lifecycleId: turnId, title: 'ChatGPT 主会话', status: 'unknown', gaps: [...new Set([...(descriptions.get(turnId)?.gaps ?? []), 'historical-execution-unconfirmed'])] })
        for (const event of [...known.flatMap(task => task.events), ...batch.events]) if (event.taskLifecycleId !== lifecycleId && !event.data?.history && !event.data?.inherited && event.data?.sourceKind === 'chatgpt-native' && ['task_complete', 'turn_aborted'].includes(event.data?.nativeType)) descriptions.set(event.taskLifecycleId, { id: entry.id, sessionId: entry.id, lifecycleId: event.taskLifecycleId, title: 'ChatGPT 主会话', status: 'idle' })
        batch.tasks = [...descriptions.values()]
        batch.hierarchyContext = materialContext(before, connection.key, batch, fork ? { taskId: entry.id, complete: observed.forkProvenance.complete, receiptId: observed.forkProvenance.receiptId, generation: observed.forkProvenance.generation } : null)
      }
      const accepted = await core.ingest(connection.key, batch)
      if (closed || epoch !== sourceEpoch || generation !== entry.generation) return entry.view
      entry.reader?.acknowledgeOrigins?.(accepted.nativeOrigins)
      await saveCursor(target, checkpoint, entry.captured, entry.captureThroughOffset, { fork, deliveryThrough: entry.deliveryThrough })
      await collectionOutcome(entry, observed.forkProvenance?.error ? 'failed' : 'succeeded', observed.forkProvenance?.error ?? null, prepared.roundId)
      const snapshot = await core.snapshot()
      if (closed) return gapView('adapter-closed')
      if (epoch !== sourceEpoch || generation !== entry.generation || source?.key !== connection.key) return entry.view
      const task = snapshot.tasks.find(task => task.sourceKey === connection.key && task.id === entry.id && task.lifecycleId === lifecycleId)
      if (!task || !collectMaterials && task.analysis.state !== 'observation-only') { await stop(entry, 'core-capability-unconfirmed'); return entry.view }
      entry.view = { identity: { threadId: entry.id, cursorKey: entry.cursorKey }, signal: 'gray', summary: phase === 'started' ? '已观察到执行开始 · 尚无分析结论' : phase === 'completed' ? '本轮已结束 · 尚无分析结论' : phase === 'aborted' ? '本轮已中断 · 尚无分析结论' : '当前执行尚未确认', observation: { phase, status: task.status, coverage: observed.coverage, ...(fork ? { currentCoverage: observed.currentCoverage } : {}), gap: observed.gap, readBytes: observed.readBytes, ...(nativeOriginVersion === 2 ? { decodedBytes: observed.decodedBytes, recordRevalidating: observed.recordRevalidating === true } : {}), observedAt: observed.observed.at }, analysis: { state: 'not-connected', summary: '六项分析尚未接入' } }
      if (collectMaterials) entry.view = materialView(entry.view, task, { snapshot })
      if (fork) {
        entry.view.forkProvenance = observed.forkProvenance
        entry.view.inheritance = { state: observed.forkProvenance.complete ? 'complete' : 'pending', gaps: [...new Set([...(task.instructionContext?.gaps ?? []), ...(observed.forkProvenance.gap ? [observed.forkProvenance.gap] : [])])] }
        if (entry.view.inheritance.state === 'pending' && !['red', 'yellow'].includes(entry.view.signal)) { entry.view.signal = 'gray'; entry.view.summary = task.currentRequest ? '当前执行已接入，继承要求待补齐' : entry.view.summary }
      }
      await updateHealth(entry, snapshot)
      return entry.view
    } catch (error) { if (closed || prepared.epoch !== sourceEpoch || prepared.generation !== entry.generation) return entry.view; if (error.code === 'native_origin_revision_conflict') { await stop(entry, 'native-origin-revalidating', false); return entry.view } await collectionOutcome(entry, 'failed', error, prepared.roundId); await stop(entry, error.code?.startsWith('native_origin_') ? error.code : 'observation-unavailable').catch(() => {}); await updateHealth(entry); return entry.view }
  }

  function rememberCapture(entry, offset) { const at = now(); if (!Number.isSafeInteger(at) || at < 0) throw new Error('invalid capture clock'); entry.captured.set(offset, at); if (entry.captured.size > 16) entry.captured.delete(entry.captured.keys().next().value); return at }
  function enqueue(action) {
    if (closed || queuedBatches >= 64) return null
    queuedBatches++
    const pending = queue.then(() => closed ? undefined : action())
    queue = pending.catch(() => {})
    return track(pending.finally(() => { queuedBatches-- }))
  }
  function decorate(view) {
    const retained = healthOutbox?.diagnostics()
    const directory = discovery.snapshot(), forks = new Set([...discovery.candidates().filter(id => discovery.kind(id) === 'fork'), ...[...threads.values()].filter(entry => entry.fork).map(entry => entry.id)])
    const verified = [...forks].filter(id => threads.get(id)?.view.forkProvenance?.complete && threads.get(id).view.observation.coverage === 'caught-up').length
    return { ...view, ...(availabilityFailure ? { observation: { ...view.observation, failureStage: availabilityFailure.stage } } : {}), ...(delivery ? { notifications: { permission: delivery.authorization(), errorCode: notificationError, scope: { sourceKey: view.execution?.sourceKey ?? source?.key ?? null, lifecycleId: view.execution?.lifecycleId ?? null, executionKey: view.execution?.key ?? null } } } : {}), ...(healthOutbox ? { health: { ...combineHealth(view.health, sourceHealth, outboxError, availabilityFailure?.health), historyGap: retained.rejected > 0 || retained.expired > 0 } } : {}), discovery: { ...directory, gaps: [...new Set([...directory.gaps, ...(forks.size > verified ? ['fork-coverage-pending'] : [])])], forkCoverage: { known: forks.size, verified, pending: forks.size - verified }, tracked: threads.size, connected: source ? 1 : 0, currentIncluded: Boolean(view.identity.threadId && threads.has(view.identity.threadId)) } }
  }

  async function collect(requested = [], discover = false) {
    let epoch = sourceEpoch, failureStage = 'host-verification'
    try {
      const { host, gap } = await qualification()
      if (closed || epoch !== sourceEpoch) return
      if (gap) { await stopSource(gap); return }
      failureStage = 'core-connection'
      const supported = await capability()
      if (closed || epoch !== sourceEpoch) return
      if (!supported.available) { await stopSource('core-capability-unavailable'); return }
      if (coreInstanceId && supported.instanceId && coreInstanceId !== supported.instanceId) {
        try { await stopSource('core-generation-changed') } catch (error) { if (error.status !== 400) throw error }
        if (closed) return
        epoch = sourceEpoch
      }
      coreInstanceId = supported.instanceId ?? null
      failureStage = 'source-registration'
      if (collectMaterials) await sourceFor(host, epoch)
      failureStage = 'material-collection'
      if (discover) {
        const roundId = randomUUID(), result = await discovery.refresh()
        if (result.state === 'unavailable') await collectionOutcome(null, 'failed', result.error ?? { code: 'metadata-unavailable', stage: 'metadata' }, roundId)
        else if (result.state === 'ready') await collectionOutcome(null, 'succeeded', null, roundId)
        await updateHealth()
      }
      if (closed || epoch !== sourceEpoch) return
      for (const [id, entry] of threads) {
        const kind = discovery.kind(id)
        if (kind && kind !== 'main' && !(collectMaterials && kind === 'fork')) await stop(entry, kind === 'fork' ? 'fork-provenance-unverified' : 'directory-object-excluded')
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
      const { host: afterHost, gap: afterGap } = await qualification(host)
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
      failureStage = 'source-heartbeat'
      if (source && !closed && epoch === sourceEpoch) await core.heartbeat(source.key)
      if (discover && source && !closed && epoch === sourceEpoch) refreshNotifications({ sourceKey: source.key, hostRunId: source.hostRunId, appRunning: true, pluginEnabled: true })
      availabilityFailure = null
    } catch (error) {
      if (closed || epoch !== sourceEpoch) return
      const info = failureErrorInfo({ code: error.code ?? error.cause?.code ?? 'collection_failed', codeHash: error.codeHash, stage: error.stage ?? (['core-connection', 'source-registration', 'source-heartbeat'].includes(failureStage) ? 'core' : 'collection'), status: error.status, details: error.details })
      availabilityFailure = { stage: failureStage, health: { state: 'failed', failures: [{ domain: 'collection', error: info }] } }
      await collectionOutcome(null, 'failed', info, randomUUID()); await updateHealth(); await stopSource('observation-unavailable').catch(() => {})
    }
  }
  async function historyProof(entry, expected = null) {
    const { host, gap } = await qualification()
    if (gap) return { gap, scope: 'source' }
    return qualify(entry.id, expected, { host, projects: new Map() })
  }
  async function observedView(entry, selection) {
    if (!collectMaterials || !source) return entry.view
    const owner = source, epoch = sourceEpoch, generation = entry.generation
    const retainedOnly = !entry.reader || entry.view.identity.threadId !== entry.id || Boolean(entry.view.observation?.coldReadStage)
    let proof
    if (retainedOnly) {
      try { proof = await historyProof(entry) }
      catch (error) { await collectionOutcome(entry, 'failed', error, randomUUID()); await updateHealth(entry); return entry.view }
      if (proof.gap) return fail(entry, proof.gap, proof.scope)
      if (proof.source.hostRunId !== owner.hostRunId) return gapView('source-generation-changed')
    }
    const snapshot = await core.snapshot()
    if (retainedOnly) {
      let after
      try { after = await historyProof(entry, proof.source) }
      catch (error) { await collectionOutcome(entry, 'failed', error, randomUUID()); await updateHealth(entry); return entry.view }
      if (after.gap) return fail(entry, after.gap, after.scope)
    }
    if (closed || source !== owner || epoch !== sourceEpoch || generation !== entry.generation) return gapView('observation-superseded')
    const ownSources = new Set(snapshot.sources.filter(value => value.descriptor.registrationId === 'chatgpt:' + installationId && value.descriptor.policy === 'chatgpt-main/1').map(value => value.key))
    const records = snapshot.tasks.filter(task => ownSources.has(task.sourceKey) && task.id === entry.id).flatMap(task => {
      const events = task.events.filter(event => event.taskId === task.id && event.taskLifecycleId === task.lifecycleId && event.data?.runId === task.lifecycleId && event.data?.sourceKind === 'chatgpt-native' && !event.data.history && !event.data.inherited && event.capturedAt > now() - 86400000)
      if (!events.length) return []
      const start = events.find(event => event.data.nativeType === 'task_started'), end = events.findLast(event => ['task_complete', 'turn_aborted'].includes(event.data.nativeType))
      return [{ task, order: start?.occurredAt ?? Math.min(...events.map(event => event.capturedAt)), item: { key: task.key, sourceKey: task.sourceKey, lifecycleId: task.lifecycleId, status: end ? end.data.nativeType === 'turn_aborted' ? 'aborted' : 'ended' : task.status, startedAt: start?.occurredAt ?? null, observedAt: Math.min(...events.map(event => event.capturedAt)), analysisState: task.analysis.state } }]
    }).sort((a, b) => b.order - a.order || a.task.key.localeCompare(b.task.key))
    const current = records.find(value => value.task.sourceKey === owner.key && value.task.lifecycleId === entry.lifecycleId) ?? records[0]
    const selected = selection.executionKey ? records.find(value => value.task.key === selection.executionKey) : current
    const cursorIndex = selection.historyCursor ? records.findIndex(value => value.task.key === selection.historyCursor) : -1
    const page = records.slice(cursorIndex + 1, cursorIndex + 21), hasMore = records.length > cursorIndex + 21
    const currentVerified = Boolean(current && !retainedOnly && !entry.view.observation?.recordRevalidating)
    const history = { executions: page.map(value => value.item), currentKey: current?.task.key ?? null, selectedKey: selected?.task.key ?? null,
      currentStatus: currentVerified ? current.task.status : 'unknown', currentSignal: currentVerified && current.task.analysis.state !== 'paused' && !snapshot.monitoring?.paused ? current.task.gs?.light ?? 'gray' : 'gray',
      hasMore, cursor: cursorIndex >= 0 ? selection.historyCursor : null, nextCursor: hasMore ? page.at(-1).task.key : null, historyGap: Boolean(selection.historyCursor && cursorIndex < 0), currentGap: retainedOnly ? entry.view.observation?.gap ?? 'current-turn-not-observed' : null, retentionMs: 86400000 }
    if (!selected) return { ...(entry.view.observation?.coldReadStage ? entry.view : gapView(selection.executionKey ? 'history-unavailable' : entry.view.observation?.gap ?? 'current-turn-not-observed', entry.id)), health: entry.health, history }
    const historical = retainedOnly || selected !== current || ['ended', 'aborted'].includes(selected.item.status)
    const view = materialView({ ...entry.view, ...(historical ? { currentFacts: undefined, inheritance: undefined } : {}), identity: { threadId: entry.id, cursorKey: entry.cursorKey } }, selected.task, { historical, snapshot })
    if (selected === current) view.health = combineHealth(view.health, entry.health)
    if (historical) {
      view.signal = 'gray'; view.summary = retainedOnly ? '当前执行尚未确认 · 显示最近检查记录' : '历史执行 · 仅表示当时的检查记录'
      view.observation = { phase: selected.item.status === 'ended' ? 'completed' : selected.item.status === 'aborted' ? 'aborted' : 'unknown', status: selected.task.status, coverage: 'retained-history', gap: history.currentGap ?? (selected.task.currentRequest ? null : 'historical-user-request-unverified'), observedAt: selected.item.observedAt }
    }
    return { ...view, history, execution: { ...selected.item, request: selected.task.currentRequest.slice(0, 240), historical } }
  }
  async function read(meta, selection = {}) {
    const id = requestIdentity(meta)
    if (!id) return decorate(gapView('request-identity-unverified'))
    if ((selection.executionKey || selection.historyCursor) && selection.expectedThreadId !== id) return decorate(gapView('history-scope-unverified'))
    if (closed) return decorate(gapView('adapter-closed'))
    const entry = entryFor(id)
    if (!entry) return decorate(gapView('observation-capacity-reached'))
    entry.lastViewedAt = now()
    if (queuedBatches) {
      const { gap } = await qualification().catch(() => ({ gap: 'plugin-enablement-unconfirmed' }))
      if (closed) return decorate(gapView('adapter-closed'))
      if (gap) { await stopSource(gap); return decorate(entry.view) }
    }
    const pending = enqueue(async () => { await collect([id]); return closed ? gapView('adapter-closed') : observedView(entry, selection) })
    if (!pending) return decorate(gapView(closed ? 'adapter-closed' : 'collection-queue-capacity'))
    const view = await pending
    return decorate(closed ? gapView('adapter-closed') : view)
  }
  async function control(meta, change) {
    const id = requestIdentity(meta)
    if (!id || id !== change.expectedThreadId) throw new Error('当前聊天身份已变化，请重新核对')
    const pending = enqueue(async () => {
      await collect([id])
      const entry = threads.get(id)
      if (closed || !source || !entry?.reader || entry.hostRunId !== source.hostRunId || entry.view.identity.threadId !== id) throw new Error('当前聊天的监控资格尚未确认')
      if (!(await core.health()).features?.includes('conversation-monitoring/1')) throw new Error('共享核心尚不支持本聊天暂停')
      await core.setConversationMonitoring({ registrationId: 'chatgpt:' + installationId, taskId: id, paused: change.paused, revision: change.revision })
      await collect([id])
      return decorate(closed ? gapView('adapter-closed') : entry.view)
    })
    if (!pending) throw new Error('会话检查正在收尾或队列已满')
    return pending
  }
  async function requestNotifications(meta, change) {
    const id = requestIdentity(meta)
    if (!delivery || !id || id !== change.expectedThreadId) throw new Error('当前聊天身份或通知组件尚未确认')
    const pending = enqueue(async () => {
      await collect([id])
      const entry = threads.get(id), proof = entry ? await historyProof(entry) : null
      if (closed || !source || !proof?.source || proof.source.hostRunId !== source.hostRunId) throw new Error('当前聊天的通知资格尚未确认')
      track(delivery.requestAuthorization())
      return decorate(await observedView(entry, {}))
    })
    if (!pending) throw new Error('会话检查正在收尾或队列已满')
    return pending
  }
  async function notificationRecords(meta, selection) {
    const id = requestIdentity(meta)
    if (!delivery || !id || id !== selection.expectedThreadId) throw new Error('当前聊天身份或通知组件尚未确认')
    const pending = enqueue(async () => {
      const entry = threads.get(id), owner = source, epoch = sourceEpoch
      if (!entry || !owner) throw new Error('当前聊天尚未接入')
      const before = await historyProof(entry)
      if (before.gap || before.source.hostRunId !== owner.hostRunId) throw new Error('当前聊天的通知记录资格尚未确认')
      const view = await observedView(entry, selection)
      if (selection.executionKey && view.execution?.key !== selection.executionKey) throw new Error('历史执行已过期或不可用')
      const sourceKey = view.execution?.sourceKey ?? owner.key, lifecycleId = view.execution?.lifecycleId ?? null, values = new Map()
      let cursor = 0, historyGap = false, complete = false
      for (let page = 0; page < 11; page++) {
        const result = await core.nativeNotificationRecords({ sourceKey, afterSequence: cursor, limit: 100 })
        historyGap ||= result.historyGap
        for (const record of result.records) if (record.mainId === null || record.mainId === id && (record.lifecycleId === lifecycleId || record.kind === 'monitoring-failure' && record.lifecycleId === null)) values.set(record.id, record)
        if (!result.hasMore) { complete = true; break }
        cursor = result.nextSequence
      }
      const after = await historyProof(entry, before.source)
      if (closed || source !== owner || sourceEpoch !== epoch || after.gap) throw new Error('通知记录来源已变化，请重新核对')
      const records = [...values.values()].filter(record => selection.beforeSequence === undefined || record.sequence < selection.beforeSequence).sort((a, b) => b.sequence - a.sequence)
      return { identity: { threadId: id }, sourceKey, lifecycleId, executionKey: view.execution?.key ?? null, generatedAt: now(), records: records.slice(0, 20), hasMore: records.length > 20, nextBeforeSequence: records.length > 20 ? records[19].sequence : null, historyGap: historyGap || !complete }
    })
    if (!pending) throw new Error('会话检查正在收尾或队列已满')
    return pending
  }
  async function refresh() { if (closed) return; if (refreshPending) return refreshPending; const pending = enqueue(() => collect([], true)); if (!pending) return; refreshPending = pending.finally(() => { refreshPending = null }); return refreshPending }
  let panel
  async function qualifySettings() {
    if (closed) throw new Error('观察实例已关闭')
    const qualified = await qualification(), { gap } = qualified
    if (closed || gap && gap !== 'unsupported-host-version') throw new Error('本安装设置资格未确认：' + (gap ?? 'adapter-closed'))
    return qualified
  }
  function settingsRead(requested, action) {
    const signal = AbortSignal.any([settingsLifetime.signal, ...(requested ? [requested] : [])])
    return track((async () => {
      signal.throwIfAborted()
      await qualifySettings()
      signal.throwIfAborted()
      const result = await action(signal)
      const qualified = await qualifySettings()
      signal.throwIfAborted()
      return { result, qualified }
    })())
  }
  const modelSettings = collectMaterials && typeof core.installationAnalysisConfig === 'function' ? {
    async read() { await qualifySettings(); await core.health(); await qualifySettings(); return core.installationAnalysisConfig('chatgpt:' + installationId) },
    async save({ config, revision }) { await qualifySettings(); await core.health(); await qualifySettings(); return core.configureInstallationAnalysis('chatgpt:' + installationId, config, revision) },
    async directory({ baseUrl }, { signal } = {}) { return (await settingsRead(signal, signal => readModelDirectory(baseUrl, { signal }))).result },
    async diagnostics({ signal } = {}) {
      const { result, qualified } = await settingsRead(signal, async signal => {
        await core.health(signal)
        await qualifySettings()
        signal.throwIfAborted()
        const [setting, snapshot] = await Promise.all([core.installationAnalysisConfig('chatgpt:' + installationId, signal), core.snapshot(signal)])
        return { setting, snapshot }
      })
      const failures = combineHealth(sourceHealth, availabilityFailure?.health, outboxError, ...[...threads.values()].map(entry => entry.health)).failures.filter(failure => failure.domain === 'collection')
      const collectionError = qualified.gap ? { stage: 'host-verification', code: qualified.gap } : failures[0]?.error ?? null
      return projectInstallationDiagnostics(result.setting, result.snapshot, { collectionError, collectionState: qualified.gap ? 'unconfirmed' : null })
    },
  } : undefined
  const installationNotifications = collectMaterials ? {
    async read({ signal } = {}) {
      const { result } = await settingsRead(signal, async signal => {
        await core.health(signal); await qualifySettings(); signal.throwIfAborted()
        return core.installationNotificationPreferences('chatgpt:' + installationId, signal)
      })
      return { ...result, permission: delivery?.authorization() ?? null }
    },
    async save({ kind, enabled }, { signal } = {}) {
      const { result } = await settingsRead(signal, async signal => {
        await core.health(signal); await qualifySettings(); signal.throwIfAborted()
        return core.setInstallationNotificationPreference('chatgpt:' + installationId, kind, enabled, signal)
      })
      if (delivery) track(delivery.preferencesChanged(kind)).catch(() => { notificationError = 'native_unavailable' })
      return result
    },
    ...(delivery ? { async requestPermission({ signal } = {}) {
      const { result } = await settingsRead(signal, () => delivery.requestAuthorization())
      return { registrationId: 'chatgpt:' + installationId, permission: result }
    } } : {}),
  } : undefined
  try { panel = await startPanelServer({ read, modelSettings, installationNotifications, ...(collectMaterials ? { control } : {}), ...(delivery ? { requestNotifications, notificationRecords } : {}), runtimeStatus, displayName, token, port }) }
  catch (error) { await delivery?.stop(); await healthOutbox?.close(); throw error }
  if (onQualificationLost) await qualification()
  const schedule = () => { if (!closed && pollMs) timer = setTimeout(async () => { timer = null; await refresh(); schedule() }, pollMs) }
  if (pollMs && typeof native.listThreads === 'function') void refresh()
  schedule()
  function close(reason = 'adapter-closed') {
    return closing ??= (async () => {
      closed = true; clearTimeout(timer); timer = null
      settingsLifetime.abort(new Error('观察实例已关闭'))
      const cancelling = onQualificationLost ? core.cancelPending?.() : null
      const notificationClosing = delivery?.stop().then(result => {
        if (result.errorCode || result.remainingPending !== 0) throw Object.assign(new Error('Native notification cleanup incomplete'), { code: 'notification_cleanup_failed', errorCode: result.errorCode, remainingPending: result.remainingPending })
        return result
      }), directoryClosing = discovery.close()
      const sourceClosing = onQualificationLost ? stopSource(reason) : null
      sourceClosing?.catch(() => {})
      const cleanup = await Promise.allSettled([cancelling, notificationClosing, directoryClosing, sourceClosing, Promise.resolve().then(() => native.close?.()), panel.close()])
      await Promise.allSettled([...operations])
      if (!onQualificationLost) await stopSource(reason).catch(() => {})
      threads.clear()
      try { await healthOutbox?.close() } catch (reason) { cleanup.push({ status: 'rejected', reason }) }
      const failures = cleanup.filter(value => value.status === 'rejected').map(value => value.reason)
      if (qualificationError) failures.unshift(qualificationError)
      if (failures.length) throw failures.length === 1 ? failures[0] : new AggregateError(failures, 'Observation cleanup failed')
    })()
  }
  return { url: panel.url, read, refresh, discovery: () => decorate(gapView('not-observed')).discovery, diagnostics: () => ({ closed, trackedThreads: threads.size, openReaders: [...threads.values()].reduce((total, entry) => total + (entry.reader?.diagnostics?.().readers ?? Number(Boolean(entry.reader?.handle))), 0), pendingOperations: operations.size, refreshTimers: timer === null ? 0 : 1, queuedBatches, ...(healthOutbox ? { healthOutbox: healthOutbox.diagnostics() } : {}), ...discovery.diagnostics(), ...panel.diagnostics() }), close }
}
