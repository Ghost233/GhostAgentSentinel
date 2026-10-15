import { createHash, randomUUID } from 'node:crypto'
import { sanitizeObservation } from '../src/privacy.mjs'
import { extractVerificationEvidence } from './verification-evidence.mjs'

const identityStore = Symbol.for('ghost-agent-sentinel.dsh-host-identities.v1')
const hostIdentities = globalThis[identityStore] ??= new WeakMap()
const DAY = 24 * 60 * 60 * 1000

// Only scalar identity/watermarks survive adapter replacement. The Host owns Agents.
export function hostIdentity(root) {
  let identity = hostIdentities.get(root)
  if (!identity) {
    identity = { hostRunId: randomUUID(), agents: new WeakMap(), sequence: 0, hierarchyRevision: 0, hierarchyContent: null }
    hostIdentities.set(root, identity)
  }
  return identity
}

const excerpt = value => String(value ?? '').slice(0, 240)
const contentText = content => (Array.isArray(content) ? content : []).filter(block => block?.type === 'text').map(block => block.text ?? '').join('\n')
const statusOf = value => ['running', 'idle', 'disposed'].includes(value) ? value : 'unknown'
const callKey = (lifecycle, callId) => `${lifecycle.id}/${String(callId)}`
const weakKey = value => (typeof value === 'object' && value !== null) || typeof value === 'symbol'
const inputOrigin = (source, delegated) => source.form === 'notice' || ['runtime-context', 'subagent-settled'].includes(source.kind) ? 'runtime-notice' : source.form === 'relay' || source.kind === 'agent-message' || source.senderSessionId ? 'agent-relay' : source.kind === 'user' && delegated ? 'delegated' : 'unknown'
const taskSnapshot = ({ observedAt, acknowledgedMetadata, ...row }) => row

export function createObservation({ identity, now = Date.now, maxEvents = 500, changed = () => {}, readTitle = () => undefined, hierarchy = false, readCatalog = () => undefined }) {
  const rows = new Map(), sessions = new WeakMap(), toolTokens = new Map(), toolEvents = new Map(), redactedCalls = new Map(), gaps = new Set()
  // Weak tokens preserve a first-capture stamp if the native Host still owns a call.
  // They neither own native calls nor retain their arguments/results.
  let stamps = new WeakMap(), approvalEvents = new WeakSet(), referenceTimes = new WeakMap(), events = [], closed = false
  const historyGaps = new Map()
  const claimedInputs = new Map(), observedSessions = new Map(), externalRows = new Map(), pendingExternalRuns = new Map()
  const catalog = new Map(), states = new Map(), inputs = new Map(), hierarchyEvidence = new Map(), checkedCatalogs = new Map(), nativeRuns = new Map(), hierarchyRows = new Map()
  const waits = new Map(), waitGaps = new Map()
  const waitGap = (row, reason, at = now()) => {
    const previous = waitGaps.get(row.lifecycleId)
    if (previous) previous.reasons = [...new Set([...previous.reasons, reason])].slice(0, 8)
    else waitGaps.set(row.lifecycleId, { sessionId: row.sessionId, reasons: [reason], at })
    bound(waitGaps, '父等待缺口超过采集预算；覆盖不完整')
  }
  const waitRequest = lifecycleId => (inputs.get(lifecycleId) ?? []).filter(entry => entry.phase === 'consumed' && hierarchyEvidence.get(entry.inputRef.eventId)?.data.inputOrigin !== 'runtime-notice').at(-1)
  const reference = event => ({ taskId: event.taskId, lifecycleId: event.taskLifecycleId, eventId: event.id })
  const sessionRow = id => [...rows.values(), ...hierarchyRows.values()].filter(row => row.sessionId === id).sort((left, right) => Number(right.status === 'running') - Number(left.status === 'running') || (right.observedAt ?? 0) - (left.observedAt ?? 0))[0]
  const retainedRef = reference => reference && (hierarchyEvidence.has(reference.eventId) || reference.retainedAt > now() - DAY)
  function collectEvidence() {
    const needed = new Set(events.map(event => event.id))
    const keep = ref => { if (ref?.eventId) needed.add(ref.eventId) }
    for (const state of states.values()) { keep(state.stateRef); keep(state.runRef) }
    for (const run of nativeRuns.values()) keep(run.runRef)
    for (const run of pendingExternalRuns.values()) keep(run.runRef)
    for (const relation of catalog.values()) keep(relation.evidenceRef)
    for (const ref of claimedInputs.values()) keep(ref)
    for (const entries of inputs.values()) for (const entry of entries) { keep(entry.inputRef); keep(entry.claimRef); for (const ref of entry.ancestorLimitRefs ?? []) keep(ref) }
    for (const wait of waits.values()) for (const ref of [wait.joinRef, wait.auditRef, wait.callerRef, wait.callerEndRef, wait.requestRef]) keep(ref)
    for (const id of hierarchyEvidence.keys()) if (!needed.has(id)) hierarchyEvidence.delete(id)
  }
  function retain(event) { hierarchyEvidence.set(event.id, event); const ref = reference(event); referenceTimes.set(ref, event.capturedAt); return ref }
  function finishEvidence() {
    // Owner pointers must already cite the new proof before old references can be collected.
    if (hierarchyEvidence.size > maxEvents) { collectEvidence(); bound(hierarchyEvidence, '层级依据索引超过采集预算；覆盖不完整') }
  }
  function externalRow(relation) {
    let row = externalRows.get(relation.childSessionId)
    if (!row) { row = { id: relation.childSessionId, lifecycleId: randomUUID(), sessionId: null, title: excerpt(relation.childSessionId), status: 'unknown', gaps: ['外部执行内部材料不可观察'], observedAt: now() }; externalRows.set(relation.childSessionId, row); bound(externalRows, '外部执行索引超过采集预算；覆盖不完整') }
    const pending = pendingExternalRuns.get(relation.childSessionId)
    if (pending && hierarchyEvidence.has(pending.runRef.eventId) && sessionRow(relation.parentSessionId)?.id === pending.runRef.taskId) {
      row.status = pending.state; states.set(row.lifecycleId, { runId: pending.runId, runRef: pending.runRef }); nativeRuns.set(row.id, { runId: pending.runId, runRef: pending.runRef, lifecycleId: row.lifecycleId }); pendingExternalRuns.delete(row.id)
    }
    return row
  }
  function gap(reason) {
    gaps.add(excerpt(sanitizeObservation(reason).text))
    while (gaps.size > maxEvents) gaps.delete(gaps.values().next().value)
  }
  function dropRow(id) {
    const row = rows.get(id)
    if (hierarchy && row?.observedAt > now() - DAY) { hierarchyRows.set(id, { ...taskSnapshot(row), observedAt: row.observedAt, acknowledgedMetadata: row.acknowledgedMetadata }); bound(hierarchyRows, '历史执行节点超过采集预算；覆盖不完整') }
    rows.delete(id)
    for (const [key, value] of toolEvents) if (value.lifecycleId === id) toolEvents.delete(key)
    for (const [key, value] of redactedCalls) if (value.lifecycleId === id) redactedCalls.delete(key)
    for (const [key, value] of toolTokens) if (value.lifecycleId === id) toolTokens.delete(key)
    if (!hierarchy) { states.delete(id); inputs.delete(id) }
  }
  function bound(map, reason) {
    while (map.size > maxEvents) {
      if (map === catalog) checkedCatalogs.delete(map.values().next().value.parentSessionId)
      const victim = map === hierarchyRows ? [...map].find(([, row]) => row.status === 'unknown' && !states.has(row.lifecycleId) && !inputs.has(row.lifecycleId) && ![...catalog.values()].some(item => item.parentSessionId === row.sessionId)) ?? [...map].find(([, row]) => row.status !== 'running') ?? map.entries().next().value : map.entries().next().value
      map.delete(victim[0])
      if (map === hierarchyRows) {
        const row = victim[1]
        states.delete(row.lifecycleId); inputs.delete(row.lifecycleId); checkedCatalogs.delete(row.sessionId)
        if (observedSessions.get(row.sessionId)?.lifecycleId === row.lifecycleId) observedSessions.delete(row.sessionId)
        if (nativeRuns.get(row.sessionId)?.lifecycleId === row.lifecycleId) nativeRuns.delete(row.sessionId)
        historyGaps.set(row.sessionId ?? row.id, { reason, at: now() }); bound(historyGaps, '历史分支缺口超过记录预算；覆盖不完整')
      }
      gap(reason)
    }
  }
  function sweep() {
    const cutoff = now() - DAY, before = events.length
    events = events.filter(event => event.capturedAt > cutoff)
    for (const [id, event] of hierarchyEvidence) if (event.capturedAt <= cutoff) hierarchyEvidence.delete(id)
    for (const [key, wait] of waits) if (wait.startedAt <= cutoff || !rows.has(wait.lifecycleId) && !hierarchyRows.has(wait.lifecycleId)) waits.delete(key)
    for (const [id, value] of waitGaps) if (value.at <= cutoff || !rows.has(id) && !hierarchyRows.has(id)) waitGaps.delete(id)
    for (const [id, value] of historyGaps) if (value.at <= cutoff) historyGaps.delete(id)
    for (const [id, row] of externalRows) if (row.observedAt <= cutoff) { externalRows.delete(id); states.delete(row.lifecycleId); nativeRuns.delete(id) }
    for (const [id, item] of pendingExternalRuns) if (!hierarchyEvidence.has(item.runRef.eventId)) pendingExternalRuns.delete(id)
    for (const [id, item] of claimedInputs) if (!hierarchyEvidence.has(item.eventId)) claimedInputs.delete(id)
    for (const [id, row] of hierarchyRows) if (row.observedAt <= cutoff) { hierarchyRows.delete(id); states.delete(id); inputs.delete(id); checkedCatalogs.delete(row.sessionId); nativeRuns.delete(row.sessionId) }
    for (const id of states.keys()) if (!rows.has(id) && !hierarchyRows.has(id) && ![...externalRows.values()].some(row => row.lifecycleId === id)) states.delete(id)
    for (const id of inputs.keys()) if (!rows.has(id) && !hierarchyRows.has(id)) inputs.delete(id)
    const expiredRef = ref => ref && (referenceTimes.get(ref) ?? ref.retainedAt ?? Infinity) <= cutoff
    for (const [id, entries] of inputs) {
      const retained = entries.filter(entry => !expiredRef(entry.inputRef))
      for (const entry of retained) {
        if (expiredRef(entry.claimRef)) delete entry.claimRef
        if (entry.ancestorLimitRefs) entry.ancestorLimitRefs = entry.ancestorLimitRefs.filter(ref => !expiredRef(ref))
      }
      if (retained.length) inputs.set(id, retained); else inputs.delete(id)
    }
    for (const [id, state] of states) {
      if (expiredRef(state.stateRef)) delete state.stateRef
      if (expiredRef(state.runRef)) { delete state.runRef; state.runId = null }
      if (!state.stateRef && !state.runRef) states.delete(id)
    }
    for (const [id, run] of nativeRuns) if (expiredRef(run.runRef)) nativeRuns.delete(id)
    for (const [id, state] of observedSessions) if (state.observedAt <= cutoff || (!rows.has(state.lifecycleId) && !hierarchyRows.has(state.lifecycleId))) observedSessions.delete(id)
    for (const [id, relation] of catalog) if (!hierarchyEvidence.has(relation.evidenceRef.eventId)) { catalog.delete(id); checkedCatalogs.delete(relation.parentSessionId) }
    for (const [id, checkedAt] of checkedCatalogs) if (checkedAt <= cutoff) checkedCatalogs.delete(id)
    if (events.length < before) gap('断连超过保留时间；到期证据未发送')
    for (const [key, value] of toolEvents) if (value.capturedAt <= cutoff) toolEvents.delete(key)
    for (const [key, value] of redactedCalls) if (value.capturedAt <= cutoff) redactedCalls.delete(key)
    for (const [key, value] of toolTokens) if (value.capturedAt <= cutoff) {
      toolTokens.delete(key); gap('未完成工具索引超过 24 小时，已回收；完成状态未知')
    }
    const pending = new Set(events.map(event => event.taskLifecycleId))
    for (const value of toolTokens.values()) pending.add(value.lifecycleId)
    for (const [id, row] of rows) {
      const metadataPending = row.acknowledgedMetadata !== JSON.stringify(taskSnapshot(row)) && row.observedAt > cutoff
      const retainedParent = hierarchy && [...catalog.values()].some(relation => relation.parentSessionId === row.sessionId && sessionRow(relation.childSessionId)?.status === 'running')
      if (row.status !== 'running' && !pending.has(id) && !metadataPending && !retainedParent) dropRow(id)
    }
    while (rows.size > maxEvents) {
      const victim = [...rows].find(([, row]) => row.status !== 'running') ?? rows.entries().next().value
      dropRow(victim[0]); gap('任务索引超过 500 条，旧索引被淘汰；身份仍由宿主弱引用保存，覆盖不完整')
    }
    if (hierarchy) {
      bound(claimedInputs, '输入来源索引超过采集预算；覆盖不完整')
      bound(catalog, '委派目录超过采集预算；覆盖不完整')
      bound(states, '执行状态超过采集预算；覆盖不完整')
      bound(inputs, '指令索引超过采集预算；覆盖不完整')
      bound(checkedCatalogs, '目录覆盖索引超过采集预算；覆盖不完整')
    }
  }
  function task(agent, baseline = false) {
    let title
    try { title = readTitle(agent.session) } catch { /* Missing cached titles do not widen collection to history. */ }
    const sanitizedTitle = sanitizeObservation(title || agent.session?.header?.title || agent.session?.id || agent.id)
    const safeTitle = sanitizedTitle.text
    let lifecycle = identity.agents.get(agent)
    if (!lifecycle) {
      lifecycle = { id: randomUUID(), revision: 0, sequence: -1 }
      identity.agents.set(agent, lifecycle)
    }
    if (!lifecycle.sequenceVersion) {
      // The earlier build used 0 as an empty watermark, but native logs start at 0.
      if (lifecycle.sequence === 0) lifecycle.sequence = -1
      lifecycle.sequenceVersion = 1
    }
    // Retire the previous development build's per-Agent strong tool indexes.
    delete lifecycle.toolEvents
    delete lifecycle.redactedCalls
    if (lifecycle.disposed) return { lifecycle }
    let row = rows.get(lifecycle.id)
    if (!row) {
      row = { id: String(agent.id), lifecycleId: lifecycle.id, sessionId: agent.session?.id,
        title: excerpt(safeTitle || agent.id), status: statusOf(agent.status),
        gaps: baseline ? ['未采集启用前的历史材料', '通过当前存活 Agent 基线确认；采集断档不能恢复'] : [], observedAt: now(), acknowledgedMetadata: null }
      const parentId = agent.session?.header?.parentSession
      if (typeof parentId === 'string') row.parentId = parentId
      rows.set(lifecycle.id, row)
      hierarchyRows.delete(lifecycle.id)
      if (agent.session && typeof agent.session === 'object') sessions.set(agent.session, { lifecycle, agent: new WeakRef(agent) })
      if (hierarchy) {
        record(row, 'status', `任务状态：${row.status}`, { type: 'native-baseline' }, { status: row.status, nativeType: 'native-baseline' })
        const nativeRun = lifecycle.nativeRun?.retainedAt > now() - DAY ? lifecycle.nativeRun : null
        states.set(lifecycle.id, { runId: nativeRun?.runId ?? null, runRef: nativeRun?.reference, stateRef: retain(events.at(-1)) }); finishEvidence()
        try {
          const entries = readCatalog(agent.session)
          if (Array.isArray(entries)) {
            checkedCatalogs.set(row.sessionId, now())
            for (const entry of entries.slice(0, maxEvents)) {
              record(row, 'status', '已核对委派目录成员', { type: 'subagent/catalog', baseline: true }, { nativeType: 'subagent/catalog', childSessionId: entry.id })
              catalog.set(entry.id, { parentSessionId: row.sessionId, childSessionId: entry.id, mode: entry.mode, evidenceRef: retain(events.at(-1)) }); finishEvidence()
            }
          }
        } catch { /* A missing native projection remains a hierarchy coverage gap. */ }
      }
    }
    if (sanitizedTitle.redacted) row.gaps = [...new Set([...row.gaps, '任务名称中认证材料已省略'])]
    row.title = excerpt(safeTitle || agent.id)
    row.status = statusOf(agent.status)
    return { row, lifecycle }
  }
  function record(row, kind, text = '', evidence = {}, data = {}, occurredAt = null, id, capturedAt = now()) {
    const sanitized = sanitizeObservation(text), redacted = sanitized.redacted || data.redacted === true
    const full = redacted ? '' : sanitized.text
    if (redacted) {
      data.redacted = true; delete data.command
      row.gaps = [...new Set([...row.gaps, '包含认证或凭据的观察字段已省略；当前请求证据不完整'])]
    }
    row.observedAt = capturedAt
    if (full.length > 240) { data.truncated = true; if (data.inputOrigin !== 'runtime-notice') row.gaps = [...new Set([...row.gaps, '部分证据摘录超过 240 字符；全文未采集'])] }
    events.push({ id: id ?? `${identity.hostRunId}/${++identity.sequence}`, taskId: row.id,
      taskLifecycleId: row.lifecycleId, kind, occurredAt: Number.isFinite(occurredAt) ? occurredAt : null,
      capturedAt, text: excerpt(full), evidence: typeof evidence === 'string' ? evidence : JSON.stringify(evidence).slice(0, 2000), data })
    if (events.length > maxEvents) { events.splice(0, events.length - maxEvents); gap('断连缓冲超过 500 条，旧事件被淘汰；覆盖不完整') }
    changed()
  }
  function inputFacts(row, message, phase, nativeType, evidence, occurredAt, inherited = false) {
    if (typeof message?.id !== 'string' || inherited) return
    const source = message.source ?? {}, origin = inputOrigin(source, catalog.has(row.sessionId))
    const inputKey = `${row.lifecycleId}/${message.id}/${phase}/${nativeType}`
    const previous = (inputs.get(row.lifecycleId) ?? []).find(item => item.key === inputKey)
    if (previous) return
    if (Number.isFinite(occurredAt) && occurredAt <= now() - DAY) { gap('输入原文超过 24 小时，未重新采集旧证据'); return }
    const capturedAt = Number.isFinite(occurredAt) ? Math.min(now(), occurredAt) : now()
    const facts = { nativeType, inputPhase: phase, inputOrigin: origin, messageId: message.id, sourceKind: source.kind ?? 'unknown', inherited: false }
    if (source.senderSessionId) facts.senderSessionId = source.senderSessionId
    if (source.form) facts.sourceForm = source.form
    if (Number.isSafeInteger(evidence.seq)) facts.inputSequence = evidence.seq
    record(row, 'input', contentText(message.content), evidence, facts, occurredAt, `${identity.hostRunId}/input/${createHash('sha256').update(inputKey).digest('hex')}`, capturedAt)
    const inputRef = retain(events.at(-1)), claimKey = `${row.lifecycleId}/${message.id}`
    if (nativeType === 'agent/inbox/claimed') claimedInputs.set(claimKey, inputRef)
    const entry = { key: inputKey, inputRef, phase, claimRef: claimedInputs.get(claimKey), ancestorLimitRefs: null }
    const retained = [...(inputs.get(row.lifecycleId) ?? []), entry].slice(-100)
    inputs.set(row.lifecycleId, retained)
    finishEvidence()
  }
  function recordTool(row, lifecycle, kind, callId, text, evidence, data, time, capturedAt = now()) {
    const key = callId ? `${kind}/${callKey(lifecycle, callId)}` : null
    const previous = key && toolEvents.get(key)
    const canonical = data.resultSource === 'native-shell-canonical'
    if (previous && (!canonical || previous.canonical)) return
    const base = key ? `${identity.hostRunId}/tool/${createHash('sha256').update(key).digest('hex')}` : undefined
    if (previous) { data.refinesEventId = base; capturedAt = previous.capturedAt }
    if (key) { toolEvents.set(key, { capturedAt, lifecycleId: lifecycle.id, canonical }); bound(toolEvents, '工具去重索引超过 500 条，旧窗口被淘汰；重传仍使用稳定事件 ID') }
    const id = previous ? `${base}/native` : base
    record(row, kind, text, evidence, data, time, id, capturedAt)
  }
  function approvalFacts(row, event, history = false) {
    if (approvalEvents.has(event)) return
    if (typeof event.data?.id !== 'string') { gap('原生审批审计缺少实际审批引用；未建立等待归属'); return }
    const capturedAt = Number.isFinite(event.time) ? Math.min(now(), event.time) : now()
    if (capturedAt <= now() - DAY) { gap('原生审批审计超过 24 小时，未重新采集旧证据'); return }
    approvalEvents.add(event)
    const data = { nativeType: event.type, approvalId: event.data.id, ...(typeof event.data.callId === 'string' ? { callId: event.data.callId } : {}), ...(history ? { history: true } : {}) }
    record(row, 'status', event.type === 'approval/asked' ? '原生审批正在等待用户决定' : '原生审批已结束', { type: event.type, seq: event.seq }, data, event.time, `${identity.hostRunId}/approval/${row.lifecycleId}/${event.seq}`, capturedAt)
  }
  const collector = {
    baseline(agents) { if (closed) return; for (const agent of agents) task(agent, true); sweep(); finishEvidence(); changed() },
    recordGap(reason) { if (closed) return; gap(reason); changed() },
    observe(type, payload) {
      if (closed) return
      sweep()
      if (hierarchy && type === 'subagent/wait-gap') {
        const lifecycle = weakKey(payload.parent) && identity.agents.get(payload.parent), row = lifecycle && (rows.get(lifecycle.id) ?? hierarchyRows.get(lifecycle.id))
        if (row && !lifecycle.disposed && ['audit-capacity', 'target-capacity', 'unpublished-descendant', 'idle-quiescence'].includes(payload.reason) && Number.isFinite(payload.timestamp) && payload.timestamp <= now() && payload.timestamp > now() - DAY) waitGap(row, `原生父等待审计缺口：${payload.reason}；未猜测阻塞或后代`, payload.timestamp)
        return
      }
      if (hierarchy && ['subagent/wait-start', 'subagent/wait-targets', 'subagent/wait-end'].includes(type)) {
        const lifecycle = weakKey(payload.parent) && identity.agents.get(payload.parent)
        const row = lifecycle && (rows.get(lifecycle.id) ?? hierarchyRows.get(lifecycle.id))
        if (lifecycle?.disposed && type === 'subagent/wait-end') return
        if (!row || lifecycle.disposed) { gap('原生等待审计缺少已核对父 Agent；未建立父等待'); return }
        if (payload.purpose !== 'join-operation' || typeof payload.waitId !== 'string' || !payload.waitId || payload.waitId.length > 2000 || !Number.isFinite(payload.timestamp) || payload.timestamp > now() || payload.timestamp <= now() - DAY) { gap('原生等待审计缺失或超过保留窗口；未重新采集旧证据'); return }
        const key = JSON.stringify([lifecycle.id, payload.waitId]), previous = waits.get(key)
        if (type === 'subagent/wait-start' && previous) return
        if (!previous && type !== 'subagent/wait-start') {
          if ([...waits.values()].some(wait => wait.waitId === payload.waitId && wait.parent.deref() === payload.parent)) return
          waitGap(row, '原生等待缺少本次观察窗口的开始审计；未重建父等待', payload.timestamp); return
        }
        const wait = previous ?? { waitId: payload.waitId, parent: new WeakRef(payload.parent), lifecycleId: lifecycle.id, taskId: row.id, sessionId: row.sessionId, startedAt: payload.timestamp, parentStatusAtStart: payload.parentStatusAtStart, parentSeqAtStart: payload.parentSeqAtStart, parentRunId: states.get(lifecycle.id)?.runId ?? null, phase: 'joining', targets: [], updates: 0 }
        wait.targets = (payload.targets ?? []).slice(0, Math.min(maxEvents, 100)).map(target => ({ id: target.id, runId: target.runId, lifecycleId: weakKey(target.agent) ? identity.agents.get(target.agent)?.id ?? null : null }))
        wait.coverageComplete = payload.coverage?.complete === true && (payload.coverage.gaps?.length ?? 0) === 0 && (payload.targets?.length ?? 0) <= Math.min(maxEvents, 100)
        if (type === 'subagent/wait-end') wait.phase = payload.operationSettled === true ? 'ended' : 'unobserved'
        record(row, 'status', type === 'subagent/wait-end' ? '原生子代理 join 操作观察已结束' : '原生子代理 join 操作已观察；阻塞调用者尚未核对', { type }, { nativeType: type, waitId: wait.waitId }, null, `${identity.hostRunId}/wait/${lifecycle.id}/${wait.waitId}/${type}${type === 'subagent/wait-targets' ? `/${++wait.updates}` : ''}`, payload.timestamp)
        wait.auditRef = retain(events.at(-1))
        if (type === 'subagent/wait-start') wait.joinRef = wait.auditRef
        waits.set(key, wait); bound(waits, '原生等待审计索引超过采集预算；覆盖不完整')
        while ([...waits.values()].reduce((count, item) => count + item.targets.length, 0) > maxEvents) {
          waits.delete(waits.keys().next().value)
          gap('原生等待目标总数超过采集预算；旧等待未保留，覆盖不完整')
        }
        finishEvidence()
        return
      }
      if (hierarchy && ['subagent/wait-caller-start', 'subagent/wait-caller-end'].includes(type)) {
        const lifecycle = weakKey(payload.parent) && identity.agents.get(payload.parent), execution = payload.execution
        const row = lifecycle && (rows.get(lifecycle.id) ?? hierarchyRows.get(lifecycle.id))
        const wait = lifecycle && waits.get(JSON.stringify([lifecycle.id, payload.waitId]))
        if (lifecycle?.disposed || !wait && [...waits.values()].some(item => item.waitId === payload.waitId && item.parent.deref() === payload.parent)) return
        if (!row || !wait || !Number.isFinite(payload.timestamp) || payload.timestamp < wait.startedAt || payload.timestamp > now() || payload.timestamp <= now() - DAY) { if (row) waitGap(row, '阻塞调用者缺少匹配的原生等待开始；未建立父等待'); return }
        const stamp = weakKey(execution?.token) && stamps.get(execution.token)
        if (execution?.agent !== payload.parent || stamp?.execution?.deref() !== execution || stamp.lifecycleId !== lifecycle.id) { wait.callerIssue = '原生父等待调用者的精确工具执行身份未核对'; return }
        if (type.endsWith('start')) {
          if (wait.callerRef) return
          const entry = waitRequest(lifecycle.id), input = entry && hierarchyEvidence.get(entry.inputRef.eventId)
          if (wait.parentStatusAtStart !== 'running' || payload.parent.status !== 'running' || stamp.completed || !toolTokens.has(execution.token) || !Number.isSafeInteger(wait.parentSeqAtStart) || !Number.isSafeInteger(input?.data.inputSequence) || input.data.inputSequence >= wait.parentSeqAtStart) { wait.callerIssue = '原生父等待缺少当前活动调用者或已消费请求'; return }
          wait.requestRef = entry.inputRef; wait.execution = new WeakRef(execution)
          record(row, 'status', '已核对实际 await 调用者的父等待开始', { type }, { nativeType: 'execution/wait-start', waitId: wait.waitId, callId: execution.callId }, null, `${identity.hostRunId}/wait/${lifecycle.id}/${wait.waitId}/caller-start`, payload.timestamp)
          wait.callerRef = retain(events.at(-1)); delete wait.callerIssue
        } else {
          if (!wait.callerRef || wait.execution?.deref() !== execution || wait.callerEndRef) return
          record(row, 'status', '实际 await 调用者的父等待作用域已退出', { type }, { nativeType: 'execution/wait-end', waitId: wait.waitId, callId: execution.callId }, null, `${identity.hostRunId}/wait/${lifecycle.id}/${wait.waitId}/caller-end`, payload.timestamp)
          wait.callerEndRef = retain(events.at(-1))
        }
        finishEvidence()
        return
      }
      if (['user-questions/request-start', 'user-questions/request-end'].includes(type)) {
        const agent = payload.agent, lifecycle = weakKey(agent) && identity.agents.get(agent)
        if (!lifecycle || lifecycle.disposed) { if (type.endsWith('start')) gap('用户问询缺少已核对原生 Agent 归属；未建立执行等待'); return }
        if (type.endsWith('end') && payload.lifecycleId !== lifecycle.id) return
        const row = type.endsWith('start') ? task(agent).row : rows.get(lifecycle.id) ?? hierarchyRows.get(lifecycle.id)
        if (!row) return
        const data = { nativeType: type, waitId: payload.waitId, ...(typeof payload.callId === 'string' ? { callId: payload.callId } : {}) }
        record(row, 'status', type.endsWith('start') ? '原生用户问询正在等待回答' : '原生用户问询已返回', { type: 'user-questions/request', waitId: payload.waitId }, data, null, `${identity.hostRunId}/question/${lifecycle.id}/${payload.waitId}/${type.endsWith('start') ? 'start' : 'end'}`)
        return { lifecycleId: lifecycle.id, ...(typeof payload.callId === 'string' ? { callId: payload.callId } : {}) }
      }
      if (hierarchy && type === 'session/observed') {
        if (typeof payload.sessionId !== 'string') return
        let row = sessionRow(payload.sessionId), state = observedSessions.get(payload.sessionId)
        if (!row) {
          state ??= { lifecycleId: randomUUID(), sequence: -1 }
          const title = sanitizeObservation(payload.header?.title || payload.sessionId)
          row = { id: `observed-session/${payload.sessionId}`, lifecycleId: state.lifecycleId, sessionId: payload.sessionId, title: excerpt(title.text), status: 'unknown', gaps: title.redacted ? ['任务名称中认证材料已省略'] : [], observedAt: now() }
          if (payload.header?.parentSession) row.parentId = payload.header.parentSession
          hierarchyRows.set(row.lifecycleId, row); bound(hierarchyRows, '历史执行节点超过采集预算；覆盖不完整')
          if (!hierarchyRows.has(row.lifecycleId)) return { acceptedThrough: state.sequence, complete: false, gaps: [historyGaps.get(row.sessionId).reason] }
        }
        if (!state || state.lifecycleId !== row.lifecycleId) state = { lifecycleId: row.lifecycleId, sequence: -1 }
        state.observedAt = now()
        observedSessions.set(payload.sessionId, state); bound(observedSessions, '只读会话索引超过采集预算；覆盖不完整')
        const candidates = (payload.events ?? []).filter(event => Number.isSafeInteger(event.seq) && event.seq >= (payload.inheritedEventCount ?? 0) && event.seq > state.sequence)
        const own = candidates.slice(0, maxEvents)
        const readGaps = (payload.gaps ?? []).slice(0, 100).map(reason => excerpt(sanitizeObservation(reason).text))
        state.catalogFailures ??= new Map()
        for (const [key, failure] of state.catalogFailures) if (failure.at <= now() - DAY) state.catalogFailures.delete(key)
        if (state.catalogFailureOverflowAt <= now() - DAY) delete state.catalogFailureOverflowAt
        const failedCatalog = (event, reason) => {
          const key = JSON.stringify(typeof event.data?.childId === 'string' ? ['child', event.data.childId] : ['event', event.seq])
          state.catalogFailures.set(key, { reason, at: now() })
          while (state.catalogFailures.size > 100) { state.catalogFailures.delete(state.catalogFailures.keys().next().value); state.catalogFailureOverflowAt ??= now() }
        }
        if (candidates.length > own.length) readGaps.push('只读会话页面超过采集预算；剩余原生材料尚未核对')
        for (const event of own) {
          state.sequence = Math.max(state.sequence, event.seq)
          if (event.type === 'user/message') inputFacts(row, event.data, 'consumed', event.type, { type: event.type, seq: event.seq, observed: true }, event.time)
          if (['approval/asked', 'approval/decided'].includes(event.type)) approvalFacts(row, event, true)
          if (event.type === 'subagent/catalog') {
            const entry = event.data
            if (typeof entry?.childId !== 'string' || ![0, 1, 2].includes(entry.version)) { failedCatalog(event, '只读委派目录格式无法核对'); continue }
            if (Number.isFinite(event.time) && event.time <= now() - DAY) { failedCatalog(event, '只读委派目录超过 24 小时，未重新采集旧证据'); continue }
            state.catalogFailures.delete(JSON.stringify(['child', entry.childId]))
            record(row, 'status', '已核对委派目录成员', { type: event.type, seq: event.seq, observed: true }, { nativeType: event.type, childSessionId: entry.childId }, event.time, `${identity.hostRunId}/observed/${payload.sessionId}/${event.seq}`, Number.isFinite(event.time) ? Math.min(now(), event.time) : now())
            catalog.set(entry.childId, { parentSessionId: row.sessionId, childSessionId: entry.childId, mode: entry.mode, evidenceRef: retain(events.at(-1)) }); finishEvidence()
          }
        }
        state.gaps = [...new Set([...readGaps, ...[...state.catalogFailures.values()].map(failure => failure.reason), ...(state.catalogFailureOverflowAt === undefined ? [] : ['目录失败身份超过记录预算；此前失败未能完整核对'])])]
        if (payload.readComplete && state.gaps.length === 0 && (rows.has(row.lifecycleId) || hierarchyRows.has(row.lifecycleId))) historyGaps.delete(row.sessionId)
        const complete = payload.readComplete === true && candidates.length === own.length && state.gaps.length === 0
        if (complete) checkedCatalogs.set(payload.sessionId, now())
        else checkedCatalogs.delete(payload.sessionId)
        return { acceptedThrough: state.sequence, complete, gaps: state.gaps }
      }
      if (hierarchy && ['subagent/start', 'subagent/end'].includes(type)) {
        const parent = payload.parent, facts = payload.activation
        const capturedAt = Number.isFinite(payload.capturedAt) ? payload.capturedAt : now()
        if (capturedAt <= now() - DAY) { gap('原生执行依据超过 24 小时，未重新续期'); return }
        if (typeof facts?.id !== 'string' || typeof facts?.runId !== 'string') return
        const parentRow = parent ? task(parent).row : sessionRow(catalog.get(facts.id)?.parentSessionId)
        if (!parentRow) return
        record(parentRow, 'status', type === 'subagent/start' ? '委派执行已开始' : '委派执行已结束', { type }, { nativeType: type, childSessionId: facts.id, runId: facts.runId }, null, undefined, capturedAt)
        const evidenceRef = retain(events.at(-1)), relation = catalog.get(facts.id), previousRun = nativeRuns.get(facts.id)
        if (relation?.mode === 'external') externalRow(relation)
        let childRow = payload.child ? task(payload.child).row : sessionRow(facts.id) ?? externalRows.get(facts.id)
        const previousState = childRow && states.get(childRow.lifecycleId)
        if (type === 'subagent/start' && childRow && previousState?.runId && previousState.runId !== facts.runId) {
          childRow.status = 'unknown'
          if (payload.child) {
            const prior = identity.agents.get(payload.child)
            identity.agents.set(payload.child, { id: randomUUID(), revision: prior?.revision ?? 0, sequence: prior?.sequence ?? -1, sequenceVersion: 1 })
            childRow = task(payload.child).row
          } else if (externalRows.has(facts.id)) { hierarchyRows.set(childRow.lifecycleId, { ...childRow }); bound(hierarchyRows, '历史执行节点超过采集预算；覆盖不完整'); externalRows.delete(facts.id); childRow = externalRow(relation) }
        }
        const knownState = childRow && states.get(childRow.lifecycleId)
        const oldEnd = type === 'subagent/end' && knownState?.runId && knownState.runId !== facts.runId
        const boundChild = childRow && !oldEnd && (payload.child || externalRows.has(facts.id) || (previousRun?.lifecycleId === childRow.lifecycleId && previousRun.runId === facts.runId && retainedRef(previousRun.runRef)) || (knownState?.runId === facts.runId && retainedRef(knownState.runRef)))
        if (boundChild) {
          if (externalRows.has(facts.id)) childRow.status = type === 'subagent/start' ? 'running' : 'idle'
          nativeRuns.set(facts.id, { runId: facts.runId, runRef: evidenceRef, lifecycleId: childRow.lifecycleId })
          bound(nativeRuns, '执行代次索引超过采集预算；覆盖不完整')
          states.set(childRow.lifecycleId, { ...states.get(childRow.lifecycleId), runId: facts.runId, runRef: evidenceRef })
        } else { if (!childRow) { pendingExternalRuns.set(facts.id, { runId: facts.runId, runRef: evidenceRef, state: type === 'subagent/start' ? 'running' : 'idle' }); bound(pendingExternalRuns, '未关联外部执行索引超过采集预算；覆盖不完整') }; finishEvidence(); gap('原生执行代次归属未核对；结束依据保留，节点状态不可判断') }
        if (payload.child && boundChild) {
          const lifecycle = identity.agents.get(payload.child)
          if (lifecycle) lifecycle.nativeRun = { runId: facts.runId, reference: { ...evidenceRef, retainedAt: capturedAt }, retainedAt: capturedAt }
        }
        finishEvidence()
        return
      }
      if (type === 'session/event') {
        const state = sessions.get(payload.session), event = payload.event
        if (!state || state.lifecycle.disposed || !Number.isSafeInteger(event?.seq) || event.seq <= state.lifecycle.sequence) return
        state.lifecycle.sequence = event.seq
        if (event.type === 'session/title') {
          const title = sanitizeObservation(event.data?.title)
          if (!title.text && !title.redacted) return
          let row = rows.get(state.lifecycle.id)
          if (!row) {
            const agent = state.agent.deref()
            if (!agent || agent.status === 'disposed') return
            row = task(agent).row
          }
          if (!row) return
          if (title.redacted) row.gaps = [...new Set([...row.gaps, '任务名称中认证材料已省略'])]
          if (title.text) row.title = excerpt(title.text)
          row.observedAt = now()
          changed()
          return
        }
        const row = rows.get(state.lifecycle.id) ?? hierarchyRows.get(state.lifecycle.id)
        if (!row) return
        if (['approval/asked', 'approval/decided'].includes(event.type)) {
          if (event.seq >= (payload.session?.inheritedEventCount ?? 0)) approvalFacts(row, event)
          return
        }
        if (hierarchy && event.type === 'subagent/catalog' && event.seq >= (payload.session?.inheritedEventCount ?? 0)) {
          const entry = event.data
          if (![0, 1, 2].includes(entry?.version) || !['one-shot', 'continuable', 'external', 'unknown'].includes(entry.mode) || typeof entry.childId !== 'string') { gap('委派目录格式无法核对'); return }
          record(row, 'status', '已核对委派目录成员', { type: event.type, seq: event.seq }, { nativeType: event.type, childSessionId: entry.childId }, event.time)
          catalog.set(entry.childId, { parentSessionId: row.sessionId, childSessionId: entry.childId, mode: entry.mode, evidenceRef: retain(events.at(-1)) }); finishEvidence()
          if (checkedCatalogs.has(row.sessionId)) checkedCatalogs.set(row.sessionId, now())
          return
        }
        if (hierarchy && event.type === 'user/message') {
          inputFacts(row, event.data, 'consumed', event.type, { type: event.type, seq: event.seq }, event.time, event.seq < (payload.session?.inheritedEventCount ?? 0))
          return
        }
        if (!['tool/call', 'tool/result', 'tool/ptc-dispatch-start', 'tool/ptc-dispatch'].includes(event.type)) return
        if (Number.isFinite(event.time) && event.time <= now() - DAY) { gap('工具持久事件超过 24 小时，未重新采集旧证据'); return }
        const data = event.type === 'tool/result' ? event.data.message : event.data
        // A tool message's id identifies the message; toolCallId cites its call.
        const callId = event.type === 'tool/result' ? data.toolCallId : data.subCallId ?? data.callId
        if (typeof callId !== 'string' || !callId.trim()) {
          row.gaps = [...new Set([...row.gaps, '工具持久事件缺少真实工具调用引用；未建立结果归属'])]
          row.observedAt = now(); changed(); return
        }
        const kind = ['tool/call', 'tool/ptc-dispatch-start'].includes(event.type) ? 'tool-start' : 'tool-result'
        const facts = { callId, rootCallId: data.rootCallId, parentCallId: data.parentCallId, toolName: data.name,
          isError: data.isError, errorCode: data.error?.code }
        const verification = extractVerificationEvidence({ type, event })
        Object.assign(facts, verification.data)
        if (verification.gaps.length) facts.verificationGaps = verification.gaps
        if (redactedCalls.has(callKey(state.lifecycle, callId))) facts.redacted = true
        let args = data.arguments
        if (event.type === 'tool/call' && typeof args === 'string') {
          if (args.length > 24_000) { args = undefined; row.gaps = [...new Set([...row.gaps, '原生工具参数超过固定输入预算；动作文本未采集'])] }
          else try { args = JSON.parse(args) } catch { args = undefined; row.gaps = [...new Set([...row.gaps, '原生工具参数 JSON 无法核对；动作文本未采集'])] }
        }
        const command = kind === 'tool-start' && typeof args?.command === 'string' ? sanitizeObservation(args.command) : null
        if (command) {
          if (command.redacted) facts.redacted = true
          else { facts.command = excerpt(command.text); if (command.text.length > 240) facts.truncated = true }
        }
        recordTool(row, state.lifecycle, kind, callId, command ? command.text : kind === 'tool-start' && typeof args?.prompt === 'string' ? args.prompt : contentText(data.content), { type: event.type, seq: event.seq }, facts, event.time)
        return
      }
      const agent = payload.agent ?? payload.exec?.agent
      if (!agent) return
      const { row, lifecycle } = task(agent)
      if (!row) return // Late events from a disposed Agent cannot recreate its lifecycle.
      if (type === 'agent/created') { record(row, 'status', '已发现任务', { type }, { status: row.status, nativeType: type }); if (hierarchy) { states.set(lifecycle.id, { ...states.get(lifecycle.id), stateRef: retain(events.at(-1)) }); finishEvidence() }; sweep(); return }
      if (type === 'agent/status' || type === 'agent/disposed') {
        row.status = type === 'agent/disposed' ? 'disposed' : statusOf(payload.status)
        if (type === 'agent/disposed') lifecycle.disposed = true
        record(row, 'status', `任务状态：${row.status}`, { type }, { status: row.status, nativeType: type }); if (hierarchy) { states.set(lifecycle.id, { ...states.get(lifecycle.id), stateRef: retain(events.at(-1)) }); finishEvidence() }; return
      }
      if (hierarchy && type === 'agent/inbox/spliced') {
        for (const message of payload.inserted ?? []) inputFacts(row, message, 'accepted', type, { type, turn: payload.turn }, null)
        return
      }
      if (type === 'agent/inbox/claimed') {
        if (hierarchy) inputFacts(row, payload.message, 'accepted', type, { type, turn: payload.turn }, null)
        else if (payload.message?.source?.kind === 'user') record(row, 'input', contentText(payload.message.content), { type, turn: payload.turn, source: 'user' })
        else { row.gaps = [...new Set([...row.gaps, '部分输入来自代理或系统，未当作用户授权'])]; record(row, 'gap', contentText(payload.message?.content), { type, turn: payload.turn }) }
        return
      }
      if (type === 'agent/assistant-stream') {
        const frame = payload.frame
        if (!Number.isSafeInteger(frame?.revision) || frame.revision <= lifecycle.revision) return
        lifecycle.revision = frame.revision
        const text = frame.type === 'chunk' ? frame.chunk?.text ?? frame.chunk?.block?.text : ''
        if (typeof text === 'string' && text) record(row, 'output', text, { type, revision: frame.revision, attemptId: frame.attemptId, chunkType: frame.chunk?.type }, {}, frame.time)
        return
      }
      if (type === 'tools/execute' || type === 'tools/result') {
        const { exec, result } = payload, key = callKey(lifecycle, exec.callId)
        let stamp = weakKey(exec.token) ? stamps.get(exec.token) : undefined
        if (!stamp) {
          stamp = { capturedAt: now(), lifecycleId: lifecycle.id, redacted: false, completed: false }
          if (weakKey(exec.token)) stamps.set(exec.token, stamp)
        }
        if (stamp.completed) return
        if (stamp.lifecycleId !== lifecycle.id || (type === 'tools/execute' && stamp.capturedAt <= now() - DAY)) {
          toolTokens.delete(exec.token); redactedCalls.delete(key)
          gap('工具调用已超过采集窗口或生命周期不符，晚到结果未复活旧证据'); return
        }
        if (type === 'tools/execute') {
          stamp.execution ??= new WeakRef(exec)
          toolTokens.set(exec.token, { callId: exec.callId, lifecycleId: lifecycle.id, capturedAt: stamp.capturedAt })
          bound(toolTokens, '未完成工具索引超过 500 条，旧索引被淘汰；完成状态未知')
        }
        const parent = toolTokens.get(exec.parent)
        const facts = { callId: exec.callId, rootCallId: exec.rootCallId, parentCallId: parent?.lifecycleId === lifecycle.id ? parent.callId : undefined, toolName: exec.name }
        if (result) { facts.isError = result.isError; if (result.error?.code) facts.errorCode = result.error.code }
        const verification = extractVerificationEvidence({ type, exec, result })
        Object.assign(facts, verification.data)
        if (verification.gaps.length) facts.verificationGaps = verification.gaps
        const command = exec.arguments?.command
        const prompt = exec.arguments?.prompt
        if (typeof command === 'string') {
          const sanitized = sanitizeObservation(command)
          if (sanitized.redacted) {
            facts.redacted = stamp.redacted = true
            redactedCalls.set(key, { lifecycleId: lifecycle.id, capturedAt: stamp.capturedAt })
            bound(redactedCalls, '凭据省略调用索引超过 500 条，旧索引被淘汰；该窗口覆盖不完整')
          } else { facts.command = excerpt(sanitized.text); if (sanitized.text.length > 240) facts.truncated = true }
        }
        if (stamp.redacted || redactedCalls.has(key)) facts.redacted = true
        recordTool(row, lifecycle, type === 'tools/execute' ? 'tool-start' : 'tool-result', exec.callId,
          result ? contentText(result.content) : typeof command === 'string' ? command : typeof prompt === 'string' ? prompt : '', { type }, facts, null, type === 'tools/execute' ? stamp.capturedAt : now())
        if (type === 'tools/result') { stamp.completed = true; toolTokens.delete(exec.token); redactedCalls.delete(key) }
      }
      sweep()
      finishEvidence()
    },
    batch() {
      sweep()
      if (hierarchy) for (const relation of catalog.values()) if (relation.mode === 'external') externalRow(relation)
      const historicalTasks = hierarchy ? [...hierarchyRows.values(), ...externalRows.values()] : []
      const batch = { events, tasks: [...historicalTasks.filter(row => row.acknowledgedMetadata !== JSON.stringify(taskSnapshot(row))), ...rows.values()].map(taskSnapshot), gaps: [...gaps] }
      if (hierarchy) {
        const graphTasks = [...historicalTasks, ...rows.values()].map(taskSnapshot)
        const liveSessions = new Set(graphTasks.map(row => row.sessionId).filter(Boolean))
        const relations = [...catalog.values()].filter(item => liveSessions.has(item.parentSessionId) && (liveSessions.has(item.childSessionId) || externalRows.has(item.childSessionId)) && hierarchyEvidence.has(item.evidenceRef.eventId)).map(item => ({ parentSessionId: item.parentSessionId, ...(item.mode === 'external' && externalRows.has(item.childSessionId) ? { childActivationRef: { taskId: externalRows.get(item.childSessionId).id, lifecycleId: externalRows.get(item.childSessionId).lifecycleId } } : { childSessionId: item.childSessionId }), kind: 'delegation', evidenceRefs: [item.evidenceRef] }))
        const branchGaps = [...historyGaps].map(([sessionId, value]) => ({ sessionId, reason: value.reason })).concat([...waitGaps.values()].flatMap(({ sessionId, reasons }) => reasons.map(reason => ({ sessionId, reason }))))
        const waitBindings = []
        for (const wait of waits.values()) {
          let reason = wait.callerIssue
          const parent = graphTasks.find(row => row.lifecycleId === wait.lifecycleId), execution = wait.execution?.deref()
          const request = waitRequest(wait.lifecycleId)
          if (!wait.callerRef) reason ??= '原生 join 操作缺少已核对阻塞调用者；父等待关系不可判断'
          else if (!hierarchyEvidence.has(wait.joinRef?.eventId) || !hierarchyEvidence.has(wait.callerRef.eventId) || !hierarchyEvidence.has(wait.auditRef?.eventId) || !hierarchyEvidence.has(wait.requestRef?.eventId) || wait.callerEndRef && !hierarchyEvidence.has(wait.callerEndRef.eventId) || !wait.callerEndRef && (wait.phase !== 'joining' || parent?.status !== 'running' || request?.inputRef.eventId !== wait.requestRef?.eventId || !execution || !toolTokens.has(execution.token) || (states.get(wait.lifecycleId)?.runId ?? null) !== wait.parentRunId)) reason ??= '原生父等待已超出原请求、工具或执行代次；开闭依据不足'
          const childRefs = []
          for (const target of wait.targets) {
            const child = graphTasks.find(row => row.lifecycleId === target.lifecycleId && row.sessionId === target.id), run = child && states.get(child.lifecycleId)
            const seen = new Set(); let ancestor = target.id
            while (ancestor && ancestor !== wait.sessionId && !seen.has(ancestor)) { seen.add(ancestor); ancestor = relations.find(relation => relation.childSessionId === ancestor)?.parentSessionId }
            if (!child || run?.runId !== target.runId || !retainedRef(run.runRef) || ancestor !== wait.sessionId || target.id === wait.sessionId) { reason ??= '原生父等待目标或当前执行代次尚未核对'; continue }
            childRefs.push({ taskId: child.id, lifecycleId: child.lifecycleId })
          }
          if (!wait.coverageComplete || !childRefs.length || childRefs.length !== wait.targets.length) reason ??= '原生父等待目标为空或覆盖不完整；未制造后代依据'
          if (!reason) waitBindings.push({ taskId: wait.taskId, lifecycleId: wait.lifecycleId, waitId: wait.waitId, phase: wait.callerEndRef ? 'ended' : 'started', childRefs, evidenceRefs: [...new Map([wait.joinRef, wait.callerRef, wait.auditRef, wait.callerEndRef].filter(Boolean).map(ref => [ref.eventId, ref])).values()] })
          else if (!wait.callerEndRef && wait.phase !== 'ended') branchGaps.push({ sessionId: wait.sessionId, reason })
        }
        branchGaps.push(...[...externalRows.values()].map(row => ({ sessionId: row.id, reason: '外部执行内部材料不可观察' })))
        branchGaps.push(...[...observedSessions].flatMap(([sessionId, state]) => (state.gaps ?? []).map(reason => ({ sessionId, reason }))))
        branchGaps.push(...[...liveSessions].filter(id => !checkedCatalogs.has(id)).map(sessionId => ({ sessionId, reason: '后代目录尚未完成核对' })))
        for (const relation of catalog.values()) if (!liveSessions.has(relation.parentSessionId)) branchGaps.push({ sessionId: relation.parentSessionId, reason: '目录拥有节点缺少当前观察身份；归属尚未完整核对' })
        for (const relation of catalog.values()) if (!liveSessions.has(relation.childSessionId) && !externalRows.has(relation.childSessionId)) branchGaps.push({ sessionId: relation.childSessionId, reason: '目录成员缺少已核对执行与会话材料' })
        const context = { schemaVersion: 1, sessions: [...liveSessions].map(sessionId => ({ sessionId })), activations: graphTasks.map(row => {
          const state = states.get(row.lifecycleId), stateRef = state?.stateRef && hierarchyEvidence.has(state.stateRef.eventId) ? state.stateRef : null
          const runKey = row.sessionId ?? row.id
          const run = state?.runId ? state : nativeRuns.get(runKey)?.lifecycleId === row.lifecycleId ? nativeRuns.get(runKey) : null
          const runRef = retainedRef(run?.runRef) ? { taskId: run.runRef.taskId, lifecycleId: run.runRef.lifecycleId, eventId: run.runRef.eventId } : null
          return { taskId: row.id, lifecycleId: row.lifecycleId, sessionId: row.sessionId ?? null, runId: runRef ? run.runId : null, state: stateRef || row.sessionId === null && runRef ? row.status === 'running' ? 'running' : ['idle', 'disposed'].includes(row.status) ? 'ended' : 'unknown' : 'unknown', stateRefs: [...new Map([stateRef, runRef].filter(Boolean).map(reference => [reference.eventId, reference])).values()] }
        }), relations, instructionBindings: graphTasks.flatMap(row => {
          const relation = relations.find(item => item.childSessionId === row.sessionId)
          return (inputs.get(row.lifecycleId) ?? []).flatMap(entry => {
            const event = hierarchyEvidence.get(entry.inputRef.eventId)
            if (!event) return []
            const data = event.data, claimRef = entry.claimRef ?? claimedInputs.get(`${row.lifecycleId}/${data.messageId}`)
            const classified = inputOrigin({ kind: data.sourceKind, form: data.sourceForm, senderSessionId: data.senderSessionId }, !!relation)
            const origin = classified === 'unknown' && data.sourceKind === 'user' && entry.phase === 'consumed' && claimRef && !row.parentId && !data.sourceForm ? 'user-entry' : classified
            if (entry.phase === 'consumed' && relation && entry.ancestorLimitRefs === null) {
              const parent = sessionRow(relation.parentSessionId), parentInputs = parent && inputs.get(parent.lifecycleId)
              const current = parentInputs?.filter(item => item.phase === 'consumed' && hierarchyEvidence.get(item.inputRef.eventId)?.data.sourceKind === 'user' && hierarchyEvidence.get(item.inputRef.eventId)?.data && ['unknown', 'delegated'].includes(hierarchyEvidence.get(item.inputRef.eventId)?.data.inputOrigin)).at(-1)
              const earlier = (inputs.get(row.lifecycleId) ?? []).filter(item => item !== entry && item.phase === 'consumed' && item.ancestorLimitRefs?.length).at(-1)
              entry.ancestorLimitRefs = [...(earlier?.ancestorLimitRefs ?? current?.ancestorLimitRefs ?? []), ...(!earlier && current && !relations.some(item => item.childSessionId === parent.sessionId) ? [current.inputRef] : [])].slice(0, 100)
            }
            const originRefs = [...(relation?.evidenceRefs ?? []), ...(claimRef && hierarchyEvidence.has(claimRef.eventId) ? [claimRef] : [])]
            return [{ taskId: row.id, lifecycleId: row.lifecycleId, inputRef: entry.inputRef, phase: entry.phase, origin, originRefs, ancestorLimitRefs: (entry.ancestorLimitRefs ?? []).filter(ref => hierarchyEvidence.has(ref.eventId) && graphTasks.some(task => task.id === ref.taskId && task.lifecycleId === ref.lifecycleId)) }]
          })
        }), ...(waitBindings.length ? { waitBindings } : {}), coverage: { complete: branchGaps.length === 0, branchGaps: branchGaps.slice(0, 100) } }
        const content = JSON.stringify(context)
        if (identity.hierarchyContent !== content) { identity.hierarchyRevision = (identity.hierarchyRevision ?? 0) + 1; identity.hierarchyContent = content }
        batch.hierarchyContext = { ...context, revision: identity.hierarchyRevision }
        const needed = new Set([...context.activations.flatMap(item => item.stateRefs), ...context.relations.flatMap(item => item.evidenceRefs), ...context.instructionBindings.flatMap(item => [item.inputRef, ...item.originRefs, ...item.ancestorLimitRefs]), ...(context.waitBindings ?? []).flatMap(item => item.evidenceRefs)].map(item => item.eventId))
        batch.events = [...new Map([...events, ...[...hierarchyEvidence.values()].filter(event => needed.has(event.id))].map(event => [event.id, event])).values()].slice(-1000)
      }
      return structuredClone(batch)
    },
    acknowledge(ids, taskSnapshots = []) {
      const accepted = new Set(ids)
      events = events.filter(event => !accepted.has(event.id))
      for (const snapshot of taskSnapshots) {
        const row = rows.get(snapshot.lifecycleId) ?? hierarchyRows.get(snapshot.lifecycleId) ?? [...externalRows.values()].find(row => row.lifecycleId === snapshot.lifecycleId), delivered = JSON.stringify(snapshot)
        // A newer title/status arriving during transport must still be sent.
        if (row && delivered === JSON.stringify(taskSnapshot(row))) row.acknowledgedMetadata = delivered
      }
      sweep()
      if (hierarchy) collectEvidence()
    },
    diagnostics() { sweep(); return { bufferedEvents: events.length, taskRows: rows.size, toolDedupeEntries: toolEvents.size,
      redactedCalls: redactedCalls.size, pendingTools: toolTokens.size, gaps: gaps.size, closed, ...(hierarchy ? { hierarchyWaits: waits.size, hierarchyWaitGaps: waitGaps.size, hierarchyWaitTargets: [...waits.values()].reduce((count, wait) => count + wait.targets.length, 0), hierarchyRows: hierarchyRows.size, hierarchyEvidence: hierarchyEvidence.size, hierarchyRelations: catalog.size, hierarchyStates: states.size, hierarchyInputs: inputs.size, hierarchyInputBindings: [...inputs.values()].reduce((sum, entries) => sum + entries.length, 0), hierarchyAncestorRefs: [...inputs.values()].reduce((sum, entries) => sum + entries.reduce((count, entry) => count + (entry.ancestorLimitRefs?.length ?? 0), 0), 0), hierarchyExternalRows: externalRows.size, hierarchyPendingExternalRuns: pendingExternalRuns.size, hierarchyCatalogs: checkedCatalogs.size, hierarchyRuns: nativeRuns.size, hierarchyObservedSessions: observedSessions.size, hierarchyHistoryGaps: historyGaps.size } : {}) } },
    close() { if (closed) return; closed = true; events = []; rows.clear(); toolTokens.clear(); toolEvents.clear(); redactedCalls.clear(); gaps.clear(); claimedInputs.clear(); observedSessions.clear(); historyGaps.clear(); externalRows.clear(); pendingExternalRuns.clear(); catalog.clear(); states.clear(); inputs.clear(); hierarchyEvidence.clear(); checkedCatalogs.clear(); nativeRuns.clear(); hierarchyRows.clear(); waits.clear(); waitGaps.clear(); stamps = new WeakMap(); approvalEvents = new WeakSet(); referenceTimes = new WeakMap(); if (hierarchy) identity.hierarchyContent = null },
  }
  return collector
}
