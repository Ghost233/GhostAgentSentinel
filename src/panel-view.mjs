function summaryEvent(task, reference, tasks) {
  const owner = tasks.find(candidate => candidate.sourceKey === task.sourceKey && candidate.id === reference.taskId && candidate.lifecycleId === reference.lifecycleId)
  return owner?.events.find(event => event.id === reference.eventId && event.taskId === reference.taskId && event.taskLifecycleId === reference.lifecycleId)
}

function blockingView(decision, readEvidence, readAt, historical) {
  if (!decision) return null
  const evidence = []
  for (const reference of decision.evidenceRefs) {
    const event = readEvidence(reference)
    if (!event || !(event.capturedAt > readAt - 86400000 && event.capturedAt <= readAt)) return null
    evidence.push({ text: event.text.slice(0, 240), reference: event.evidence ?? JSON.stringify(reference) })
  }
  return { status: decision.status, reason: decision.reason, evidence, historical }
}

export function materialView(view, task, { historical = false, snapshot, readEvidence } = {}) {
  const states = { queued: '检查排队中', analyzing: '正在检查', unavailable: '本地摘取模型或 JEV 未配置', failed: '本轮分析失败', unknown: '当前请求或分析证据不足', cooldown: '本轮六项检查已更新', idle: '最近检查结果' }
  const readAt = snapshot?.generatedAt ?? Date.now()
  const ownerTasks = snapshot?.tasks ?? [task]
  const evidenceFor = readEvidence ?? (reference => summaryEvent(task, reference, ownerTasks))
  const findings = historical ? task.analysis.checks.filter(check => check.status === 'attention').map(check => ({ ...check, checkId: check.id })) : task.alerts.filter(alert => alert.status === 'attention')
  const issues = findings.map(alert => {
    const check = task.analysis.checks.find(check => check.id === alert.checkId)
    const confirmed = ['current', 'reused'].includes(task.analysis.resultDisposition) && !['failed', 'unavailable', 'interrupted', 'stale'].includes(task.analysis.state) && check?.status === 'attention' && !view.observation?.recordRevalidating
    const evidence = (Array.isArray(alert.evidenceRefs)
      ? alert.evidenceRefs.map(evidenceFor).filter(Boolean)
      : task.events.filter(event => alert.evidenceIds?.includes(event.id)))
      .filter(event => event.capturedAt <= readAt && event.capturedAt > readAt - SESSION_HISTORY_MS)
    const results = evidence.filter(event => event.kind === 'tool-result').slice(-2)
    const selected = alert.checkId !== 'stalled_progress' ? evidence.slice(0, 2) : results.length ? [...evidence.filter(event => event.kind !== 'tool-result').slice(-1), ...results].slice(-2) : evidence.slice(-2)
    let reviewReason = '已有新材料或检查尚未完成，等待本轮复核'
    if (task.analysis.coverage?.gaps?.includes('analysis_input_budget') && check?.status === 'unknown') reviewReason = '本轮判断材料超过预算，尚未确认原疑点是否仍存在'
    else if (['current', 'reused'].includes(task.analysis.resultDisposition) && check?.status !== 'attention') reviewReason = check?.reason ?? '本轮尚未确认原疑点是否仍存在'
    return { checkId: alert.checkId, reason: alert.reason.slice(0, 240), assessment: historical ? 'historical' : confirmed ? 'current' : 'retained', observedAt: historical ? task.analysis.completedAt : alert.updatedAt, evidenceThrough: historical ? task.analysis.through : alert.evidenceThrough,
      ...(!historical && !confirmed ? { reviewReason } : {}),
      notificationStatus: !historical && ['pending', 'claimed', 'submitted', 'failed', 'cancelled', 'expired'].includes(alert.notification?.status) ? alert.notification.status : null,
      evidence: selected.map(event => ({ text: event.text.slice(0, 120), reference: event.evidence ?? event.id })) }
  })
  const own = task.events.filter(event => event.taskLifecycleId === task.lifecycleId && !event.data?.history && !event.data?.inherited)
  const inputRef = task.instructionContext?.binding?.inputRef
  const request = own.find(event => event.kind === 'input' && event.id === inputRef?.eventId && event.taskId === inputRef.taskId && event.taskLifecycleId === inputRef.lifecycleId && event.authorizationEligible)
  const currentFacts = !historical ? { ...(request ? { request: { text: task.currentRequest, evidenceIds: [request.id] } } : {}), activity: { ...task.activity, evidenceIds: (task.activity?.evidenceIds ?? []).filter(id => own.some(event => event.id === id)) } } : null
  const forkBinding = task.instructionContext?.binding?.forkContext
  const inheritance = forkBinding ? { state: forkBinding.receiptId ? 'complete' : 'pending', gaps: [...new Set([...(task.instructionContext.gaps ?? []), ...(!historical ? view.inheritance?.gaps ?? [] : [])])] } : null
  const revalidating = !historical && view.observation?.recordRevalidating === true
  const retainedOnly = issues.length > 0 && issues.every(issue => issue.assessment === 'retained')
  const summaryVisible = (historical || ['current', 'reused'].includes(task.analysis.resultDisposition)) && !['failed', 'unavailable', 'interrupted', 'stale'].includes(task.analysis.state)
  const generated = summaryVisible ? task.analysis.summary?.generated : null
  const partial = summaryVisible && task.analysis.coverage?.complete === false ? task.analysis.summary?.partial : null
  const prior = task.analysis.lastSuccessfulSummary
  const generatedView = (value, historicalDecision = historical || revalidating) => {
    if (!value) return null
    const blocking = blockingView(value.blocking, evidenceFor, readAt, historicalDecision)
    const evidence = value.evidenceRefs.slice(0, 2).map(reference => {
      const event = evidenceFor(reference)
      return { text: event?.text?.slice(0, 240) ?? '引用材料当前不可读取', reference: event?.evidence ?? JSON.stringify(reference) }
    })
    return { text: value.text, ...(value.sections ? { sections: value.sections } : {}), evidence, ...(blocking ? { blocking } : {}) }
  }
  const lastSuccessfulSummary = prior && prior.completedAt <= readAt && prior.expiresAt > readAt && (!prior.configScope || prior.configScope.registrationId === task.analysis.currentConfigScope?.registrationId)
    ? { ...prior, generated: generatedView(prior.generated, true) } : null
  const available = !historical && task.analysis.availableSummary
  const availableSummary = available ? { result: generatedView(available.generated ?? available.partial), partial: Boolean(available.partial), gaps: available.gaps ?? [] } : null
  return { ...view, ...(currentFacts ? { currentFacts } : {}), ...(!historical && inheritance ? { inheritance } : {}), signal: task.conversationMonitoring?.paused || revalidating ? 'gray' : task.gs?.light ?? 'gray', summary: (task.conversationMonitoring?.paused ? '本聊天分析已暂停，继续只读采集' : revalidating ? '当前记录重验中' : retainedOnly ? '保留历史告警，等待本轮复核；当前是否仍存在尚未确认' : task.gs?.reason ?? view.summary).slice(0, 240),
    ...(task.conversationMonitoring ? { monitoring: task.conversationMonitoring } : {}),
    health: task.monitoringHealth,
    observation: { ...view.observation, gap: view.observation.gap ?? own.find(event => event.kind === 'gap' && event.data?.verificationGaps?.length)?.data.verificationGaps[0] ?? (!task.currentRequest ? 'current-user-request-unverified' : null) },
    analysis: { connected: true, state: task.analysis.state, summary: states[task.analysis.state] ?? '本轮尚无有效检查结论', checks: task.analysis.checks, issues,
      generated: generatedView(generated), partial: generatedView(partial), ...(availableSummary ? { availableSummary } : {}), lastSuccessfulSummary, configScope: task.analysis.configScope, currentConfigScope: task.analysis.currentConfigScope,
      resultDisposition: task.analysis.resultDisposition, completedAt: task.analysis.completedAt, nextEligibleAt: task.analysis.nextEligibleAt, through: task.analysis.through, waitMs: task.analysis.waitMs, lagMs: task.analysis.lagMs, error: task.analysis.error, coverage: task.analysis.coverage ?? task.coverage } }
}

export function recentModelActivity(chatTasks, currentTask, registrationId, readAt, currentObserved, ownerTasks = chatTasks, readEvidence) {
  const cutoff = readAt - 86400000
  let prefill = null, summary = null
  for (const task of chatTasks) {
    const evidenceFor = readEvidence ?? (reference => summaryEvent(task, reference, ownerTasks))
    const { lastPrefill, lastSuccessfulSummary, lastPartialSummary, lastUnjudgedSummary } = task.analysis
    if (lastPrefill?.submittedAt > cutoff && lastPrefill.submittedAt <= readAt && (!prefill || lastPrefill.submittedAt > prefill.submittedAt)) {
      prefill = { submittedAt: lastPrefill.submittedAt, state: lastPrefill.state }
    }
    const available = currentObserved && task === currentTask ? task.analysis.availableSummary : null
    for (const record of [available, lastSuccessfulSummary, lastPartialSummary, lastUnjudgedSummary]) {
      const result = record?.generated ?? record?.partial
      if (!result?.text || !(record.completedAt > cutoff && record.completedAt <= readAt && record.expiresAt > readAt)
        || summary && record.completedAt <= summary.completedAt || record.configScope && record.configScope.registrationId !== registrationId) continue
      if (!result.evidenceRefs?.length || !result.evidenceRefs.every(ref => {
        const event = evidenceFor(ref)
        return event && event.capturedAt > cutoff && event.capturedAt <= readAt
      })) continue
      const current = record === available || currentObserved && task === currentTask && task.status === 'running' && record.completedAt === task.analysis.completedAt
        && ['current', 'reused'].includes(task.analysis.resultDisposition) && ['cooldown', 'idle'].includes(task.analysis.state)
        && !task.conversationMonitoring?.paused && task.monitoringHealth?.state !== 'failed'
      const blocking = blockingView(result.blocking, evidenceFor, readAt, !current)
      summary = { text: result.text, ...(result.sections ? { sections: result.sections } : {}), completedAt: record.completedAt, expiresAt: record.expiresAt, partial: Boolean(record.partial), gaps: record.gaps ?? [], ...(record.final ? { final: true } : {}), ...(blocking ? { blocking } : {}) }
    }
  }
  const attempt = currentTask?.analysis.prefill
  if (!prefill && attempt?.updatedAt > cutoff && attempt.updatedAt <= readAt) {
    if (attempt.state === 'unsupported') prefill = { state: attempt.reason === 'not-supported' ? 'unsupported' : 'unconfirmed' }
    if (attempt.state === 'completed') prefill = { state: 'time-unrecorded' }
  }
  const prefillRecovery = currentTask?.analysis.prefillRecovery
  return { prefill, summary, ...(prefillRecovery ? { prefillRecovery } : {}) }
}

const SESSION_HISTORY_MS = 86_400_000

export function projectPanelOverview(sources, tasks, hierarchy, offset = 0) {
  const counts = { green: 0, yellow: 0, red: 0, gray: 0 }
  const tasksByKey = new Map(tasks.map(task => [task.key, task]))
  const sourceByKey = new Map(sources.map(source => [source.key, source]))
  const grouped = new Set()
  const rows = []
  const rank = value => value.red ? 0 : value.yellow ? 1 : value.gray ? 2 : value.green ? 3 : 4
  function addRow(items, main, tally, complete) {
    for (const color of Object.keys(counts)) counts[color] += tally[color] ?? 0
    const source = sourceByKey.get(main.sourceKey)
    const attention = items.find(task => task.gs.light === 'red') ?? items.find(task => task.gs.light === 'yellow') ?? main
    rows.push({ title: main.title?.slice(0, 240) || '未命名会话', source: source.descriptor.name.slice(0, 160), connected: source.status === 'connected',
      status: main.status, counts: tally, complete, reason: attention.gs.reason?.slice(0, 240), updatedAt: main.lastActivity ?? main.updatedAt })
  }
  for (const group of hierarchy.groups) {
    const items = group.nodes.map(node => tasksByKey.get(node.taskKey)).filter(Boolean)
    if (!items.length) continue
    for (const task of items) grouped.add(task.key)
    const main = items.find(task => task.sessionId === group.rootSessionId) ?? items[0]
    addRow(items, main, group.counts, group.coverage.complete)
  }
  const remaining = new Map()
  for (const task of tasks) {
    if (grouped.has(task.key)) continue
    const key = JSON.stringify([task.sourceKey, task.sessionId ?? task.id])
    const previous = remaining.get(key)
    const active = Object.hasOwn(counts, task.gs.light)
    const previousActive = previous && Object.hasOwn(counts, previous.gs.light)
    if (!previous || (active && !previousActive) || active === previousActive && task.updatedAt > previous.updatedAt) remaining.set(key, task)
  }
  for (const task of remaining.values()) {
    const color = task.gs.light
    addRow([task], task, Object.hasOwn(counts, color) ? { [color]: 1 } : {}, task.gs.coverage.complete)
  }
  rows.sort((a, b) => rank(a.counts) - rank(b.counts) || (b.updatedAt ?? 0) - (a.updatedAt ?? 0) || a.title.localeCompare(b.title))
  const page = rows.slice(offset, offset + 30)
  return { counts, total: rows.length, rows: page, offset, nextOffset: offset + page.length < rows.length ? offset + page.length : null }
}

export function projectSessionPanel(snapshot, {
  sourceKey, sessionId, selection = {}, readEvidence,
  observation, lifecycleId, retainedOnly = false,
}) {
  if ((selection.executionKey || selection.historyCursor) && selection.expectedThreadId !== sessionId) throw new Error('历史检查的会话身份已变化')
  const source = snapshot.sources.find(value => value.key === sourceKey)
  const registrationId = source?.descriptor.registrationId
  const native = source?.descriptor.policy === 'chatgpt-main/1'
  const sourceKeys = new Set(source ? native ? snapshot.sources
    .filter(value => value.descriptor.policy === 'chatgpt-main/1' && value.descriptor.registrationId === registrationId)
    .map(value => value.key) : [sourceKey] : [])
  const ownerTasks = snapshot.tasks.filter(task => sourceKeys.has(task.sourceKey))
  const readAt = snapshot.generatedAt
  const records = ownerTasks.flatMap(task => {
    if ((task.sessionId ?? task.id) !== sessionId) return []
    const events = native ? task.events.filter(event => event.taskId === task.id && event.taskLifecycleId === task.lifecycleId &&
      event.data?.runId === task.lifecycleId && event.data?.sourceKind === 'chatgpt-native' && !event.data.history && !event.data.inherited &&
      event.capturedAt > readAt - SESSION_HISTORY_MS) : task.events
    if (!events.length) return []
    const start = native && events.find(event => event.data.nativeType === 'task_started')
    const end = native && events.findLast(event => ['task_complete', 'turn_aborted'].includes(event.data.nativeType))
    const observedAt = native ? events.reduce((first, event) => Math.min(first, event.capturedAt), Infinity) : task.lastActivity ?? task.updatedAt
    const item = { key: task.key, sourceKey: task.sourceKey, lifecycleId: task.lifecycleId,
      status: end ? end.data.nativeType === 'turn_aborted' ? 'aborted' : 'ended' : task.status,
      startedAt: start?.occurredAt ?? null, observedAt, analysisState: task.analysis.state }
    return [{ task, item, order: item.startedAt ?? observedAt }]
  }).sort((a, b) => (b.order ?? 0) - (a.order ?? 0) || a.item.key.localeCompare(b.item.key))
  const current = records.find(({ task }) => task.sourceKey === sourceKey &&
    (lifecycleId ? task.lifecycleId === lifecycleId : task.status === 'running')) ?? records[0]
  const selected = selection.executionKey ? records.find(value => value.task.key === selection.executionKey) : current
  const cursorIndex = selection.historyCursor ? records.findIndex(value => value.task.key === selection.historyCursor) : -1
  const page = records.slice(cursorIndex + 1, cursorIndex + 21), hasMore = records.length > cursorIndex + 21
  const connected = source?.status === 'connected'
  const observed = connected && !retainedOnly && !observation?.observation?.recordRevalidating
  const paused = snapshot.monitoring?.paused
  const history = { executions: page.map(value => value.item), currentKey: current?.task.key ?? null, selectedKey: selected?.task.key ?? null,
    currentStatus: observed ? current?.task.status : 'unknown',
    currentSignal: observed && !paused && current?.task.analysis.state !== 'paused' ? current?.task.gs?.light ?? 'gray' : 'gray',
    cursor: cursorIndex >= 0 ? selection.historyCursor : null, nextCursor: hasMore ? page.at(-1).task.key : null,
    historyGap: Boolean(selection.historyCursor && cursorIndex < 0), hasMore, retentionMs: SESSION_HISTORY_MS,
    currentGap: retainedOnly ? observation?.observation?.gap ?? 'current-turn-not-observed' : !connected ? 'observer-unavailable' :
      current?.task.status === 'idle' ? 'chat-idle' : null }
  const identity = { ...observation?.identity, threadId: sessionId, title: observation?.identity?.title ?? current?.task.title }
  const recentModel = recentModelActivity(records.map(value => value.task), current?.task, registrationId, readAt, observed && !paused, ownerTasks, readEvidence)
  if (!selected) {
    const gap = selection.executionKey ? 'history-unavailable' : observation?.observation?.gap ?? (connected ? 'current-turn-not-observed' : 'observer-unavailable')
    const checking = observation?.observation?.coldReadStage || !selection.executionKey && observation?.observation?.recordRevalidating
    return { ...(checking ? observation : {
      signal: 'gray', summary: selection.executionKey ? '这条历史执行已不可读取' : '暂无本会话的执行记录',
      observation: { phase: 'unknown', status: 'unknown', coverage: 'unavailable', gap },
      analysis: { state: 'unknown', checks: [] },
    }), identity, history, recentModelActivity: recentModel }
  }
  const { task, item } = selected
  const ended = ['ended', 'aborted'].includes(item.status) || !native && ['idle', 'disposed'].includes(item.status)
  const historical = !connected || retainedOnly || selected !== current || ended
  const view = materialView({ ...observation, identity, signal: 'gray', summary: '当前状态不可判断',
    ...(historical ? { currentFacts: undefined, inheritance: undefined } : {}),
    observation: { ...(historical ? {} : observation?.observation), status: task.status,
      coverage: historical ? 'retained-history' : observation?.observation?.coverage ?? 'caught-up',
      gap: historical ? history.currentGap : observation?.observation?.gap ?? history.currentGap },
  }, task, { historical, snapshot: { ...snapshot, tasks: ownerTasks }, readEvidence })
  if (historical) {
    view.signal = 'gray'
    view.summary = history.currentGap === 'chat-idle' ? '会话空闲 · 显示上次检查结果，有新活动后自动监控' :
      history.currentGap ? '当前执行尚未确认 · 显示最近检查记录' : '历史执行 · 仅表示当时的检查记录'
    view.observation.phase = item.status === 'aborted' ? 'aborted' : ended ? 'completed' : 'unknown'
    if (native && !retainedOnly && connected) {
      view.observation.gap = task.events.find(event => event.kind === 'gap' && !event.data?.history && event.data?.verificationGaps?.length)?.data.verificationGaps[0]
        ?? (task.currentRequest ? null : 'historical-user-request-unverified')
    }
  }
  return { ...view, history, recentModelActivity: recentModel, execution: { ...item, request: (task.currentRequest ?? '').slice(0, 240), historical } }
}
