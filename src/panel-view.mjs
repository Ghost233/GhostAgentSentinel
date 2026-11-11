function summaryEvent(task, reference, tasks) {
  const owner = tasks.find(candidate => candidate.sourceKey === task.sourceKey && candidate.id === reference.taskId && candidate.lifecycleId === reference.lifecycleId)
  return owner?.events.find(event => event.id === reference.eventId && event.taskId === reference.taskId && event.taskLifecycleId === reference.lifecycleId)
}

function blockingView(decision, task, tasks, readAt, historical) {
  if (!decision) return null
  const evidence = []
  for (const reference of decision.evidenceRefs) {
    const event = summaryEvent(task, reference, tasks)
    if (!event || !(event.capturedAt > readAt - 86400000 && event.capturedAt <= readAt)) return null
    evidence.push({ text: event.text.slice(0, 240), reference: event.evidence ?? JSON.stringify(reference) })
  }
  return { status: decision.status, reason: decision.reason, evidence, historical }
}

export function materialView(view, task, { historical = false, snapshot } = {}) {
  const states = { queued: '检查排队中', analyzing: '正在检查', unavailable: '本地摘取模型或 JEV 未配置', failed: '本轮分析失败', unknown: '当前请求或分析证据不足', cooldown: '本轮六项检查已更新', idle: '最近检查结果' }
  const findings = historical ? task.analysis.checks.filter(check => check.status === 'attention').map(check => ({ ...check, checkId: check.id })) : task.alerts.filter(alert => alert.status === 'attention')
  const issues = findings.map(alert => {
    const check = task.analysis.checks.find(check => check.id === alert.checkId)
    const confirmed = ['current', 'reused'].includes(task.analysis.resultDisposition) && !['failed', 'unavailable', 'interrupted', 'stale'].includes(task.analysis.state) && check?.status === 'attention' && !view.observation?.recordRevalidating
    const evidence = task.events.filter(event => alert.evidenceIds?.includes(event.id))
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
  const prior = task.analysis.lastSuccessfulSummary, readAt = snapshot?.generatedAt ?? Date.now()
  const ownerTasks = snapshot?.tasks ?? [task]
  const generatedView = (value, historicalDecision = historical || revalidating) => {
    if (!value) return null
    const blocking = blockingView(value.blocking, task, ownerTasks, readAt, historicalDecision)
    const evidence = value.evidenceRefs.slice(0, 2).map(reference => {
      const event = summaryEvent(task, reference, ownerTasks)
      return { text: event?.text?.slice(0, 240) ?? '引用材料当前不可读取', reference: event?.evidence ?? JSON.stringify(reference) }
    })
    return { text: value.text, evidence, ...(blocking ? { blocking } : {}) }
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

export function recentModelActivity(chatTasks, currentTask, registrationId, readAt, currentObserved, ownerTasks = chatTasks) {
  const cutoff = readAt - 86400000
  let prefill = null, summary = null
  for (const task of chatTasks) {
    const { lastPrefill, lastSuccessfulSummary, lastPartialSummary, lastUnjudgedSummary } = task.analysis
    if (lastPrefill?.submittedAt > cutoff && lastPrefill.submittedAt <= readAt && (!prefill || lastPrefill.submittedAt > prefill.submittedAt)) {
      prefill = { submittedAt: lastPrefill.submittedAt, state: lastPrefill.state }
    }
    const available = currentObserved && task === currentTask ? task.analysis.availableSummary : null
    for (const record of [available, lastSuccessfulSummary, lastPartialSummary, lastUnjudgedSummary]) {
      const result = record?.generated ?? record?.partial
      if (!result?.text || !(record.completedAt > cutoff && record.completedAt <= readAt && record.expiresAt > readAt)
        || summary && record.completedAt <= summary.completedAt || record.configScope && record.configScope.registrationId !== registrationId) continue
      if (!result.evidenceRefs?.length || !result.evidenceRefs.every(ref => ownerTasks.some(owner => owner.sourceKey === task.sourceKey
        && owner.id === ref.taskId && owner.lifecycleId === ref.lifecycleId && owner.events.some(event => event.id === ref.eventId && event.capturedAt > cutoff && event.capturedAt <= readAt)))) continue
      const current = record === available || currentObserved && task === currentTask && task.status === 'running' && record.completedAt === task.analysis.completedAt
        && ['current', 'reused'].includes(task.analysis.resultDisposition) && ['cooldown', 'idle'].includes(task.analysis.state)
        && !task.conversationMonitoring?.paused && task.monitoringHealth?.state !== 'failed'
      const blocking = blockingView(result.blocking, task, ownerTasks, readAt, !current)
      summary = { text: result.text, completedAt: record.completedAt, expiresAt: record.expiresAt, partial: Boolean(record.partial), gaps: record.gaps ?? [], ...(record.final ? { final: true } : {}), ...(blocking ? { blocking } : {}) }
    }
  }
  const attempt = currentTask?.analysis.prefill
  if (!prefill && attempt?.updatedAt > cutoff && attempt.updatedAt <= readAt) {
    if (attempt.state === 'unsupported') prefill = { state: attempt.reason === 'not-supported' ? 'unsupported' : 'unconfirmed' }
    if (attempt.state === 'completed') prefill = { state: 'time-unrecorded' }
  }
  return { prefill, summary }
}

