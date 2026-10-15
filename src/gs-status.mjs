export const GS_FEATURE = 'gs-status/1'
const activityKinds = new Set(['output', 'tool-start', 'tool-result'])

function currentEvents(task, events) {
  const id = task.requestId ?? task.instructionContext?.binding?.inputRef?.eventId
  const index = id ? events.findIndex(event => event.kind === 'input' && event.id === id) : -1
  return (index >= 0 ? events.slice(index) : events.filter(event => task.requestStartedAt == null || event.capturedAt >= task.requestStartedAt)).filter(event => !event.data?.history)
}

function userWaitAudit(task, events, current) {
  const open = new Map(), periods = []
  if (task.status !== 'running') return { open, periods }
  for (const event of currentEvents(task, events)) {
    if (event.kind !== 'status') continue
    const type = event.data?.nativeType
    if (!['approval/asked', 'approval/decided', 'user-questions/request-start', 'user-questions/request-end'].includes(type)) continue
    const id = type?.startsWith('approval/') ? event.data.approvalId : event.data?.waitId
    if (!id) continue
    const key = `${type.startsWith('approval/') ? 'approval' : 'question'}/${id}`
    if (['approval/asked', 'user-questions/request-start'].includes(type) && !open.has(key)) open.set(key, event)
    if (['approval/decided', 'user-questions/request-end'].includes(type) && open.has(key)) {
      const start = open.get(key); periods.push({ start, endAt: event.capturedAt }); open.delete(key)
    }
  }
  return { open, periods: [...periods, ...[...open.values()].map(start => ({ start, endAt: current }))] }
}

export function userWait(task, events, current) {
  const { open } = userWaitAudit(task, events, current)
  const start = [...open.values()].at(-1)
  return start ? { kind: 'user', since: start.capturedAt, durationMs: Math.max(0, current - start.capturedAt), evidenceIds: [start.id], callId: start.data.callId ?? null } : null
}

function userWaitIntervals(task, events, current, since, callId) {
  return userWaitAudit(task, events, current).periods.filter(period => callId === undefined || period.start.data.callId === callId).map(period => [Math.max(since, period.start.capturedAt), Math.min(current, period.endAt)]).filter(([start, end]) => end > start).sort((a, b) => a[0] - b[0])
}

export function userWaitDuration(task, events, current, since, callId) {
  const intervals = userWaitIntervals(task, events, current, since, callId)
  let total = 0, through = since
  for (const [start, end] of intervals) { total += Math.max(0, end - Math.max(through, start)); through = Math.max(through, end) }
  return total
}

export function userWaitThresholdAt(task, events, current, since, waitMs, callId) {
  let threshold = since + waitMs, through = since
  for (const [start, end] of userWaitIntervals(task, events, current, since, callId)) {
    if (start >= threshold) break
    threshold += Math.max(0, end - Math.max(through, start)); through = Math.max(through, end)
  }
  return threshold
}

export function waitExempts(check, waiting, events) {
  if (check.checkId !== 'long_wait' && check.id !== 'long_wait' || waiting?.kind !== 'user') return false
  if (!Number.isFinite(check.observedAt) || check.observedAt <= waiting.since) return false
  const evidence = (check.evidenceIds ?? []).map(id => events.find(event => event.id === id)).filter(Boolean)
  return evidence.length > 0 && evidence.every(event => event.kind === 'tool-start' ? waiting.callId && event.data?.callId === waiting.callId : event.capturedAt <= waiting.since)
}

// GS is a core projection. Activity observes execution; it does not certify progress.
export function childWait(task, source, tasks, current) {
  if (source.status !== 'connected' || task.status !== 'running') return null
  const context = source.hierarchyContext
  const bindings = (context?.waitBindings ?? []).filter(binding => binding.taskId === task.id && binding.lifecycleId === task.lifecycleId && binding.phase === 'started')
  for (const binding of bindings.reverse()) {
    const start = binding.evidenceRefs.map(ref => currentEvents(task, task.events).find(event => event.taskId === ref.taskId && event.taskLifecycleId === ref.lifecycleId && event.id === ref.eventId)).find(event => event?.data?.nativeType === 'execution/wait-start' && event.data.waitId === binding.waitId)
    if (!start || task.events.some(event => event.data?.nativeType === 'execution/wait-end' && event.data.waitId === binding.waitId && event.capturedAt >= start.capturedAt)) continue
    const active = binding.childRefs.filter(reference => {
      const child = tasks.find(child => child.sourceKey === task.sourceKey && child.id === reference.taskId && child.lifecycleId === reference.lifecycleId)
      const activation = context.activations.find(item => item.taskId === reference.taskId && item.lifecycleId === reference.lifecycleId)
      return child?.status === 'running' && child.currentRequest?.trim() && activation?.state === 'running' && child.coverage.complete && currentEvents(child, child.events).some(event => activityKinds.has(event.kind) && !event.data?.refinesEventId && current - event.capturedAt < 60_000)
    })
    if (active.length) return { kind: 'children', since: start.capturedAt, durationMs: Math.max(0, current - start.capturedAt), evidenceIds: [start.id], childRefs: active }
  }
  return null
}

export function gsStatus(task, source, current, descendantWait = null) {
  const active = task.status === 'running' || task.status === 'unknown' && task.lastKnownStatus === 'running'
  const scope = currentEvents(task, task.events)
  const latest = scope.filter(event => activityKinds.has(event.kind) && !event.data?.refinesEventId).at(-1)
  const silence = { since: latest?.capturedAt ?? null, durationMs: latest ? Math.max(0, current - latest.capturedAt) : null, initial: !latest }
  const coverage = { complete: task.coverage.complete, gaps: task.coverage.gaps }
  const waiting = source.status === 'connected' ? userWait(task, scope, current) ?? descendantWait : null
  const requestId = task.requestId ?? task.instructionContext?.binding?.inputRef?.eventId
  const completed = task.analysis.completedAt != null && requestId != null && task.analysis.inputEventIds?.includes(requestId) && (!task.analysis.resultDisposition || ['current', 'reused'].includes(task.analysis.resultDisposition))
  const semanticGap = task.analysis.checks.some(check => check.judgmentState === 'failed' || check.id !== 'long_wait' && check.applicability?.state === 'triggered' && check.status === 'unknown') || completed && task.analysis.coverage?.complete === false
  if (waiting) waiting.semanticState = semanticGap ? 'insufficient' : completed ? 'complete' : 'pending'
  const base = { silence: { ...silence, exempt: Boolean(waiting) }, waiting, coverage }
  if (!active) return { light: null, label: task.status === 'unknown' ? '状态未知' : '已结束', reason: '不计入活动执行节点', ...base }
  if (source.status !== 'connected') return { light: 'gray', label: '不可判断', reason: '来源失联；不能把缺失消息当作原代理静默', ...base }
  const alerts = task.alerts.filter(alert => alert.status === 'attention' && !waitExempts(alert, waiting, scope))
  if (alerts.length) return { light: 'red', label: '需要关注', reason: alerts.map(alert => alert.reason).join('；'), ...base }
  const activation = source.hierarchyContext?.activations.find(item => item.taskId === task.id && item.lifecycleId === task.lifecycleId)
  if (task.status === 'unknown' || source.hierarchyContext && (!activation || activation.state === 'unknown' || activation.sessionId === null)) return { light: 'gray', label: '不可判断', reason: '已知执行当前不可观察；不能凭缺失内部材料判断静默', ...base }
  if (waiting && task.coverage.complete && !task.instructionContext?.gaps?.length && !semanticGap) return { light: 'green', label: waiting.kind === 'user' ? '等待你' : '等待子代理', reason: (waiting.kind === 'user' ? '已观察到当前执行的用户等待开闭依据' : '已核对父等待审计及仍有活动的委派后代；下级状态独立保留') + (waiting.semanticState === 'pending' ? '；六项语义尚待完成' : ''), ...base }
  if (!waiting && (!latest || silence.durationMs >= 60_000)) return { light: 'yellow', label: '观察', reason: !latest ? '尚无可见执行活动' : `已 ${Math.floor(silence.durationMs / 1000)} 秒无新可见执行活动；是否卡死不可判断`, ...base }
  const complete = task.coverage.complete && task.analysis.coverage?.complete && !task.instructionContext?.gaps?.length && !task.analysis.error && !task.analysis.coverage?.dependencies?.length && task.analysis.checks.every(check => check.applicability?.state === 'not-triggered' || check.applicability?.state === 'triggered' && check.status === 'clear')
  if (complete && (!task.analysis.resultDisposition || task.analysis.resultDisposition !== 'invalid')) return { light: 'green', label: task.analysis.resultDisposition === 'pending' ? '上轮正常 · 待分析' : '正常', reason: task.analysis.resultDisposition === 'pending' ? '上轮正常判断仍适用于同一请求与执行范围；新增材料尚未完成分析' : '适用检查未见疑点；有据的未触发项不影响灯号', ...base }
  return { light: 'gray', label: '不可判断', reason: waiting ? `${waiting.kind === 'user' ? '等待你' : '等待子代理'}依据保留；其他判断或关键材料不足` : '尚无足够的当前判断依据', ...base }
}
