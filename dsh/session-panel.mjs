import { materialView, recentModelActivity } from '../src/panel-view.mjs'

const DAY = 86_400_000

export function projectDSHSessionPanel(snapshot, sourceKey, sessionId, selection = {}) {
  if ((selection.executionKey || selection.historyCursor) && selection.expectedThreadId !== sessionId) throw new Error('历史检查的会话身份已变化')
  const source = snapshot.sources.find(value => value.key === sourceKey)
  const ownerTasks = source ? snapshot.tasks.filter(task => task.sourceKey === sourceKey) : []
  const tasks = ownerTasks.filter(task => task.sessionId === sessionId && task.events.length)
  const connected = source?.status === 'connected'
  const readAt = snapshot.generatedAt
  const records = tasks.map(task => ({ task, item: { key: task.key, status: task.status, observedAt: task.lastActivity ?? task.updatedAt, analysisState: task.analysis.state } }))
    .sort((a, b) => (b.item.observedAt ?? 0) - (a.item.observedAt ?? 0) || a.item.key.localeCompare(b.item.key))
  const current = records.find(value => value.task.status === 'running') ?? records[0]
  const selected = selection.executionKey ? records.find(value => value.task.key === selection.executionKey) : current
  const cursorIndex = selection.historyCursor ? records.findIndex(value => value.task.key === selection.historyCursor) : -1
  const page = records.slice(cursorIndex + 1, cursorIndex + 21), hasMore = records.length > cursorIndex + 21
  const history = { executions: page.map(value => value.item), currentKey: current?.task.key ?? null, selectedKey: selected?.task.key ?? null,
    currentStatus: connected ? current?.task.status : 'unknown', currentSignal: connected ? current?.task.gs?.light : 'gray',
    cursor: cursorIndex >= 0 ? selection.historyCursor : null, nextCursor: hasMore ? page.at(-1).task.key : null, historyGap: Boolean(selection.historyCursor && cursorIndex < 0),
    currentGap: connected ? current?.task.status === 'idle' ? 'chat-idle' : null : 'observer-unavailable', retentionMs: DAY }
  const identity = { threadId: sessionId, title: current?.task.title }
  const recent = recentModelActivity(tasks, current?.task, source?.descriptor.registrationId, readAt, connected && !snapshot.monitoring?.paused, ownerTasks)
  if (!selected) return { identity, signal: 'gray', summary: selection.executionKey ? '这条历史执行已不可读取' : '暂无本会话的执行记录',
    observation: { status: 'unknown', gap: connected ? 'current-turn-not-observed' : 'observer-unavailable' },
    analysis: { state: 'unknown', checks: [] }, history, recentModelActivity: recent }
  const { task, item } = selected
  const historical = !connected || selected !== current || task.status !== 'running'
  const view = materialView({ identity, signal: 'gray', summary: '当前状态不可判断',
    observation: { status: task.status, coverage: historical ? 'retained-history' : 'caught-up', gap: history.currentGap } }, task, { historical, snapshot: { ...snapshot, tasks: ownerTasks } })
  return { ...view, history, recentModelActivity: recent, execution: { ...item, request: task.currentRequest ?? '', historical } }
}
