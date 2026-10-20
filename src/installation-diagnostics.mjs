import { failureErrorInfo } from './core/failure-receipts.mjs'

const states = new Set(['unknown', 'queued', 'analyzing', 'cooldown', 'idle', 'stale', 'failed', 'interrupted', 'unavailable', 'paused', 'observation-only'])
const time = value => Number.isSafeInteger(value) && value >= 0 ? value : null
const state = value => states.has(value) ? value : 'unknown'
const safeError = error => {
  if (!error) return null
  const safe = failureErrorInfo(error)
  return { stage: ['host-verification', 'core-connection', 'source-registration', 'material-collection', 'source-heartbeat'].includes(error.stage) ? error.stage : safe.stage,
    code: error.code == null ? null : ['unsupported-host-version', 'analyzer_unconfigured'].includes(error.code) ? error.code : safe.code }
}

export function projectInstallationDiagnostics(setting, snapshot, { collectionError = null, collectionState = null } = {}) {
  const { registrationId, config, revision } = setting
  const sources = snapshot.sources.filter(source => source.descriptor.registrationId === registrationId)
  const sourceKeys = new Set(sources.map(source => source.key)), tasks = snapshot.tasks.filter(task => sourceKeys.has(task.sourceKey))
  const active = tasks.filter(task => task.status === 'running'), counts = {}
  for (const task of active) { const name = state(task.analysis.state); counts[name] = (counts[name] ?? 0) + 1 }
  const order = task => Math.max(time(task.analysis.startedAt) ?? 0, time(task.analysis.completedAt) ?? 0)
  const latest = [...tasks].sort((a, b) => order(b) - order(a) || b.updatedAt - a.updatedAt)[0]?.analysis
  const failure = tasks.filter(task => task.analysis.state === 'failed' && task.analysis.error).sort((a, b) => order(b) - order(a))[0]?.analysis
  const collectionFailure = [...sources, ...tasks].flatMap(value => value.monitoringHealth?.failures ?? []).filter(value => value.domain === 'collection').sort((a, b) => b.updatedAt - a.updatedAt)[0]
  const error = safeError(collectionError ?? collectionFailure?.error)
  const connectedSources = sources.filter(source => source.status === 'connected').length
  const lastSeen = sources.map(source => time(source.lastSeenAt)).filter(value => value !== null)
  return {
    protocol: 1, registrationId, generatedAt: snapshot.generatedAt, configRevision: revision,
    models: Object.fromEntries(['llm', 'jev'].map(role => [role, { configured: Boolean(config[role]?.baseUrl && config[role]?.model) }])),
    collection: { state: collectionState ?? (error ? 'failed' : connectedSources ? 'connected' : sources.length ? 'disconnected' : 'unconfirmed'), sourceCount: sources.length, connectedSources, lastSeenAt: lastSeen.length ? Math.max(...lastSeen) : null, error },
    analysis: {
      counts: { scope: 'active-tasks', total: active.length, states: counts },
      latest: latest ? { state: state(latest.state), resultDisposition: ['none', 'current', 'pending', 'invalid'].includes(latest.resultDisposition) ? latest.resultDisposition : null,
        configRevision: latest.configScope?.registrationId === registrationId ? latest.configScope.revision : null,
        startedAt: time(latest.startedAt), completedAt: time(latest.completedAt), nextEligibleAt: time(latest.nextEligibleAt), error: safeError(latest.error) } : null,
      latestFailure: failure ? { ...safeError(failure.error), at: time(failure.completedAt) } : null,
    },
  }
}
