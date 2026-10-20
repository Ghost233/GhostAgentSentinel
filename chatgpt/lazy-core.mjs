import { connectCore } from '../src/core-client.mjs'
import { lstat } from 'node:fs/promises'
import { join } from 'node:path'
import { CHATGPT_RETENTION_MAINTENANCE_FEATURE, DEFAULT_RETENTION_MS } from '../src/protocol.mjs'

export async function retentionProfileSignature(dataDir) {
  const read = async path => { try { return await lstat(path) } catch (error) { if (error.code === 'ENOENT') return null; throw error } }
  const directory = await read(dataDir)
  if (!directory) return null
  const trusted = info => info.uid === process.getuid() && !(info.mode & 0o077) && !info.isSymbolicLink()
  if (!directory.isDirectory() || !trusted(directory)) throw Object.assign(new Error('Untrusted retention profile'), { code: 'retention_profile_untrusted' })
  const database = await read(join(dataDir, 'observations.sqlite'))
  if (!database) return null
  const wal = await read(join(dataDir, 'observations.sqlite-wal'))
  for (const file of [database, wal].filter(Boolean)) if (!file.isFile() || !trusted(file)) throw Object.assign(new Error('Untrusted retention storage'), { code: 'retention_profile_untrusted' })
  const identity = file => file ? [file.dev, file.ino, file.size, file.mtimeMs, file.ctimeMs] : null
  return JSON.stringify([identity(database), identity(wal?.size ? wal : null)])
}

// Collection attaches after qualification; existing profiles also permit explicit
// retention maintenance. Cleanup never starts or stops the shared server.
export function createLazyCore({ dataDir, connect = connectCore, onRetention } = {}) {
  let client = null, pending = null, closed = false, requestController = null, closing = null
  let maintenancePending = null, maintenanceController = null, maintenanceCleanupFailure = null
  const lifetime = new AbortController()
  const unavailable = () => Object.assign(new Error('Shared Core is in standby'), { code: 'core_standby' })
  const discard = async value => { if (client === value) client = null; await value?.close() }
  const health = signal => {
    if (closed) return Promise.reject(unavailable())
    if (pending) return pending
    const controller = new AbortController(); requestController = controller
    const combined = AbortSignal.any([lifetime.signal, controller.signal, ...(signal ? [signal] : [])])
    pending = (async () => {
      if (client) {
        const current = client
        try { return await current.health(combined) }
        catch (error) { combined.throwIfAborted(); await discard(current) }
      }
      const current = await connect({ dataDir, signal: combined })
      if (closed || combined.aborted) { await current.close(); combined.throwIfAborted(); throw unavailable() }
      client = current
      try { return await current.health(combined) }
      catch (error) { await discard(current); throw error }
    })().finally(() => { pending = null; if (requestController === controller) requestController = null })
    return pending
  }
  const methods = ['registerSource', 'heartbeat', 'nativeOriginFence', 'ingest', 'unregisterSource', 'snapshot', 'failureReceipts', 'recordCollectionOutcome', 'setConversationMonitoring', 'setNativeNotificationEligibility', 'claimNotifications', 'nativeNotificationRecords', 'recordNativeNotificationResult', 'installationAnalysisConfig', 'configureInstallationAnalysis', 'installationNotificationPreferences', 'setInstallationNotificationPreference', 'installationNotificationRecords']
  return {
    ...Object.fromEntries(methods.map(name => [name, async (...args) => {
      const current = client
      if (!current || closed) {
        if (['unregisterSource', 'setNativeNotificationEligibility'].includes(name)) return null
        if (name === 'snapshot') return { sources: [], tasks: [] }
        throw unavailable()
      }
      const result = await current[name](...args)
      if (name === 'unregisterSource' && onRetention) {
        try { onRetention(await current.diagnostics()) } catch (error) { onRetention(null, error) }
      }
      return result
    }])),
    health,
    maintenance: () => {
      if (closed) return Promise.reject(unavailable())
      if (maintenancePending) return maintenancePending
      const controller = new AbortController(); maintenanceController = controller
      const timer = setTimeout(() => controller.abort(new Error('Retention maintenance timed out')), 5000)
      const signal = AbortSignal.any([lifetime.signal, controller.signal])
      maintenancePending = (async () => {
        if (!await retentionProfileSignature(dataDir)) return null
        signal.throwIfAborted()
        let current, failure
        try {
          current = await connect({ dataDir, signal })
          signal.throwIfAborted()
          const value = await current.health(signal)
          if (!value.features?.includes(CHATGPT_RETENTION_MAINTENANCE_FEATURE)) throw Object.assign(new Error('Core retention maintenance capability unavailable'), { code: 'retention_capability_unavailable' })
          const diagnostics = await current.diagnostics(signal), next = diagnostics.retention?.nextExpiryAt
          if (diagnostics.retention?.retentionMs !== DEFAULT_RETENTION_MS || !Number.isSafeInteger(diagnostics.generatedAt) || next !== null && (!Number.isSafeInteger(next) || next <= diagnostics.generatedAt)) throw Object.assign(new Error('Core retention deadline is unconfirmed'), { code: 'retention_deadline_unconfirmed' })
          return { generatedAt: diagnostics.generatedAt, nextExpiryAt: next }
        } catch (error) { failure = error; throw error }
        finally {
          try { await current?.close() }
          catch (error) { maintenanceCleanupFailure = error; throw failure ? new AggregateError([failure, error], failure.message) : error }
        }
      })().finally(() => { clearTimeout(timer); maintenancePending = null; if (maintenanceController === controller) maintenanceController = null })
      return maintenancePending
    },
    cancelPending: async () => { requestController?.abort(new Error('Core attachment cancelled')); maintenanceController?.abort(new Error('Retention maintenance cancelled')); await Promise.allSettled([client?.cancelPending(), pending, maintenancePending]) },
    transportDiagnostics: () => ({ ...(client?.transportDiagnostics() ?? { pendingRequests: 0, deadlineTimers: 0 }), closed, connecting: Boolean(pending), maintenancePending: Boolean(maintenancePending) }),
    close: () => closing ??= (async () => {
      closed = true; lifetime.abort(unavailable())
      await Promise.allSettled([pending, maintenancePending])
      const current = client; client = null
      try { await current?.close() }
      catch (error) { throw maintenanceCleanupFailure ? new AggregateError([maintenanceCleanupFailure, error], 'Core client cleanup failed') : error }
      if (maintenanceCleanupFailure) throw maintenanceCleanupFailure
    })(),
  }
}
