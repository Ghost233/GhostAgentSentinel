import { connectCore } from '../src/core-client.mjs'

// Only an already-qualified health request may attach the shared Core. Cleanup
// uses the existing client and never starts or stops the shared server.
export function createLazyCore({ dataDir, connect = connectCore } = {}) {
  let client = null, pending = null, closed = false, requestController = null, closing = null
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
  const methods = ['registerSource', 'heartbeat', 'nativeOriginFence', 'ingest', 'unregisterSource', 'snapshot', 'failureReceipts', 'recordCollectionOutcome', 'setConversationMonitoring', 'setNativeNotificationEligibility', 'claimNotifications', 'nativeNotificationRecords', 'recordNativeNotificationResult']
  return {
    ...Object.fromEntries(methods.map(name => [name, async (...args) => {
      const current = client
      if (!current || closed) {
        if (['unregisterSource', 'setNativeNotificationEligibility'].includes(name)) return null
        if (name === 'snapshot') return { sources: [], tasks: [] }
        throw unavailable()
      }
      return current[name](...args)
    }])),
    health,
    cancelPending: async () => { requestController?.abort(new Error('Core attachment cancelled')); await Promise.allSettled([client?.cancelPending(), pending]) },
    transportDiagnostics: () => ({ ...(client?.transportDiagnostics() ?? { pendingRequests: 0, deadlineTimers: 0 }), closed, connecting: Boolean(pending) }),
    close: () => closing ??= (async () => {
      closed = true; lifetime.abort(unavailable())
      await Promise.allSettled([pending])
      const current = client; client = null
      await current?.close()
    })(),
  }
}
