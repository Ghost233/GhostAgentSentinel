import { NOTIFICATION_BUNDLE_ID, notificationPrefix } from './native-notifications.mjs'

const permitted = value => ['authorized', 'provisional'].includes(value)
const safeError = error => ['native_unavailable', 'native_protocol', 'native_submission', 'native_timeout'].includes(error?.code) ? error.code : 'native_unavailable'

// One installation, one native child at a time. Refresh never requests permission.
export function createNotificationDelivery({ core, native, installationId, qualify, now = Date.now }) {
  if (!core || !native || typeof qualify !== 'function' || typeof installationId !== 'string' || !installationId) throw new Error('Notification delivery requires core, native IO and installation qualification')
  const prefix = notificationPrefix(installationId), sources = new Map(), refreshing = new Map()
  let stopped = false, stopping = null, queue = Promise.resolve(), authorizing = null
  let authorization = { authorization: 'unknown', lastCheckedAt: null, bundleId: NOTIFICATION_BUNDLE_ID, alertSetting: 'unknown', notificationCenterSetting: 'unknown', requestInFlight: false, errorCode: null }
  const serial = operation => { const pending = queue.then(operation); queue = pending.catch(() => {}); return pending }
  const cache = result => { authorization = { authorization: result.authorization, lastCheckedAt: now(), bundleId: NOTIFICATION_BUNDLE_ID, alertSetting: result.alertSetting ?? authorization.alertSetting, notificationCenterSetting: result.notificationCenterSetting ?? authorization.notificationCenterSetting, requestInFlight: false, errorCode: null } }
  const owned = id => typeof id === 'string' && id.startsWith(prefix)
  const packet = (notice, fields) => ({ notificationId: notice.id, attemptId: notice.native.attemptId, nativeRequestId: notice.native.nativeRequestId ?? prefix + notice.native.attemptId, bundleId: NOTIFICATION_BUNDLE_ID, observedAt: now(), ...fields })
  const sameScope = (notice, metadata) => metadata && ['sourceKey', 'hostRunId', 'mainId', 'lifecycleId'].every(key => metadata[key] === notice[key]) && metadata.notificationId === notice.id && metadata.attemptId === notice.native?.attemptId
  async function records(sourceKey) {
    const values = []; let cursor = 0, gap = false
    for (let page = 0; page < 11; page++) {
      const result = await core.nativeNotificationRecords({ sourceKey, afterSequence: cursor, limit: 100 })
      values.push(...result.records); gap ||= result.historyGap
      if (!result.hasMore) return { values, gap }
      cursor = result.nextSequence
    }
    return { values, gap: true }
  }
  async function record(notice, fields, summary) {
    try { return await core.recordNativeNotificationResult(packet(notice, fields)) }
    catch { summary.historyGap = true; return null }
  }
  async function observe(values, snapshot, summary) {
    summary.historyGap ||= snapshot.historyGap === true || snapshot.deliveredHasMore === true
    for (const notice of values) {
      if (!owned(notice.native?.nativeRequestId)) continue
      const delivered = snapshot.delivered.find(value => value.requestId === notice.native.nativeRequestId && sameScope(notice, value.metadata))
      if (delivered || !snapshot.deliveredHasMore) {
        const previous = notice.native.deliveredSnapshot
        if (!previous || previous.present !== Boolean(delivered) || delivered && previous.deliveryDate !== delivered.deliveryDate) await record(notice, { event: 'delivered-snapshot', present: Boolean(delivered), observedAt: snapshot.observedAt, ...(delivered ? { deliveryDate: delivered.deliveryDate } : {}) }, summary)
      }
      const action = snapshot.actions.filter(value => value.requestId === notice.native.nativeRequestId && sameScope(notice, value.metadata)).sort((a, b) => b.observedAt - a.observedAt)[0]
      if (action && (!notice.native.userAction || notice.native.userAction.observedAt < action.observedAt)) await record(notice, { event: 'user-action', action: action.action, observedAt: action.observedAt }, summary)
    }
  }
  async function cancel(values, snapshot, summary, all = false) {
    const ids = [...new Set(all ? snapshot.pending.map(value => value.requestId).filter(owned) : values.filter(value => owned(value.native?.nativeRequestId) && value.native.pendingCancellation?.pending !== false && (value.native.cancelRequestedAt || ['cancelled', 'expired', 'failed'].includes(value.status))).map(value => value.native.nativeRequestId))]
    for (let offset = 0; offset < Math.min(ids.length, 1100); offset += 100) {
      const batch = ids.slice(offset, offset + 100), result = await native.cancel(batch), pending = new Set(result.pendingIds)
      for (const notice of values) if (batch.includes(notice.native?.nativeRequestId)) await record(notice, { event: 'pending-cancelled', pending: pending.has(notice.native.nativeRequestId), observedAt: result.observedAt }, summary)
      summary.cancelled += batch.filter(id => !pending.has(id)).length
    }
    if (ids.length > 1100) summary.historyGap = true
  }
  async function refreshSource(sourceKey) {
    const summary = { submitted: 0, grouped: 0, suppressed: 0, failed: 0, cancelled: 0, historyGap: false, errorCode: null, stopped }
    if (stopped) return summary
    const state = sources.get(sourceKey), facts = state.facts, revision = state.revision
    const current = () => !stopped && sources.get(sourceKey)?.revision === revision
    await state.invalidation
    if (!current()) return summary
    await core.setNativeNotificationEligibility(facts)
    if (!current()) return summary
    const retained = await records(sourceKey); summary.historyGap ||= retained.gap
    try {
      if (facts.appRunning === true && facts.pluginEnabled === true && (authorization.lastCheckedAt === null || now() - authorization.lastCheckedAt >= 60_000)) cache(await native.settings())
      const snapshot = await native.snapshot()
      await observe(retained.values, snapshot, summary)
      await cancel(retained.values, snapshot, summary)
    } catch (error) { authorization = { ...authorization, authorization: 'unknown', errorCode: safeError(error) }; summary.errorCode = safeError(error); return summary }
    if (!current() || facts.appRunning !== true || facts.pluginEnabled !== true || !permitted(authorization.authorization)) return summary
    const notices = await core.claimNotifications(sourceKey)
    for (const notice of notices) {
      if (!notice.native?.attemptId || notice.hostRunId !== facts.hostRunId) { summary.historyGap = true; continue }
      let qualification
      try { qualification = await qualify(notice) } catch { qualification = null }
      if (qualification?.hostRunId === notice.hostRunId && (qualification.appRunning !== true || qualification.pluginEnabled !== true)) await core.setNativeNotificationEligibility({ sourceKey, hostRunId: qualification.hostRunId, appRunning: qualification.appRunning ?? null, pluginEnabled: qualification.pluginEnabled ?? null })
      let reason = stopped ? 'driver-stopped' : !current() || !qualification || qualification.hostRunId !== notice.hostRunId || qualification.appRunning !== true || qualification.pluginEnabled !== true ? 'qualification-unconfirmed' : notice.mainId !== null && qualification.projectEnabled !== true ? qualification.projectEnabled === false ? 'project-disabled' : 'project-unconfirmed' : null
      if (reason) { await record(notice, { event: 'suppressed', reason }, summary); summary.suppressed++; continue }
      await core.setNativeNotificationEligibility({ sourceKey, hostRunId: qualification.hostRunId, appRunning: qualification.appRunning, pluginEnabled: qualification.pluginEnabled })
      if (!current()) { await record(notice, { event: 'suppressed', reason: stopped ? 'driver-stopped' : 'qualification-unconfirmed' }, summary); summary.suppressed++; continue }
      await record(notice, { event: 'authorization', authorization: authorization.authorization, observedAt: authorization.lastCheckedAt }, summary)
      const intent = await record(notice, { event: 'submitting' }, summary)
      if (!intent?.maySubmit || !current()) {
        if (!current()) {
          await record(notice, { event: 'suppressed', reason: stopped ? 'driver-stopped' : 'qualification-unconfirmed' }, summary)
          if (intent?.maySubmit) await record(notice, { event: 'not-submitted' }, summary)
        }
        if (intent?.record?.status === 'grouped') summary.grouped++
        else summary.suppressed++
        continue
      }
      try {
        const result = await native.submit({ requestId: packet(notice, {}).nativeRequestId, title: notice.title, body: notice.body, metadata: { notificationId: notice.id, attemptId: notice.native.attemptId, sourceKey: notice.sourceKey, hostRunId: notice.hostRunId, mainId: notice.mainId, lifecycleId: notice.lifecycleId } })
        if (!current()) await record(notice, { event: 'suppressed', reason: stopped ? 'driver-stopped' : 'qualification-unconfirmed' }, summary)
        cache({ ...authorization, ...result })
        await record(notice, { event: 'authorization', authorization: result.authorization, observedAt: result.observedAt }, summary)
        if (result.accepted) { await record(notice, { event: 'accepted', observedAt: result.observedAt }, summary); summary.submitted++ }
        else { await record(notice, { event: 'suppressed', reason: 'qualification-unconfirmed', observedAt: result.observedAt }, summary); summary.suppressed++ }
      } catch (error) {
        if (!current()) await record(notice, { event: 'suppressed', reason: stopped ? 'driver-stopped' : 'qualification-unconfirmed' }, summary)
        await record(notice, { event: safeError(error) === 'native_submission' ? 'submission-failed' : 'submission-unknown', errorCode: safeError(error) }, summary); summary.failed++; summary.errorCode = safeError(error)
      }
    }
    return summary
  }
  return {
    authorization: () => ({ ...authorization, requestInFlight: authorizing !== null }),
    requestAuthorization() {
      if (stopped) return Promise.resolve({ ...authorization, errorCode: 'native_unavailable' })
      if (authorizing) return authorizing
      authorizing = serial(async () => {
        if (stopped) return { ...authorization, errorCode: 'native_unavailable' }
        try { cache(await native.requestAuthorization()) } catch (error) { authorization = { ...authorization, authorization: 'unknown', errorCode: safeError(error) } }
        return { ...authorization }
      }).finally(() => { authorizing = null })
      return authorizing
    },
    refresh(facts) {
      if (stopped) return Promise.resolve({ stopped: true })
      if (!sources.has(facts.sourceKey) && sources.size >= 256) return Promise.resolve({ errorCode: 'native_unavailable', historyGap: true })
      const normalized = { sourceKey: facts.sourceKey, hostRunId: facts.hostRunId, appRunning: facts.appRunning, pluginEnabled: facts.pluginEnabled }, previous = sources.get(facts.sourceKey)
      const changed = !previous || Object.keys(normalized).some(key => normalized[key] !== previous.facts[key])
      const state = { facts: normalized, revision: (previous?.revision ?? 0) + Number(changed), invalidation: previous?.invalidation }
      if (changed && (facts.appRunning !== true || facts.pluginEnabled !== true)) {
        native.cancelPendingOperations?.()
        state.invalidation = core.setNativeNotificationEligibility(normalized).catch(() => {})
      }
      sources.set(facts.sourceKey, state)
      if (refreshing.has(facts.sourceKey)) { const job = refreshing.get(facts.sourceKey); job.dirty = true; return job.promise }
      const job = { dirty: false, promise: null }
      const pending = serial(async () => { let result; do { job.dirty = false; result = await refreshSource(facts.sourceKey) } while (job.dirty && !stopped); return result }).finally(() => refreshing.delete(facts.sourceKey))
      job.promise = pending
      refreshing.set(facts.sourceKey, job)
      return pending
    },
    stop() {
      if (stopping) return stopping
      stopped = true; native.cancelPendingOperations?.()
      stopping = serial(async () => {
        const summary = { stopped: true, cancelled: 0, remainingPending: null, historyGap: false, errorCode: null }
        const values = []
        try {
          for (const state of sources.values()) {
            const facts = state.facts
            try { await state.invalidation; await core.setNativeNotificationEligibility({ ...facts, appRunning: null, pluginEnabled: null }); const retained = await records(facts.sourceKey); values.push(...retained.values); summary.historyGap ||= retained.gap }
            catch { summary.historyGap = true }
          }
          for (let page = 0; page < 11; page++) {
            const snapshot = await native.snapshot(); await observe(values, snapshot, summary)
            if (!snapshot.pending.length) { summary.remainingPending = 0; break }
            await cancel(values, snapshot, summary, true)
            if (page === 10) { const last = await native.snapshot(); summary.remainingPending = last.pending.length; summary.historyGap ||= last.pendingHasMore === true || last.pending.length > 0 }
          }
        } catch (error) { summary.errorCode = safeError(error); summary.historyGap = true }
        finally { native.close?.(); sources.clear() }
        return summary
      })
      return stopping
    },
  }
}
