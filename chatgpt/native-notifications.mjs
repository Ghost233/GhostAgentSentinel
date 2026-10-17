import { execFile } from 'node:child_process'
import { createHash } from 'node:crypto'
import { isAbsolute, join } from 'node:path'

export const NOTIFICATION_BUNDLE_ID = 'com.ghostsentinel.notifications'
export const notificationPrefix = installationId => 'ghostsentinel:' + createHash('sha256').update(String(installationId)).digest('hex').slice(0, 24) + ':'
const protocol = () => Object.assign(new Error('Native notification protocol response is invalid'), { code: 'native_protocol' })
const id = value => typeof value === 'string' && value.length > 0 && value.length <= 256 && !/[\x00-\x1f\x7f]/.test(value)
const auth = new Set(['unknown', 'not-determined', 'denied', 'authorized', 'provisional'])
const setting = new Set(['not-supported', 'disabled', 'enabled', 'unknown'])
const time = value => Number.isSafeInteger(value) && value >= 0
function metadata(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some(key => !['notificationId', 'attemptId', 'sourceKey', 'hostRunId', 'mainId', 'lifecycleId'].includes(key))) throw protocol()
  if (!['notificationId', 'attemptId', 'sourceKey', 'hostRunId'].every(key => id(value[key])) || ![value.mainId, value.lifecycleId].every(value => value === null || id(value)) || value.mainId === null && value.lifecycleId !== null) throw protocol()
  return { ...value }
}
function runNativeProcess(executable, args, input, { timeoutMs, maxBuffer, signal }) {
  return new Promise((resolve, reject) => {
    const child = execFile(executable, args, { timeout: timeoutMs, maxBuffer, signal, encoding: 'utf8', shell: false, windowsHide: true }, (error, stdout) => {
      if (error) reject(Object.assign(new Error('Native notification component did not return a usable response'), { code: error.killed ? 'native_timeout' : error.code === 'ERR_CHILD_PROCESS_STDIO_MAXBUFFER' ? 'native_protocol' : 'native_unavailable' }))
      else resolve({ stdout })
    })
    child.stdin.once('error', () => { child.kill(); reject(Object.assign(new Error('Native notification input could not be delivered'), { code: 'native_unavailable' })) })
    child.stdin.end(input)
  })
}

// Only this installation's identifiers cross the fixed stdin/stdout protocol.
export function createNativeNotificationIO({ bundlePath, installationId, run = runNativeProcess }) {
  if (typeof bundlePath !== 'string' || !isAbsolute(bundlePath) || !bundlePath.endsWith('.app') || !id(installationId)) throw new Error('native notification bundle and installation identity are required')
  const executable = join(bundlePath, 'Contents/MacOS/GhostSentinelNotifications'), ownerPrefix = notificationPrefix(installationId)
  let active = null, closed = false
  const owned = value => id(value) && value.length <= 200 && value.startsWith(ownerPrefix) && value.length > ownerPrefix.length
  async function invoke(operation, fields = {}) {
    if (closed || active) throw Object.assign(new Error('Native notification component is unavailable'), { code: 'native_unavailable' })
    const input = JSON.stringify({ schemaVersion: 1, operation, ownerPrefix, ...fields }) + '\n'
    if (Buffer.byteLength(input) > 131072) throw protocol()
    const controller = new AbortController()
    active = controller
    let response
    try { response = await run(executable, ['--stdio'], input, { timeoutMs: operation === 'authorize' ? 60000 : 5000, maxBuffer: 262144, signal: controller.signal }) }
    catch (error) { throw Object.assign(new Error('Native notification component failed'), { code: ['native_unavailable', 'native_protocol', 'native_timeout', 'native_submission'].includes(error.code) ? error.code : 'native_unavailable' }) }
    finally { active = null }
    if (typeof response?.stdout !== 'string' || Buffer.byteLength(response.stdout) > 262144) throw protocol()
    let packet
    try { packet = JSON.parse(response.stdout) } catch { throw protocol() }
    if (packet?.schemaVersion !== 1 || packet.bundleId !== NOTIFICATION_BUNDLE_ID || packet.operation !== operation || typeof packet.ok !== 'boolean') throw protocol()
    if (!packet.ok) throw Object.assign(new Error('Native notification operation failed'), { code: ['native_unavailable', 'native_protocol', 'native_submission', 'native_timeout'].includes(packet.error?.code) ? packet.error.code : 'native_protocol' })
    const result = packet.result
    if (!result || typeof result !== 'object' || Array.isArray(result) || !time(result.observedAt)) throw protocol()
    if (['status', 'authorize'].includes(operation)) {
      if (!auth.has(result.authorization) || !setting.has(result.alertSetting) || !setting.has(result.notificationCenterSetting)) throw protocol()
    } else if (operation === 'submit') {
      if (result.requestId !== fields.requestId || typeof result.accepted !== 'boolean' || !auth.has(result.authorization)) throw protocol()
    } else if (operation === 'cancel') {
      if (!Array.isArray(result.pendingIds) || result.pendingIds.length > 100 || result.pendingIds.some(value => !owned(value))) throw protocol()
    } else if (operation === 'purge-installation') {
      if (!Number.isSafeInteger(result.requestedCancellations) || result.requestedCancellations < 0 || result.requestedCancellations > 100 || !Number.isSafeInteger(result.remainingPending) || result.remainingPending < 0 || !Number.isSafeInteger(result.clearedActions) || result.clearedActions < 0 || result.clearedActions > 256 || result.deliveredPreserved !== true || result.complete !== (result.remainingPending === 0)) throw protocol()
    } else {
      if (!['pending', 'delivered', 'actions'].every(key => Array.isArray(result[key]) && result[key].length <= 100)) throw protocol()
      for (const value of result.pending) if (!owned(value.requestId)) throw protocol()
      for (const value of result.delivered) if (!owned(value.requestId) || !time(value.deliveryDate) || !metadata(value.metadata)) throw protocol()
      for (const value of result.actions) if (!owned(value.requestId) || !id(value.observationId) || !['default', 'dismiss'].includes(value.action) || !time(value.observedAt) || !metadata(value.metadata)) throw protocol()
    }
    return { ...result, bundleId: NOTIFICATION_BUNDLE_ID }
  }
  return {
    settings: () => invoke('status'),
    requestAuthorization: () => invoke('authorize'),
    submit(value) {
      if (!owned(value?.requestId)) return Promise.reject(new Error('notification request belongs to another installation'))
      if (typeof value.title !== 'string' || !value.title.trim() || value.title.length > 200 || typeof value.body !== 'string' || value.body.length > 1200) return Promise.reject(protocol())
      return invoke('submit', { requestId: value.requestId, title: value.title, body: value.body, metadata: metadata(value.metadata) })
    },
    cancel(requestIds) {
      if (!Array.isArray(requestIds) || requestIds.length > 100 || requestIds.some(value => !owned(value))) return Promise.reject(new Error('notification cancellation belongs to another installation or exceeds its budget'))
      return invoke('cancel', { requestIds: [...new Set(requestIds)] })
    },
    snapshot: () => invoke('snapshot'),
    purgeInstallation: () => invoke('purge-installation'),
    cancelPendingOperations: () => active?.abort(),
    close: () => { closed = true; active?.abort() },
  }
}
