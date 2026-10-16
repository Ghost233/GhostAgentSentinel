import { normalizeNativeOrigins } from './native-origin.mjs'
import { normalizeForkReceipt } from './fork-proof.mjs'
export const PROTOCOL = 1
export const GS_SUMMARY_FEATURE = 'gs-summary/1'
export const SOURCE_OBSERVATION_FEATURE = 'source-observation/1'
export const CHATGPT_MAIN_FEATURE = 'chatgpt-main/1'
export const CONVERSATION_MONITORING_FEATURE = 'conversation-monitoring/1'
export const MONITORING_FAILURES_FEATURE = 'monitoring-failures/1'
export const MONITORING_NOTIFICATIONS_FEATURE = 'monitoring-notifications/1'
export const CHATGPT_INSTALLATION_PURGE_FEATURE = 'chatgpt-installation-purge/1'
export const CHATGPT_RETENTION_MAINTENANCE_FEATURE = 'chatgpt-retention-maintenance/1'
export const DEFAULT_RETENTION_MS = 24 * 60 * 60 * 1000
const kinds = new Set(['input', 'output', 'tool-start', 'tool-result', 'status', 'gap'])
const statuses = new Set(['running', 'idle', 'disposed', 'unknown'])
const dataStrings = ['callId', 'rootCallId', 'parentCallId', 'toolName', 'command', 'errorCode', 'sourceVersion', 'validationScope', 'status', 'cwd', 'resultSource', 'signal', 'refinesEventId', 'nativeType', 'childSessionId', 'runId', 'turnTrigger', 'inputOrigin', 'messageId', 'inputPhase', 'sourceKind', 'sourceForm', 'senderSessionId', 'approvalId', 'waitId', 'evidenceKind', 'forkThreadId', 'sourceThreadId', 'sourceOwnerThreadId', 'originRunId', 'historyOwnerThreadId']

function object(value, field) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${field} must be an object`)
  return value
}
function string(value, field, max = 500) {
  if (typeof value !== 'string' || !value.trim() || value.length > max) throw new Error(`${field} must be a nonempty string (max ${max})`)
  return value
}
export function normalizeChatGPTInstallationPurge(value) {
  object(value, 'ChatGPT installation purge')
  if (Object.keys(value).length !== 1 || !Object.hasOwn(value, 'installationId')) throw new Error('ChatGPT installation purge requires only installationId')
  return { installationId: string(value.installationId, 'installationId', 1992) }
}
function strings(value, field) {
  if (!Array.isArray(value) || value.length > 100) throw new Error(`${field} must be a bounded list`)
  return value.map(item => string(item, field))
}
function time(value, field) {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${field} must be an epoch millisecond integer`)
  return value
}

export function normalizeConversationMonitoring(value, control = false) {
  const input = object(value, 'conversation monitoring')
  const allowed = control ? ['registrationId', 'taskId', 'paused', 'revision'] : ['registrationId', 'taskId']
  if (Object.keys(input).some(key => !allowed.includes(key))) throw new Error('unsupported conversation monitoring field')
  const result = { registrationId: string(input.registrationId, 'registrationId'), taskId: string(input.taskId, 'taskId') }
  if (control) {
    if (typeof input.paused !== 'boolean') throw new Error('paused must be boolean')
    result.paused = input.paused
    result.revision = string(input.revision, 'revision', 100)
  }
  return result
}

export function normalizeFailureScope(value) {
  const input = object(value, 'failure receipt scope')
  if (Object.keys(input).some(key => !['sourceKey', 'mainId', 'lifecycleId', 'afterSequence', 'limit'].includes(key))) throw new Error('unsupported failure receipt scope field')
  const result = { sourceKey: string(input.sourceKey, 'sourceKey', 256) }
  for (const key of ['mainId', 'lifecycleId']) if (input[key] !== undefined) result[key] = input[key] === null ? null : string(input[key], key, 256)
  if (input.afterSequence !== undefined) result.afterSequence = time(input.afterSequence, 'afterSequence')
  if (input.limit !== undefined) {
    if (!Number.isSafeInteger(input.limit) || input.limit < 1 || input.limit > 100) throw new Error('failure receipt limit must be between 1 and 100')
    result.limit = input.limit
  }
  return result
}

export function normalizeCollectionOutcome(value) {
  const input = object(value, 'collection outcome')
  if (Object.keys(input).some(key => !['sourceKey', 'mainId', 'lifecycleId', 'roundId', 'channel', 'outcome', 'settledAt', 'error'].includes(key))) throw new Error('unsupported collection outcome field')
  if (!['failed', 'succeeded'].includes(input.outcome)) throw new Error('invalid collection outcome')
  const result = { sourceKey: string(input.sourceKey, 'sourceKey', 256), roundId: string(input.roundId, 'roundId', 256), outcome: input.outcome, settledAt: time(input.settledAt, 'settledAt') }
  for (const key of ['mainId', 'lifecycleId']) result[key] = input[key] === null ? null : string(input[key], key, 256)
  if (result.mainId === null && result.lifecycleId !== null) throw new Error('collection lifecycle requires a main identity')
  if (input.channel !== undefined) {
    const channel = string(input.channel, 'channel', 200)
    if (!/^(default|directory|main:[a-f0-9]{64})$/.test(channel)) throw new Error('invalid collection channel')
    result.channel = channel
  }
  if (input.outcome === 'failed') result.error = input.error === undefined ? { code: 'collection_failed' } : object(input.error, 'collection error')
  return result
}

export function normalizeNativeNotificationEligibility(value) {
  const input = object(value, 'native notification eligibility')
  if (Object.keys(input).some(key => !['sourceKey', 'hostRunId', 'appRunning', 'pluginEnabled'].includes(key))) throw new Error('unsupported native notification eligibility field')
  const result = { sourceKey: string(input.sourceKey, 'sourceKey', 256), hostRunId: string(input.hostRunId, 'hostRunId', 256) }
  for (const key of ['appRunning', 'pluginEnabled']) {
    if (input[key] !== null && typeof input[key] !== 'boolean') throw new Error(`${key} must be boolean or null`)
    result[key] = input[key]
  }
  return result
}

export function normalizeNativeNotificationQuery(value) {
  const input = object(value, 'native notification query')
  if (Object.keys(input).some(key => !['sourceKey', 'afterSequence', 'limit'].includes(key))) throw new Error('unsupported native notification query field')
  const result = { sourceKey: string(input.sourceKey, 'sourceKey', 256) }
  if (input.afterSequence !== undefined) result.afterSequence = time(input.afterSequence, 'afterSequence')
  if (input.limit !== undefined) {
    if (!Number.isSafeInteger(input.limit) || input.limit < 1 || input.limit > 100) throw new Error('native notification limit must be between 1 and 100')
    result.limit = input.limit
  }
  return result
}

export function normalizeNativeNotificationResult(value) {
  const input = object(value, 'native notification result')
  const fields = { authorization: ['authorization'], submitting: [], accepted: [], 'submission-failed': ['errorCode'], 'submission-unknown': ['errorCode'], suppressed: ['reason'], 'pending-cancelled': ['pending'], 'delivered-snapshot': ['present', 'deliveryDate'], 'user-action': ['action'] }
  if (!Object.hasOwn(fields, input.event)) throw new Error('unsupported native notification event')
  const allowed = ['notificationId', 'attemptId', 'nativeRequestId', 'bundleId', 'event', 'observedAt', ...fields[input.event]]
  if (Object.keys(input).some(key => !allowed.includes(key))) throw new Error('unsupported native notification result field')
  if (input.bundleId !== 'com.ghostsentinel.notifications') throw new Error('unsupported native notification identity')
  const result = { notificationId: string(input.notificationId, 'notificationId', 256), attemptId: string(input.attemptId, 'attemptId', 128), nativeRequestId: string(input.nativeRequestId, 'nativeRequestId', 200), bundleId: input.bundleId, event: input.event, observedAt: time(input.observedAt, 'observedAt') }
  if (input.event === 'authorization') {
    if (!['unknown', 'not-determined', 'denied', 'authorized', 'provisional'].includes(input.authorization)) throw new Error('invalid native notification authorization')
    result.authorization = input.authorization
  }
  if (input.errorCode !== undefined) {
    if (!['native_unavailable', 'native_protocol', 'native_submission', 'native_timeout'].includes(input.errorCode)) throw new Error('invalid native notification error code')
    result.errorCode = input.errorCode
  }
  if (input.event === 'suppressed') {
    if (!['project-disabled', 'project-unconfirmed', 'qualification-unconfirmed', 'driver-stopped'].includes(input.reason)) throw new Error('invalid native notification suppression reason')
    result.reason = input.reason
  }
  if (input.event === 'pending-cancelled' || input.event === 'delivered-snapshot') {
    const key = input.event === 'pending-cancelled' ? 'pending' : 'present'
    if (typeof input[key] !== 'boolean') throw new Error(`${key} must be boolean`)
    result[key] = input[key]
    if (input.deliveryDate !== undefined) result.deliveryDate = time(input.deliveryDate, 'deliveryDate')
  }
  if (input.event === 'user-action') {
    if (!['default', 'dismiss'].includes(input.action)) throw new Error('invalid native notification action')
    result.action = input.action
  }
  return result
}

export function normalizeSource(value) {
  const input = object(value, 'source')
  const result = { registrationId: string(input.registrationId, 'registrationId'), hostRunId: string(input.hostRunId, 'hostRunId'), name: string(input.name, 'name') }
  if (input.mode !== undefined) {
    if (input.mode !== 'observation-only') throw new Error('unsupported source observation mode')
    result.mode = input.mode
  }
  if (input.policy !== undefined) {
    if (input.policy !== CHATGPT_MAIN_FEATURE) throw new Error('unsupported source observation policy')
    result.policy = input.policy
  }
  if (input.nativeOriginVersion !== undefined) { if (input.policy !== CHATGPT_MAIN_FEATURE || input.nativeOriginVersion !== 2) throw new Error('invalid native origin version'); result.nativeOriginVersion = 2 }
  if (input.profile !== undefined) result.profile = string(input.profile, 'profile', 2000)
  if (input.enroll !== undefined) {
    if (typeof input.enroll !== 'boolean') throw new Error('enroll must be boolean')
    result.enroll = input.enroll
  }
  for (const field of ['capabilities', 'gaps']) if (input[field] !== undefined) result[field] = strings(input[field], field)
  return result
}

export function normalizeBatch(value) {
  const input = object(value, 'batch')
  const result = {}
  if (input.nativeOrigins !== undefined) result.nativeOrigins = normalizeNativeOrigins(input.nativeOrigins)
  if (input.tasks !== undefined) {
    if (!Array.isArray(input.tasks) || input.tasks.length > 2000) throw new Error('tasks must be a bounded list')
    result.tasks = input.tasks.map(item => {
      object(item, 'task')
      if (!statuses.has(item.status)) throw new Error('invalid task status')
      const title = sanitizeObservation(string(item.title, 'task.title', 2000))
      const task = { id: string(item.id, 'task.id'), lifecycleId: string(item.lifecycleId, 'task.lifecycleId'), title: title.text, status: item.status }
      for (const field of ['sessionId', 'parentId']) if (item[field] !== undefined && item[field] !== null) task[field] = string(item[field], field)
      if (item.gaps !== undefined) task.gaps = strings(item.gaps, 'task.gaps')
      if (title.redacted) task.gaps = [...(task.gaps ?? []), '任务名称中认证材料已省略']
      return task
    })
  }
  if (input.events !== undefined) {
    if (!Array.isArray(input.events) || input.events.length > 1000) throw new Error('events must be a bounded list')
    result.events = input.events.map(item => {
      object(item, 'event')
      if (!kinds.has(item.kind)) throw new Error('invalid event kind')
      if (typeof item.text !== 'string') throw new Error('event.text must be a string')
      const excerpt = sanitizeObservation(item.text)
      const command = sanitizeObservation(item.data?.command)
      let redacted = excerpt.redacted || command.redacted || item.data?.redacted === true
      const event = { id: string(item.id, 'event.id'), taskId: string(item.taskId, 'taskId'), taskLifecycleId: string(item.taskLifecycleId, 'taskLifecycleId'),
        kind: item.kind, occurredAt: item.occurredAt === null ? null : time(item.occurredAt, 'occurredAt'), capturedAt: time(item.capturedAt, 'capturedAt'), text: (redacted ? '认证材料已省略，当前证据不完整' : excerpt.text).slice(0, 240) }
      if (item.evidence !== undefined) {
        const reference = sanitizeObservation(string(item.evidence, 'evidence', 2000))
        event.evidence = reference.text
        if (reference.redacted) redacted = true
      }
      if (item.data !== undefined) {
        object(item.data, 'event.data')
        for (const field of ['forkReceiptId', 'forkGeneration', 'historyGeneration', 'nativeRecordId', 'nativeGenerationKey']) if (item.data[field] !== undefined && (typeof item.data[field] !== 'string' || !/^[a-f0-9]{64}$/.test(item.data[field]))) throw new Error('invalid fork proof identity')
        event.data = { ...event.data }
        for (const field of ['forkReceiptId', 'forkGeneration', 'historyGeneration', 'nativeRecordId', 'nativeGenerationKey']) if (item.data[field] !== undefined) event.data[field] = item.data[field]
        if (item.data.forkReceipt !== undefined) event.data.forkReceipt = normalizeForkReceipt(item.data.forkReceipt)
        if (item.data.forkHistoryPending !== undefined) {
          if (item.data.forkHistoryPending !== true || !item.data.forkGeneration || item.data.forkReceiptId || item.kind !== 'input' || item.data.nativeType !== 'item_completed/UserMessage' || item.data.sourceKind !== 'chatgpt-native' || item.data.sourceForm !== 'event_msg/item_completed' || item.data.inputOrigin !== 'user-entry' || item.data.inputPhase !== 'consumed' || item.data.inherited !== false || item.data.history === true || item.data.senderSessionId) throw new Error('invalid pending fork input')
          event.data.forkHistoryPending = true
        } else if (Boolean(item.data.forkReceiptId) !== Boolean(item.data.forkGeneration)) throw new Error('incomplete fork proof marker')
        for (const key of dataStrings) if (item.data[key] !== undefined && item.data[key] !== null) {
          const field = sanitizeObservation(string(item.data[key], key, 2000))
          event.data[key] = field.text
          redacted ||= field.redacted
        }
        for (const key of ['isError', 'artifactChanged', 'truncated', 'redacted', 'timedOut', 'aborted', 'interrupted', 'inherited', 'history']) {
          if (item.data[key] !== undefined) {
            if (typeof item.data[key] !== 'boolean') throw new Error(`${key} must be boolean`)
            event.data[key] = item.data[key]
          }
        }
        for (const key of ['inputSequence', 'recordOrdinal', 'endOrdinalExclusive', 'endByteOffset', 'nativeRecordStart', 'nativeRecordEnd']) if (item.data[key] !== undefined) event.data[key] = time(item.data[key], key)
        if (item.data.exitCode !== undefined && item.data.exitCode !== null) {
          if (!Number.isSafeInteger(item.data.exitCode)) throw new Error('exitCode must be integer')
          event.data.exitCode = item.data.exitCode
        }
        if (item.data.verificationGaps !== undefined) event.data.verificationGaps = strings(item.data.verificationGaps, 'verificationGaps')
      }
      if (item.text.length > 240) event.data = { ...event.data, truncated: true }
      if (redacted) event.data = { ...event.data, redacted: true }
      return event
    })
  }
  if (input.gaps !== undefined) result.gaps = strings(input.gaps, 'gaps')
  if (input.hierarchyContext !== undefined) result.hierarchyContext = normalizeHierarchy(input.hierarchyContext)
  return result
}

export function normalizeAnalysisConfig(value) {
  object(value, 'config')
  for (const key of Object.keys(value)) if (!['llm', 'jev'].includes(key)) throw new Error('unsupported config field')
  const config = {}
  for (const key of ['llm', 'jev']) {
    if (value[key] === undefined) continue
    const input = object(value[key], key)
    for (const field of Object.keys(input)) if (!['baseUrl', 'model'].includes(field)) throw new Error('unsupported model field')
    const result = {}
    if (input.baseUrl) {
      let url
      try { url = new URL(input.baseUrl) } catch { throw new Error('model URL must be loopback HTTP(S)') }
      if (!['http:', 'https:'].includes(url.protocol) || !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname) || url.username || url.password || url.search || url.hash || !['', '/'].includes(url.pathname)) throw new Error('model URL must be loopback HTTP(S), without credentials or path')
      result.baseUrl = url.href.replace(/\/$/, '')
    }
    if (input.model) result.model = string(input.model, `${key}.model`)
    config[key] = result
  }
  return config
}
import { sanitizeObservation } from './privacy.mjs'
import { normalizeHierarchy } from './hierarchy.mjs'
