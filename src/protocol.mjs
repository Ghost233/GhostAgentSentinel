export const PROTOCOL = 1
export const GS_SUMMARY_FEATURE = 'gs-summary/1'
export const SOURCE_OBSERVATION_FEATURE = 'source-observation/1'
export const DEFAULT_RETENTION_MS = 24 * 60 * 60 * 1000
const kinds = new Set(['input', 'output', 'tool-start', 'tool-result', 'status', 'gap'])
const statuses = new Set(['running', 'idle', 'disposed', 'unknown'])
const dataStrings = ['callId', 'rootCallId', 'parentCallId', 'toolName', 'command', 'errorCode', 'sourceVersion', 'validationScope', 'status', 'cwd', 'resultSource', 'signal', 'refinesEventId', 'nativeType', 'childSessionId', 'runId', 'inputOrigin', 'messageId', 'inputPhase', 'sourceKind', 'sourceForm', 'senderSessionId', 'approvalId', 'waitId']

function object(value, field) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${field} must be an object`)
  return value
}
function string(value, field, max = 500) {
  if (typeof value !== 'string' || !value.trim() || value.length > max) throw new Error(`${field} must be a nonempty string (max ${max})`)
  return value
}
function strings(value, field) {
  if (!Array.isArray(value) || value.length > 100) throw new Error(`${field} must be a bounded list`)
  return value.map(item => string(item, field))
}
function time(value, field) {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${field} must be an epoch millisecond integer`)
  return value
}

export function normalizeSource(value) {
  const input = object(value, 'source')
  const result = { registrationId: string(input.registrationId, 'registrationId'), hostRunId: string(input.hostRunId, 'hostRunId'), name: string(input.name, 'name') }
  if (input.mode !== undefined) {
    if (input.mode !== 'observation-only') throw new Error('unsupported source observation mode')
    result.mode = input.mode
  }
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
        event.data = { ...event.data }
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
        if (item.data.inputSequence !== undefined) event.data.inputSequence = time(item.data.inputSequence, 'inputSequence')
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
