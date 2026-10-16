import { createHash } from 'node:crypto'

export const FORK_CONTEXT_FEATURE = 'chatgpt-fork-context/1'
export const FORK_STORAGE_FEATURE = 'chatgpt-fork-storage/1'
export const FORK_LINEAGE_FEATURE = 'chatgpt-fork-lineage/1'
export const FORK_LINEAGE_FRAMES = 16
export const FORK_LEGACY_FEATURE = 'chatgpt-fork-legacy/1'
export const FORK_LEGACY_PRODUCER = '0.162.0-alpha.2'
export const FORK_LEGACY_MODE = 'legacy-unverified'
export const FORK_USER_LIMIT = 1024
const hash = value => createHash('sha256').update(JSON.stringify(value)).digest('hex')
export const forkGeneration = (fileId, header) => hash(['fork-generation/1', fileId, header])
export const receiptIdentity = (receipt, owner, source, sourceOwner = source) => hash([sourceOwner === source ? 'fork-receipt/1' : 'fork-receipt/2', receipt.ownerGeneration, receipt.generation, owner, source, ...(sourceOwner === source ? [] : [sourceOwner]), receipt.beginOrdinal, receipt.endOrdinalExclusive, receipt.endByteOffset])
export const nativeRecordIdentity = (fileId, header, recordStart, nativeType) => hash(['chatgpt-native-record/1', forkGeneration(fileId, header), recordStart, nativeType])
export const emptyForkProof = () => ({ digest: hash(['fork-proof/1']), users: 0, records: 0, unconfirmed: 0, start: null, overBudget: false })
export const validForkProof = value => Boolean(value && typeof value === 'object' && Object.keys(value).length === 6 && /^[a-f0-9]{64}$/.test(value.digest) && ['users', 'records', 'unconfirmed'].every(field => Number.isSafeInteger(value[field]) && value[field] >= 0) && value.unconfirmed <= value.users && value.users <= value.records && typeof value.overBudget === 'boolean' && (value.start === null || value.start && Object.keys(value.start).length === 3 && typeof value.start.run === 'string' && value.start.run.length <= 128 && Number.isSafeInteger(value.start.ordinal) && value.start.ordinal >= 0 && (value.start.trigger === null || typeof value.start.trigger === 'string' && value.start.trigger.length <= 256)))
export const nativeForkIdentity = event => {
  const data = event.data ?? {}
  if (data.sourceKind === 'chatgpt-native' && data.nativeRecordId) return data.nativeRecordId
  if (!data.history || !data.inherited || !data.forkThreadId || data.sourceKind !== 'chatgpt-native') return null
  if (data.forkReceipt) return hash(['fork-boundary/1', event.taskId, data.forkReceipt.id])
  if (!data.historyGeneration || !data.nativeType) return null
  return hash(['fork-origin/1', event.taskId, data.historyGeneration, data.sourceThreadId, data.recordOrdinal, data.nativeType])
}

// Structural membership survives privacy normalization; excerpt completeness is
// checked separately against actual retained producer bodies.
export function foldForkProof(state, event) {
  const data = event.data ?? {}, type = data.nativeType
  if (!['task_started', 'item_completed/UserMessage'].includes(type)) return state
  const run = data.originRunId
  const next = { ...state, records: state.records + 1 }
  if (type === 'task_started') next.start = { run, ordinal: data.recordOrdinal, trigger: data.turnTrigger ?? null }
  else {
    next.users++
    if (!state.start || state.start.run !== run || state.start.ordinal >= data.recordOrdinal || !state.start.trigger) next.unconfirmed++
  }
  next.overBudget ||= next.users > FORK_USER_LIMIT || next.records > FORK_USER_LIMIT * 4
  if (!next.overBudget) next.digest = hash([state.digest, type, data.recordOrdinal, run, data.turnTrigger ?? null, data.messageId ?? null, data.inputOrigin ?? null, data.inputPhase ?? null])
  return next
}

export function normalizeForkReceipt(value) {
  const fields = ['version', 'id', 'ownerGeneration', 'generation', 'beginOrdinal', 'endOrdinalExclusive', 'endByteOffset', 'users', 'digest', 'unconfirmed', 'parentId', 'overBudget']
  const legacy = value?.version === 2
  if (legacy) fields.push('userProofMode')
  const hex = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value)
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length !== fields.length || Object.keys(value).some(key => !fields.includes(key)) || ![1, 2].includes(value.version) || legacy && value.userProofMode !== FORK_LEGACY_MODE || !['id', 'ownerGeneration', 'generation', 'digest'].every(key => hex(value[key])) || value.parentId !== null && !hex(value.parentId) || typeof value.overBudget !== 'boolean' || !['beginOrdinal', 'endOrdinalExclusive', 'endByteOffset', 'users', 'unconfirmed'].every(key => Number.isSafeInteger(value[key]) && value[key] >= 0) || value.endOrdinalExclusive <= value.beginOrdinal || value.endByteOffset === 0 || value.unconfirmed > value.users) throw Object.assign(new Error('invalid fork receipt'), legacy ? { code: 'hierarchy_context_invalid' } : {})
  return { ...value }
}
