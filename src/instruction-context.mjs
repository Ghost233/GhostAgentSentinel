import { emptyForkProof, foldForkProof, nativeForkIdentity, receiptIdentity, FORK_USER_LIMIT, FORK_LINEAGE_FRAMES, FORK_LEGACY_MODE } from './fork-proof.mjs'
const key = (...parts) => JSON.stringify(parts)
export const eventRef = event => ({ taskId: event.taskId, lifecycleId: event.taskLifecycleId, eventId: event.id })
export const referenceKey = reference => key(reference.taskId, reference.lifecycleId, reference.eventId)

// This classifies the observed entry provenance, never the identity of a person.
export function verifiedUserEntry(binding, input, { tasks, events, parents, sourcePolicy: lookupPolicy }, sourcePolicy = lookupPolicy) {
  const owner = tasks.get(key(binding.taskId, binding.lifecycleId))
  if (binding.origin !== 'user-entry' || binding.phase !== 'consumed' || !owner || parents.has(owner.sessionId) || owner.parentId || input.kind !== 'input') return false
  const data = input.data ?? {}
  if (sourcePolicy === 'chatgpt-main/1') {
    if (data.nativeType !== 'item_completed/UserMessage' || data.sourceKind !== 'chatgpt-native' || data.sourceForm !== 'event_msg/item_completed' || data.inputOrigin !== 'user-entry' || data.inputPhase !== 'consumed' || data.runId !== input.taskLifecycleId || !data.messageId || data.inherited !== false || data.history === true || data.senderSessionId) return false
    const originals = binding.originRefs.map(reference => events.get(referenceKey(reference))).filter(original => original?.taskId === input.taskId && original.taskLifecycleId === input.taskLifecycleId && original.data?.runId === data.runId && !original.data?.history && original.data?.inherited !== true)
    return originals.some(original => original.id === input.id && original.data?.nativeType === 'item_completed/UserMessage' && original.data?.messageId === data.messageId)
      && originals.some(original => original.kind === 'status' && original.data?.nativeType === 'task_started' && original.data?.sourceKind === 'chatgpt-native' && original.data?.sourceForm === 'event_msg' && ['composer', 'edit_user_message'].includes(original.data?.turnTrigger))
  }
  if (data.nativeType !== 'user/message' || data.inputPhase !== 'consumed' || data.sourceKind !== 'user' || !data.messageId || data.inherited !== false || data.senderSessionId || data.sourceForm) return false
  return binding.originRefs.some(reference => {
    const original = events.get(referenceKey(reference))
    return original?.taskId === input.taskId && original.taskLifecycleId === input.taskLifecycleId && original.data?.nativeType === 'agent/inbox/claimed' && original.data?.messageId === data.messageId && original.data?.sourceKind === 'user' && !original.data?.senderSessionId && !original.data?.sourceForm
  })
}

export function createInstructionLookup(context, sourceTasks, sourceEvents, now, sourcePolicy = null) {
  const tasks = new Map(sourceTasks.map(item => [key(item.id, item.lifecycleId), item]))
  let validUntil = Infinity
  const events = new Map(sourceEvents.filter(event => {
    const boundary = event.capturedAt > now ? event.capturedAt : event.capturedAt + 86_400_000
    if (boundary > now) validUntil = Math.min(validUntil, boundary)
    return event.capturedAt <= now && event.capturedAt > now - 86_400_000
  }).map(event => [referenceKey(eventRef(event)), event]))
  const parents = new Map(context.relations.filter(item => item.kind === 'delegation' && item.childSessionId).map(item => [item.childSessionId, item.parentSessionId]))
  return { tasks, events, parents, validUntil, sourcePolicy }
}

// Extra originRefs preserve fork user context only. They never participate in
// verifiedUserEntry: the current request still needs its own non-history run.
export function verifiedForkContext(binding, lookup) {
  const { tasks, events, sourcePolicy } = lookup
  const current = binding && events.get(referenceKey(binding.inputRef))
  if (sourcePolicy === 'chatgpt-main/1' && current?.data?.forkHistoryPending === true) {
    if (!binding?.forkContext || binding.forkContext.generation !== current.data.forkGeneration) return { inheritedEvents: [], gaps: ['fork_current_origin_missing'], valid: true }
    if (!binding.forkContext.receiptId) return { inheritedEvents: [], gaps: ['fork_context_pending'], valid: true }
    return compactForkContext(binding, current, lookup)
  }
  if (sourcePolicy === 'chatgpt-main/1' && current?.data?.forkReceiptId) return compactForkContext(binding, current, lookup)
  const inheritedEvents = [], gaps = []
  if (sourcePolicy !== 'chatgpt-main/1' || !binding) return { inheritedEvents, gaps, valid: true }
  const owner = tasks.get(key(binding.taskId, binding.lifecycleId))
  const originals = binding.originRefs.map(reference => events.get(referenceKey(reference))).filter(Boolean)
  const history = originals.filter(event => event.data?.history === true || event.data?.inherited === true)
  const boundaries = history.filter(event => event.data?.evidenceKind === 'fork-boundary')
  // Retained history may belong to an earlier observation container of this
  // same fork. Its native origin run remains independent of both containers.
  const own = event => event.taskId === binding.taskId && tasks.has(key(event.taskId, event.taskLifecycleId)) && event.data?.runId === event.taskLifecycleId && event.data?.forkThreadId === owner?.id && event.data?.history === true && event.data?.inherited === true
  const native = event => own(event) && event.data?.sourceKind === 'chatgpt-native'
  const ordinal = value => Number.isSafeInteger(value) && value >= 0
  if (boundaries.some(event => !native(event) || event.kind !== 'status' || event.data.nativeType !== 'session_meta' || event.data.sourceForm !== 'session_meta/history_base' || !event.data.sourceThreadId || !ordinal(event.data.endOrdinalExclusive) || !ordinal(event.data.endByteOffset) || event.data.endByteOffset === 0 || event.data.recordOrdinal !== undefined && event.data.recordOrdinal !== event.data.endOrdinalExclusive)) return { inheritedEvents, gaps: ['fork_boundary_origin_missing'], valid: false }
  const headerOwner = event => event.data.historyOwnerThreadId ?? owner?.id
  const headerOrdinal = event => event.data.recordOrdinal ?? event.data.endOrdinalExclusive
  const ordered = [...boundaries].sort((a, b) => headerOrdinal(b) - headerOrdinal(a))
  const connected = []
  for (const boundary of ordered) {
    const root = connected.length === 0 && headerOwner(boundary) === owner?.id
    if (!root && !connected.some(parent => parent.data.sourceThreadId === headerOwner(boundary) && headerOrdinal(boundary) < parent.data.endOrdinalExclusive)) return { inheritedEvents, gaps: ['fork_boundary_chain_missing'], valid: false }
    connected.push(boundary)
  }
  for (const input of history.filter(event => event.kind === 'input')) {
    const data = input.data
    const boundary = boundaries.find(event => event.data.sourceThreadId === data.sourceThreadId && ordinal(data.recordOrdinal) && data.recordOrdinal < event.data.endOrdinalExclusive)
    const start = history.find(event => native(event) && event.kind === 'status' && event.data?.nativeType === 'task_started' && event.data.sourceForm === 'event_msg' && ['composer', 'edit_user_message'].includes(event.data.turnTrigger) && event.data.sourceThreadId === data.sourceThreadId && event.data.originRunId === data.originRunId && ordinal(event.data.recordOrdinal) && event.data.recordOrdinal < data.recordOrdinal)
    if (!native(input) || data.nativeType !== 'item_completed/UserMessage' || data.sourceForm !== 'event_msg/item_completed' || data.inputOrigin !== 'user-entry' || data.inputPhase !== 'consumed' || !data.messageId || !data.originRunId || data.originRunId === binding.lifecycleId || data.senderSessionId || !boundary || !start) return { inheritedEvents: [], gaps: ['fork_user_origin_missing'], valid: false }
    if (!input.text?.trim() || data.truncated || data.redacted) { gaps.push('fork_user_context_excerpt_missing'); continue }
    inheritedEvents.push({ ...input, authorizationEligible: true, contextRole: 'user-limit', sourceRef: eventRef(input) })
  }
  if (boundaries.length && !inheritedEvents.length) gaps.push('fork_user_context_unconfirmed')
  return { inheritedEvents, gaps: [...new Set(gaps)], valid: true }
}

function compactForkContext(binding, current, lookup) {
  const gap = reason => ({ inheritedEvents: [], gaps: [reason], valid: true })
  if (lookup.forkCaptureBudgetExceeded) return gap('fork_context_budget_exceeded')
  if (!lookup.forkIndex) {
    const receipts = new Map(), generations = new Map()
    for (const event of lookup.events.values()) {
      const data = event.data ?? {}
      if (!data.history || !data.inherited || data.sourceKind !== 'chatgpt-native' || data.forkThreadId !== event.taskId || !lookup.tasks.has(key(event.taskId, event.taskLifecycleId))) continue
      if (data.forkReceipt) {
        const id = key(event.taskId, data.forkReceipt.id)
        if (!receipts.has(id)) receipts.set(id, event)
      } else if (data.historyGeneration && ['task_started', 'item_completed/UserMessage'].includes(data.nativeType)) {
        const id = key(event.taskId, data.historyGeneration)
        if (!generations.has(id)) generations.set(id, new Map())
        const group = generations.get(id), nativeId = nativeForkIdentity(event)
        if (group.size > FORK_USER_LIMIT * 4) group.overBudget = true
        else if (!group.has(nativeId)) group.set(nativeId, event)
      }
    }
    lookup.forkIndex = { receipts, generations }
  }
  const { receipts, generations } = lookup.forkIndex, inheritedEvents = []
  let id = binding.forkContext?.receiptId ?? current.data.forkReceiptId, generation = binding.forkContext?.generation ?? current.data.forkGeneration, owner = binding.taskId, parentCutoff = Infinity, headerOrdinal = null, users = 0, legacyUnverified = false
  const seen = new Set()
  while (id) {
    if (seen.size >= FORK_LINEAGE_FRAMES - 1 || seen.has(id)) return gap('fork_boundary_chain_missing')
    seen.add(id)
    const boundary = receipts.get(key(binding.taskId, id)), data = boundary?.data, receipt = data?.forkReceipt
    if (!receipt) return gap('fork_receipt_missing_or_expired')
    users += receipt.users
    if (receipt.overBudget || users > FORK_USER_LIMIT) return gap('fork_context_budget_exceeded')
    const sourceOwner = data.sourceOwnerThreadId ?? data.sourceThreadId
    if (data.nativeType !== 'session_meta' || data.sourceForm !== 'session_meta/history_base' || data.evidenceKind !== 'fork-boundary' || data.runId !== boundary.taskLifecycleId || data.historyOwnerThreadId !== owner || receipt.ownerGeneration !== generation || data.recordOrdinal !== receipt.endOrdinalExclusive || data.recordOrdinal >= parentCutoff || headerOrdinal !== null && data.recordOrdinal !== headerOrdinal || (receipt.beginOrdinal === 0) !== (receipt.parentId === null) || data.endOrdinalExclusive !== receipt.endOrdinalExclusive || data.endByteOffset !== receipt.endByteOffset || receipt.id !== receiptIdentity(receipt, owner, data.sourceThreadId, sourceOwner)) return gap('fork_boundary_chain_missing')
    const group = generations.get(key(binding.taskId, receipt.generation))
    const legacy = receipt.version === 2 && receipt.userProofMode === FORK_LEGACY_MODE
    legacyUnverified ||= legacy
    if (group?.overBudget) return gap('fork_context_budget_exceeded')
    const candidates = [...(group?.values() ?? [])].filter(event => event.data.sourceThreadId === sourceOwner && event.data.recordOrdinal > receipt.beginOrdinal && event.data.recordOrdinal < receipt.endOrdinalExclusive).sort((a, b) => a.data.recordOrdinal - b.data.recordOrdinal)
    let fold = emptyForkProof()
    const starts = new Map()
    for (const event of candidates) {
      const proof = event.data
      if (proof.runId !== event.taskLifecycleId || !Number.isSafeInteger(proof.recordOrdinal)) return gap('fork_user_origin_missing')
      fold = foldForkProof(fold, event)
      if (proof.nativeType === 'task_started') {
        if (event.kind !== 'status' || proof.sourceForm !== 'event_msg') return gap('fork_user_origin_missing')
        starts.set(proof.originRunId, event)
      } else {
        // Legacy range counters remain a structural witness only. Even a
        // composer-shaped candidate cannot establish this range's user scope.
        if (legacy) continue
        const start = starts.get(proof.originRunId)
        if (!start || start.data.recordOrdinal >= proof.recordOrdinal || proof.sourceForm !== 'event_msg/item_completed' || !proof.messageId || proof.senderSessionId) return gap('fork_user_origin_missing')
        if (!['composer', 'edit_user_message'].includes(start.data.turnTrigger)) continue
        if (event.kind !== 'input' || proof.inputOrigin !== 'user-entry' || proof.inputPhase !== 'consumed' || proof.originRunId === binding.lifecycleId) return gap('fork_user_origin_missing')
        if (!event.text?.trim() || proof.truncated || proof.redacted) return gap('fork_user_context_excerpt_missing')
        inheritedEvents.push({ ...event, authorizationEligible: true, contextRole: 'user-limit', sourceRef: eventRef(event) })
      }
    }
    if (fold.overBudget) return gap('fork_context_budget_exceeded')
    if ((!legacy && (receipt.unconfirmed || fold.unconfirmed)) || fold.unconfirmed !== receipt.unconfirmed || fold.users !== receipt.users || fold.digest !== receipt.digest) return gap('fork_user_proof_set_missing_or_expired')
    id = receipt.parentId; generation = receipt.generation; owner = sourceOwner; parentCutoff = receipt.endOrdinalExclusive; headerOrdinal = receipt.beginOrdinal
  }
  return legacyUnverified ? gap('fork_user_context_unverified') : { inheritedEvents, gaps: [], valid: true }
}

export function instructionContext(task, context, sourceTasks, sourceEvents, now, lookup = createInstructionLookup(context, sourceTasks, sourceEvents, now)) {
  const { tasks, events, parents, sourcePolicy } = lookup
  const bindings = context.instructionBindings.filter(item => item.taskId === task.id && item.lifecycleId === task.lifecycleId)
  const eligible = event => bindings.some(binding => binding.inputRef.eventId === event.id && verifiedUserEntry(binding, event, { tasks, events, parents, sourcePolicy }))
  const binding = bindings.find(item => item.phase === 'consumed' && item.inputRef.eventId === task.requestId)
  const gaps = [], inheritedEvents = []
  const request = events.get(key(task.id, task.lifecycleId, task.requestId))
  const forkContext = !binding && (request?.data?.forkReceiptId || request?.data?.forkHistoryPending) ? { inheritedEvents: [], gaps: ['fork_current_origin_missing'] } : verifiedForkContext(binding, lookup)
  inheritedEvents.push(...forkContext.inheritedEvents)
  gaps.push(...forkContext.gaps)
  if (parents.has(task.sessionId) && !binding) gaps.push('delegated_instruction_missing')
  const ancestors = new Set()
  for (let ancestor = parents.get(task.sessionId); ancestor; ancestor = parents.get(ancestor)) ancestors.add(ancestor)
  for (const reference of binding?.ancestorLimitRefs ?? []) {
    const event = events.get(referenceKey(reference)), owner = tasks.get(key(reference.taskId, reference.lifecycleId))
    const originalBinding = context.instructionBindings.find(item => item.taskId === reference.taskId && item.lifecycleId === reference.lifecycleId && item.inputRef.eventId === reference.eventId && item.phase === 'consumed')
    if (!event || !ancestors.has(owner?.sessionId) || !originalBinding || !verifiedUserEntry(originalBinding, event, { tasks, events, parents, sourcePolicy })) { gaps.push('ancestor_limit_origin_or_retention_missing'); continue }
    if (owner.requestId && owner.requestId !== reference.eventId && !binding.ancestorLimitRefs.some(item => item.taskId === owner.id && item.lifecycleId === owner.lifecycleId && item.eventId === owner.requestId)) {
      const latestBinding = context.instructionBindings.find(item => item.taskId === owner.id && item.lifecycleId === owner.lifecycleId && item.inputRef.eventId === owner.requestId && item.phase === 'consumed');
      const latest = latestBinding && events.get(referenceKey(latestBinding.inputRef));
      if (latest && verifiedUserEntry(latestBinding, latest, { tasks, events, parents, sourcePolicy })) gaps.push('ancestor_user_scope_update_unconfirmed');
    }
    if (!event.text?.trim() || event.data?.truncated || event.data?.redacted) { gaps.push('ancestor_limit_excerpt_missing'); continue }
    inheritedEvents.push({ ...event, authorizationEligible: true, contextRole: 'ancestor-limit', sourceRef: reference })
  }
  if (parents.has(task.sessionId) && binding && inheritedEvents.length === 0) gaps.push('ancestor_user_limits_unconfirmed')
  if (['agent-relay', 'unknown'].includes(binding?.origin) && !parents.has(task.sessionId)) {
    const original = bindings.filter(item => item.inputRef.eventId !== task.requestId && item.phase === 'consumed').map(item => ({ item, event: events.get(referenceKey(item.inputRef)) })).filter(({ item, event }) => event && verifiedUserEntry(item, event, { tasks, events, parents, sourcePolicy })).at(-1)?.event;
    if (!original) gaps.push('own_user_limits_unconfirmed');
    else if (!original.text?.trim() || original.data?.truncated || original.data?.redacted) gaps.push('own_user_limit_excerpt_missing');
    else inheritedEvents.push({ ...original, authorizationEligible: true, contextRole: 'user-limit', sourceRef: eventRef(original) });
  }
  return { binding: binding ?? null, inheritedEvents, gaps: [...new Set(gaps)], eligible }
}
