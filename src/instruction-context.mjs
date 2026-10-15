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
export function verifiedForkContext(binding, { tasks, events, sourcePolicy }) {
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
  if (boundaries.some(event => !native(event) || event.kind !== 'status' || event.data.nativeType !== 'session_meta' || event.data.sourceForm !== 'session_meta/history_base' || !event.data.sourceThreadId || event.data.sourceThreadId === owner?.id || !ordinal(event.data.endOrdinalExclusive) || !ordinal(event.data.endByteOffset) || event.data.endByteOffset === 0)) return { inheritedEvents, gaps: ['fork_boundary_origin_missing'], valid: false }
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

export function instructionContext(task, context, sourceTasks, sourceEvents, now, lookup = createInstructionLookup(context, sourceTasks, sourceEvents, now)) {
  const { tasks, events, parents, sourcePolicy } = lookup
  const bindings = context.instructionBindings.filter(item => item.taskId === task.id && item.lifecycleId === task.lifecycleId)
  const eligible = event => bindings.some(binding => binding.inputRef.eventId === event.id && verifiedUserEntry(binding, event, { tasks, events, parents, sourcePolicy }))
  const binding = bindings.find(item => item.phase === 'consumed' && item.inputRef.eventId === task.requestId)
  const gaps = [], inheritedEvents = []
  const forkContext = verifiedForkContext(binding, lookup)
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
