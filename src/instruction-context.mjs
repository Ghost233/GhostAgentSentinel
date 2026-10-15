const key = (...parts) => JSON.stringify(parts)
export const eventRef = event => ({ taskId: event.taskId, lifecycleId: event.taskLifecycleId, eventId: event.id })
export const referenceKey = reference => key(reference.taskId, reference.lifecycleId, reference.eventId)

// This classifies the observed entry provenance, never the identity of a person.
export function verifiedUserEntry(binding, input, { tasks, events, parents }) {
  const owner = tasks.get(key(binding.taskId, binding.lifecycleId))
  if (binding.origin !== 'user-entry' || binding.phase !== 'consumed' || !owner || parents.has(owner.sessionId) || owner.parentId || input.kind !== 'input') return false
  const data = input.data ?? {}
  if (data.nativeType !== 'user/message' || data.inputPhase !== 'consumed' || data.sourceKind !== 'user' || !data.messageId || data.inherited !== false || data.senderSessionId || data.sourceForm) return false
  return binding.originRefs.some(reference => {
    const original = events.get(referenceKey(reference))
    return original?.taskId === input.taskId && original.taskLifecycleId === input.taskLifecycleId && original.data?.nativeType === 'agent/inbox/claimed' && original.data?.messageId === data.messageId && original.data?.sourceKind === 'user' && !original.data?.senderSessionId && !original.data?.sourceForm
  })
}

export function createInstructionLookup(context, sourceTasks, sourceEvents, now) {
  const tasks = new Map(sourceTasks.map(item => [key(item.id, item.lifecycleId), item]))
  let validUntil = Infinity
  const events = new Map(sourceEvents.filter(event => {
    const boundary = event.capturedAt > now ? event.capturedAt : event.capturedAt + 86_400_000
    if (boundary > now) validUntil = Math.min(validUntil, boundary)
    return event.capturedAt <= now && event.capturedAt > now - 86_400_000
  }).map(event => [referenceKey(eventRef(event)), event]))
  const parents = new Map(context.relations.filter(item => item.kind === 'delegation' && item.childSessionId).map(item => [item.childSessionId, item.parentSessionId]))
  return { tasks, events, parents, validUntil }
}

export function instructionContext(task, context, sourceTasks, sourceEvents, now, lookup = createInstructionLookup(context, sourceTasks, sourceEvents, now)) {
  const { tasks, events, parents } = lookup
  const bindings = context.instructionBindings.filter(item => item.taskId === task.id && item.lifecycleId === task.lifecycleId)
  const eligible = event => bindings.some(binding => binding.inputRef.eventId === event.id && verifiedUserEntry(binding, event, { tasks, events, parents }))
  const binding = bindings.find(item => item.phase === 'consumed' && item.inputRef.eventId === task.requestId)
  const gaps = [], inheritedEvents = []
  if (parents.has(task.sessionId) && !binding) gaps.push('delegated_instruction_missing')
  const ancestors = new Set()
  for (let ancestor = parents.get(task.sessionId); ancestor; ancestor = parents.get(ancestor)) ancestors.add(ancestor)
  for (const reference of binding?.ancestorLimitRefs ?? []) {
    const event = events.get(referenceKey(reference)), owner = tasks.get(key(reference.taskId, reference.lifecycleId))
    const originalBinding = context.instructionBindings.find(item => item.taskId === reference.taskId && item.lifecycleId === reference.lifecycleId && item.inputRef.eventId === reference.eventId && item.phase === 'consumed')
    if (!event || !ancestors.has(owner?.sessionId) || !originalBinding || !verifiedUserEntry(originalBinding, event, { tasks, events, parents })) { gaps.push('ancestor_limit_origin_or_retention_missing'); continue }
    if (owner.requestId && owner.requestId !== reference.eventId && !binding.ancestorLimitRefs.some(item => item.taskId === owner.id && item.lifecycleId === owner.lifecycleId && item.eventId === owner.requestId)) {
      const latestBinding = context.instructionBindings.find(item => item.taskId === owner.id && item.lifecycleId === owner.lifecycleId && item.inputRef.eventId === owner.requestId && item.phase === 'consumed');
      const latest = latestBinding && events.get(referenceKey(latestBinding.inputRef));
      if (latest && verifiedUserEntry(latestBinding, latest, { tasks, events, parents })) gaps.push('ancestor_user_scope_update_unconfirmed');
    }
    if (!event.text?.trim() || event.data?.truncated || event.data?.redacted) { gaps.push('ancestor_limit_excerpt_missing'); continue }
    inheritedEvents.push({ ...event, authorizationEligible: true, contextRole: 'ancestor-limit', sourceRef: reference })
  }
  if (parents.has(task.sessionId) && binding && inheritedEvents.length === 0) gaps.push('ancestor_user_limits_unconfirmed')
  if (['agent-relay', 'unknown'].includes(binding?.origin) && !parents.has(task.sessionId)) {
    const original = bindings.filter(item => item.inputRef.eventId !== task.requestId && item.phase === 'consumed').map(item => ({ item, event: events.get(referenceKey(item.inputRef)) })).filter(({ item, event }) => event && verifiedUserEntry(item, event, { tasks, events, parents })).at(-1)?.event;
    if (!original) gaps.push('own_user_limits_unconfirmed');
    else if (!original.text?.trim() || original.data?.truncated || original.data?.redacted) gaps.push('own_user_limit_excerpt_missing');
    else inheritedEvents.push({ ...original, authorizationEligible: true, contextRole: 'user-limit', sourceRef: eventRef(original) });
  }
  return { binding: binding ?? null, inheritedEvents, gaps: [...new Set(gaps)], eligible }
}
