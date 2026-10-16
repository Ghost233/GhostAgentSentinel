import { verifiedUserEntry, verifiedForkContext } from './instruction-context.mjs'
export const HIERARCHY_FEATURE = 'hierarchy-context/1'
const key = (...parts) => JSON.stringify(parts)
const fail = message => { throw Object.assign(new Error(message), { code: 'hierarchy_context_invalid' }) }
const str = (value, field) => typeof value === 'string' && value.trim() && value.length <= 500 ? value : fail(`invalid ${field}`)
const list = (value, field, max) => Array.isArray(value) && value.length <= max ? value : fail(`invalid ${field}`)
const object = value => value && typeof value === 'object' && !Array.isArray(value) ? value : fail('invalid hierarchy object')
const fields = (value, allowed) => { object(value); if (Object.keys(value).some(field => !allowed.includes(field))) fail('unsupported hierarchy field') }
const ref = value => { fields(value, ['taskId', 'lifecycleId', 'eventId']); return { taskId: str(value.taskId, 'taskId'), lifecycleId: str(value.lifecycleId, 'lifecycleId'), eventId: str(value.eventId, 'eventId') } }
const refs = value => list(value, 'references', 100).map(ref)
const unique = (values, identity) => { const keys = new Set(); for (const value of values) { const id = identity(value); if (keys.has(id)) fail('duplicate hierarchy identity'); keys.add(id) } }

export function normalizeHierarchy(value) {
  object(value)
  if (value.schemaVersion !== 1) throw Object.assign(new Error('unsupported hierarchy schema version'), { code: 'hierarchy_version_unsupported' })
  fields(value, ['schemaVersion', 'revision', 'sessions', 'activations', 'relations', 'instructionBindings', 'waitBindings', 'coverage'])
  if (!Number.isSafeInteger(value.revision) || value.revision < 0) fail('invalid hierarchy revision')
  const sessions = list(value.sessions, 'sessions', 2000).map(item => { fields(item, ['sessionId']); return { sessionId: str(item.sessionId, 'sessionId') } })
  const activations = list(value.activations, 'activations', 2000).map(item => {
    fields(item, ['taskId', 'lifecycleId', 'sessionId', 'runId', 'state', 'stateRefs'])
    if (!['running', 'ended', 'unknown'].includes(item.state)) fail('invalid activation state')
    return { taskId: str(item.taskId, 'taskId'), lifecycleId: str(item.lifecycleId, 'lifecycleId'), sessionId: item.sessionId === null ? null : str(item.sessionId, 'sessionId'),
      runId: item.runId === null ? null : str(item.runId, 'runId'), state: item.state, stateRefs: refs(item.stateRefs) }
  })
  const relations = list(value.relations, 'relations', 2000).map(item => {
    fields(item, ['parentSessionId', 'childSessionId', 'childActivationRef', 'kind', 'evidenceRefs'])
    if (!['delegation', 'branch', 'unconfirmed'].includes(item.kind)) fail('invalid relation kind')
    if ((item.childSessionId !== undefined) === (item.childActivationRef !== undefined)) fail('relation requires one child identity')
    const child = item.childSessionId !== undefined ? { childSessionId: str(item.childSessionId, 'childSessionId') } : { childActivationRef: (() => { fields(item.childActivationRef, ['taskId', 'lifecycleId']); return { taskId: str(item.childActivationRef.taskId, 'taskId'), lifecycleId: str(item.childActivationRef.lifecycleId, 'lifecycleId') } })() }
    return { parentSessionId: str(item.parentSessionId, 'parentSessionId'), ...child, kind: item.kind, evidenceRefs: refs(item.evidenceRefs) }
  })
  const instructionBindings = list(value.instructionBindings, 'instructionBindings', 1000).map(item => {
    fields(item, ['taskId', 'lifecycleId', 'inputRef', 'phase', 'origin', 'originRefs', 'ancestorLimitRefs', 'forkContext'])
    if (!['accepted', 'consumed'].includes(item.phase) || !['user-entry', 'delegated', 'agent-relay', 'runtime-notice', 'unknown'].includes(item.origin)) fail('invalid instruction origin or phase')
    let forkContext
    if (item.forkContext !== undefined) {
      fields(item.forkContext, ['generation', 'receiptId'])
      if (!/^[a-f0-9]{64}$/.test(item.forkContext.generation ?? '') || item.forkContext.receiptId !== undefined && !/^[a-f0-9]{64}$/.test(item.forkContext.receiptId)) fail('invalid fork context')
      forkContext = { generation: item.forkContext.generation, ...(item.forkContext.receiptId ? { receiptId: item.forkContext.receiptId } : {}) }
    }
    return { ...(forkContext ? { forkContext } : {}), taskId: str(item.taskId, 'taskId'), lifecycleId: str(item.lifecycleId, 'lifecycleId'), inputRef: ref(item.inputRef), phase: item.phase, origin: item.origin, originRefs: refs(item.originRefs), ancestorLimitRefs: refs(item.ancestorLimitRefs) }
  })
  const waitBindings = list(value.waitBindings ?? [], 'waitBindings', 1000).map(item => {
    fields(item, ['taskId', 'lifecycleId', 'waitId', 'phase', 'childRefs', 'evidenceRefs'])
    if (!['started', 'ended'].includes(item.phase)) fail('invalid wait phase')
    const childRefs = list(item.childRefs, 'wait children', 100).map(child => { fields(child, ['taskId', 'lifecycleId']); return { taskId: str(child.taskId, 'taskId'), lifecycleId: str(child.lifecycleId, 'lifecycleId') } })
    if (!childRefs.length) fail('child wait requires bound descendants')
    unique(childRefs, child => key(child.taskId, child.lifecycleId))
    return { taskId: str(item.taskId, 'taskId'), lifecycleId: str(item.lifecycleId, 'lifecycleId'), waitId: str(item.waitId, 'waitId'), phase: item.phase, childRefs, evidenceRefs: refs(item.evidenceRefs) }
  })
  fields(value.coverage, ['complete', 'branchGaps'])
  if (typeof value.coverage.complete !== 'boolean') fail('invalid hierarchy coverage')
  const branchGaps = list(value.coverage.branchGaps, 'branchGaps', 100).map(item => { fields(item, ['sessionId', 'reason']); return { sessionId: str(item.sessionId, 'sessionId'), reason: str(item.reason, 'reason') } })
  if (value.coverage.complete && branchGaps.length) fail('complete hierarchy contains branch gaps')
  unique(sessions, item => item.sessionId); unique(activations, item => key(item.taskId, item.lifecycleId)); unique(relations, item => key(item.parentSessionId, item.childSessionId ?? item.childActivationRef)); unique(instructionBindings, item => key(item.taskId, item.lifecycleId, item.inputRef.eventId, item.phase))
  unique(waitBindings, item => key(item.taskId, item.lifecycleId, item.waitId))
  return { schemaVersion: 1, revision: value.revision, sessions, activations, relations, instructionBindings, ...(value.waitBindings === undefined ? {} : { waitBindings }), coverage: { complete: value.coverage.complete, branchGaps } }
}

// Validate against the whole prospective batch before the core mutates source/task state.
export function validateHierarchy(context, { tasks, events, now, acceptedRuns = [], sourcePolicy = null }) {
  const chatGPT = sourcePolicy === 'chatgpt-main/1'
  if (chatGPT && (context.relations.length || context.waitBindings?.length || tasks.some(task => task.parentId))) fail('ChatGPT main policy does not include subagents or unverified ancestry')
  const sessions = new Set(context.sessions.map(item => item.sessionId)), byTask = new Map(tasks.map(task => [key(task.id, task.lifecycleId), task]))
  const byEvent = new Map(events.filter(event => event.capturedAt <= now && event.capturedAt > now - 86_400_000).map(event => [key(event.taskId, event.taskLifecycleId, event.id), event]))
  const resolve = reference => { const event = byEvent.get(key(reference.taskId, reference.lifecycleId, reference.eventId)); if (!byTask.has(key(reference.taskId, reference.lifecycleId)) || !event) fail('hierarchy reference is outside source, lifecycle or retention'); return event }
  const task = item => { const owner = byTask.get(key(item.taskId, item.lifecycleId)); if (!owner) fail('activation task is not observed'); return owner }
  // Core retains only recent bindings or immutable identities of known live tasks.
  // A live run identity can remain guarded after its original proof body expires.
  const runs = new Map(acceptedRuns.map(item => [item.runId, { ...item }]))
  for (const activation of context.activations) {
    const owner = task(activation)
    if (activation.sessionId !== null && (!sessions.has(activation.sessionId) || owner.sessionId !== activation.sessionId)) fail('activation session does not match its task')
    const evidence = activation.stateRefs.map(resolve)
    const nativeMain = event => chatGPT && activation.sessionId === owner.sessionId && owner.sessionId === owner.id && activation.runId === activation.lifecycleId && event.taskId === activation.taskId && event.taskLifecycleId === activation.lifecycleId && event.kind === 'status' && event.data?.sourceKind === 'chatgpt-native' && event.data?.sourceForm === 'event_msg' && event.data?.runId === activation.runId
    if (activation.runId !== null) {
      const proof = evidence.filter(event => chatGPT ? nativeMain(event) && ['task_started', 'task_complete', 'turn_aborted'].includes(event.data?.nativeType) : ['subagent/start', 'subagent/end'].includes(event.data?.nativeType) && event.data?.childSessionId === (activation.sessionId ?? activation.taskId) && event.data?.runId === activation.runId)
      if (!proof.length) fail('activation run identity lacks its own native evidence')
      const accepted = runs.get(activation.runId)
      if (accepted && key(accepted.taskId, accepted.lifecycleId, accepted.sessionId) !== key(activation.taskId, activation.lifecycleId, activation.sessionId)) fail('native activation run is already bound to another task lifecycle')
      runs.set(activation.runId, { runId: activation.runId, taskId: activation.taskId, lifecycleId: activation.lifecycleId, sessionId: activation.sessionId, capturedAt: Math.max(accepted?.capturedAt ?? 0, ...proof.map(event => event.capturedAt)) })
      if (runs.size > 2000) fail('native activation binding retention budget exceeded')
    }
    if (activation.state === 'unknown') continue
    const expected = activation.state === 'running' ? 'running' : ['idle', 'disposed']
    const ownState = evidence.some(event => event.taskId === activation.taskId && event.taskLifecycleId === activation.lifecycleId && event.kind === 'status' && (Array.isArray(expected) ? expected.includes(event.data?.status) : event.data?.status === expected) && ['agent/created', 'agent/status', 'agent/disposed', 'native-baseline'].includes(event.data?.nativeType))
    const nativeActivation = activation.runId && evidence.some(event => event.data?.nativeType === (activation.state === 'running' ? 'subagent/start' : 'subagent/end') && event.data?.childSessionId === (activation.sessionId ?? activation.taskId) && event.data?.runId === activation.runId)
    const mainState = evidence.some(event => nativeMain(event) && (activation.state === 'running' ? event.data?.nativeType === 'task_started' && event.data?.status === 'running' : ['task_complete', 'turn_aborted'].includes(event.data?.nativeType) && ['idle', 'disposed'].includes(event.data?.status)))
    if (chatGPT ? !mainState : !ownState && !nativeActivation) fail('directory membership is not execution state evidence')
  }
  const parents = new Map(), externalParents = new Map()
  for (const relation of context.relations) {
    if (!sessions.has(relation.parentSessionId) || (relation.childSessionId !== undefined && !sessions.has(relation.childSessionId))) fail('relation session is not observed')
    if (relation.childActivationRef) task(relation.childActivationRef)
    const evidence = relation.evidenceRefs.map(resolve)
    if (relation.kind !== 'delegation') continue
    if (!evidence.some(event => byTask.get(key(event.taskId, event.taskLifecycleId))?.sessionId === relation.parentSessionId && event.data?.nativeType === 'subagent/catalog' && event.data?.childSessionId === (relation.childSessionId ?? relation.childActivationRef?.taskId))) fail('delegation requires the parent-owned native catalog')
    if (relation.childActivationRef) {
      const child = task(relation.childActivationRef), id = key(child.id, child.lifecycleId)
      if (child.sessionId != null || externalParents.has(id)) fail('external delegation requires one parent and no local session')
      externalParents.set(id, relation.parentSessionId); continue
    }
    if (relation.childSessionId === relation.parentSessionId || parents.has(relation.childSessionId)) fail('delegation has multiple parents or self reference')
    parents.set(relation.childSessionId, relation.parentSessionId)
  }
  for (const child of parents.keys()) { const seen = new Set([child]); for (let parent = parents.get(child); parent; parent = parents.get(parent)) { if (seen.has(parent)) fail('cyclic delegation'); seen.add(parent) } }
  for (const activation of context.activations) if (activation.runId !== null && (parents.has(activation.sessionId) || externalParents.has(key(activation.taskId, activation.lifecycleId)))) {
    const parentSessionId = parents.get(activation.sessionId) ?? externalParents.get(key(activation.taskId, activation.lifecycleId))
    if (!activation.stateRefs.map(resolve).some(event => event.data?.runId === activation.runId && byTask.get(key(event.taskId, event.taskLifecycleId))?.sessionId === parentSessionId)) fail('activation native run belongs to another parent')
  }
  const instructionLookup = { tasks: byTask, events: byEvent, parents, sourcePolicy }
  for (const binding of context.instructionBindings) {
    const owner = task(binding), input = resolve(binding.inputRef)
    if (binding.inputRef.taskId !== binding.taskId || binding.inputRef.lifecycleId !== binding.lifecycleId || !['input', 'gap'].includes(input.kind)) fail('instruction must cite its own input')
    const nativePhase = input.data?.nativeType === 'user/message' ? 'consumed' : ['agent/inbox/claimed', 'agent/inbox/spliced'].includes(input.data?.nativeType) ? 'accepted' : null
    if (nativePhase && nativePhase !== binding.phase) fail('instruction phase contradicts its native event type')
    if (input.data?.inputPhase && input.data.inputPhase !== binding.phase) fail('instruction phase differs from original native input')
    const originEvents = binding.originRefs.map(resolve)
    if (binding.origin === 'delegated' && !parents.has(owner.sessionId)) fail('delegated instruction has no verified relation')
    if (binding.origin === 'user-entry' && !verifiedUserEntry(binding, input, { tasks: byTask, events: byEvent, parents }, sourcePolicy)) fail('user label alone does not establish an original consumed user entry')
    if (binding.forkContext && (!chatGPT || input.data?.forkHistoryPending !== true || binding.forkContext.generation !== input.data?.forkGeneration)) fail('fork context is not bound to its current native input')
    if (!verifiedForkContext(binding, instructionLookup).valid) fail('fork context lacks its own native history boundary and user provenance')
    if (binding.origin === 'delegated' && originEvents.length === 0) fail('delegated instruction lacks origin evidence')
    const ancestors = new Set(); for (let ancestor = parents.get(owner.sessionId); ancestor; ancestor = parents.get(ancestor)) ancestors.add(ancestor)
    for (const reference of binding.ancestorLimitRefs) { const event = resolve(reference), ancestor = byTask.get(key(event.taskId, event.taskLifecycleId)); if (!ancestors.has(ancestor?.sessionId) || event.kind !== 'input' || ['delegated', 'agent-relay', 'runtime-notice'].includes(event.data?.inputOrigin)) fail('inherited limit must cite a related ancestor input') }
  }
  for (const binding of context.waitBindings ?? []) {
    const owner = task(binding), evidence = binding.evidenceRefs.map(resolve)
    const proof = type => evidence.find(event => event.taskId === binding.taskId && event.taskLifecycleId === binding.lifecycleId && event.kind === 'status' && !event.data?.history && event.data?.nativeType === type && event.data?.waitId === binding.waitId)
    const start = proof('execution/wait-start'), end = proof('execution/wait-end')
    if (!start || binding.phase === 'ended' && (!end || end.capturedAt < start.capturedAt) || binding.phase === 'started' && end) fail('child wait lacks its own current native opening/closing audit')
    for (const childRef of binding.childRefs) {
      const child = task(childRef), seen = new Set()
      let ancestor = child.sessionId == null ? externalParents.get(key(child.id, child.lifecycleId)) : parents.get(child.sessionId)
      while (ancestor && ancestor !== owner.sessionId && !seen.has(ancestor)) { seen.add(ancestor); ancestor = parents.get(ancestor) }
      if (!owner.sessionId || ancestor !== owner.sessionId) fail('wait target is not a verified delegated descendant')
    }
  }
  return [...runs.values()]
}

export { projectHierarchy } from './hierarchy-projection.mjs'
