// The wire dictionary applies only to declared provenance fields, never evidence data.
export function decodeJevRequest(body) {
  const decoded = structuredClone(body);
  const state = decoded.state, table = state?.referenceTable;
  if (typeof state?.sharedQuestionInstructions === 'string') for (const [id, question] of Object.entries(decoded.questions ?? {})) if (!id.startsWith('recover_')) question.instructions = state.sharedQuestionInstructions + question.instructions;
  delete state.sharedQuestionInstructions;
  delete state.referenceRenderingInstructions;
  if (!Array.isArray(table)) return decoded;
  if (typeof state.idReferenceRenderingInstructions === 'string' && state.idReferenceRenderingInstructions.includes('{$id:n}')) {
    const id = value => value && Object.keys(value).length === 1 && Number.isInteger(value.$id) && Object.hasOwn(table, value.$id) ? table[value.$id].eventId : value;
    state.requestId = id(state.requestId);
    for (const fact of state.facts ?? []) fact.eventId = id(fact.eventId);
    for (const candidate of Object.values(state.candidates ?? {})) if (Array.isArray(candidate.evidenceIds)) candidate.evidenceIds = candidate.evidenceIds.map(id);
    for (const alert of state.previousAlerts ?? []) if (Array.isArray(alert.evidenceIds)) alert.evidenceIds = alert.evidenceIds.map(id);
    delete state.idReferenceRenderingInstructions;
  }
  const ref = value => value && Object.keys(value).length === 1 && Number.isInteger(value.$ref) && Object.hasOwn(table, value.$ref) ? structuredClone(table[value.$ref]) : value;
  const list = values => Array.isArray(values) ? values.map(ref) : values;
  if (state.instruction) {
    for (const key of ['inputRef']) if (Object.hasOwn(state.instruction, key)) state.instruction[key] = ref(state.instruction[key]);
    for (const key of ['originRefs', 'ancestorLimitRefs']) if (Object.hasOwn(state.instruction, key)) state.instruction[key] = list(state.instruction[key]);
  }
  for (const event of state.events ?? []) {
    if (!Object.hasOwn(event, 'id') && Number.isInteger(event.$eventRef)) {
      const identity = table[event.$eventRef];
      delete event.$eventRef;
      Object.assign(event, { id: identity.eventId, taskId: identity.taskId, taskLifecycleId: identity.lifecycleId });
    }
    if (Object.hasOwn(event, 'sourceRef')) event.sourceRef = ref(event.sourceRef);
  }
  for (const fact of state.facts ?? []) if (Object.hasOwn(fact, 'sourceRef')) fact.sourceRef = ref(fact.sourceRef);
  for (const candidate of Object.values(state.candidates ?? {})) {
    if (Object.hasOwn(candidate, 'evidenceRefs')) candidate.evidenceRefs = list(candidate.evidenceRefs);
    if (candidate.measurements) {
      for (const key of ['startRef', 'failureRef', 'nextFullRef']) if (Object.hasOwn(candidate.measurements, key)) candidate.measurements[key] = ref(candidate.measurements[key]);
      if (Object.hasOwn(candidate.measurements, 'repairRefs')) candidate.measurements.repairRefs = list(candidate.measurements.repairRefs);
    }
  }
  if (state.timeBasis && Object.hasOwn(state.timeBasis, 'originRef')) state.timeBasis.originRef = ref(state.timeBasis.originRef);
  delete state.referenceTable;
  return decoded;
}
