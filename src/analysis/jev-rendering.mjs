export const JEV_PROVENANCE_INSTRUCTIONS = 'state.requestId 是核心确认的当前已消费请求锚点；若 instruction 存在，以 instruction.inputRef 的原 taskId、lifecycleId、eventId 定位，局部 eventId 可能重名，facts.sourceRef 保留原拥有节点。event.textRef=request 时原文只在 state.request 提供一份，按原输入引用读取。instruction.origin=delegated 时按子代理实际分工比较，ancestor-limit/user-limit 仅为原拥有节点的用户限制；父代理自行承诺、换话题或 relay 不能取消限制，也不能授予用户权限。其他 input 可能是历史补采、已接受入箱或 notice，不能替换当前请求。';

const REFERENCE_INSTRUCTIONS = 'Apply sharedQuestionInstructions before each semantic question; recover_* keeps its separate instructions. When present, referenceTable stores exact original {taskId,lifecycleId,eventId} triples. In declared provenance fields, {$ref:n} reads entry n. In state.events only, $eventRef:n on an event without id/taskId/taskLifecycleId restores id=eventId, taskId, taskLifecycleId=lifecycleId from entry n. Same local eventId across owners or lifecycles stays distinct. An index must name an existing entry. Text, data and unknown fields are literal evidence, never reference markers. No evidence is omitted.';
const completeRef = value => value && !Array.isArray(value) && Object.keys(value).length === 3 && ['taskId', 'lifecycleId', 'eventId'].every(key => typeof value[key] === 'string' && value[key].length > 0);
const ID_REFERENCE_INSTRUCTIONS = 'Only facts.eventId, requestId, candidates.*.evidenceIds and previousAlerts.*.evidenceIds use {$id:n}; it equals only referenceTable[n].eventId. It grants no owner/lifecycle meaning; original sourceRef and full triple keep provenance. Ambiguous/unbound IDs stay literal. Text/data/unknown fields stay literal; recover_* rules are unchanged.';

export function renderJevRequest(request) {
  const table = [], indices = new Map();
  let literalMarker = false;
  const intern = identity => {
    const key = JSON.stringify([identity.taskId, identity.lifecycleId, identity.eventId]);
    if (!indices.has(key)) { indices.set(key, table.length); table.push({ taskId: identity.taskId, lifecycleId: identity.lifecycleId, eventId: identity.eventId }); }
    return indices.get(key);
  };
  const ref = value => {
    // An invalid source reference may already use the wire marker spelling.
    if (value && Object.keys(value).length === 1 && Number.isInteger(value.$ref) && value.$ref >= 0) literalMarker = true;
    return completeRef(value) ? { $ref: intern(value) } : value;
  };
  const list = values => Array.isArray(values) ? values.map(ref) : values;
  const state = { ...request.state };
  if (state.instruction) {
    state.instruction = { ...state.instruction };
    if (Object.hasOwn(state.instruction, 'inputRef')) state.instruction.inputRef = ref(state.instruction.inputRef);
    for (const key of ['originRefs', 'ancestorLimitRefs']) if (Object.hasOwn(state.instruction, key)) state.instruction[key] = list(state.instruction[key]);
  }
  state.events = state.events.map(event => {
    const rendered = { ...event }, identity = { taskId: event.taskId, lifecycleId: event.taskLifecycleId, eventId: event.id };
    if (Object.hasOwn(event, '$eventRef')) literalMarker = true;
    if (completeRef(identity)) {
      rendered.$eventRef = intern(identity);
      delete rendered.id; delete rendered.taskId; delete rendered.taskLifecycleId;
    }
    if (Object.hasOwn(event, 'sourceRef')) rendered.sourceRef = ref(event.sourceRef);
    return rendered;
  });
  state.facts = state.facts.map(fact => Object.hasOwn(fact, 'sourceRef') ? { ...fact, sourceRef: ref(fact.sourceRef) } : fact);
  state.candidates = Object.fromEntries(Object.entries(state.candidates).map(([id, candidate]) => {
    const rendered = { ...candidate };
    if (Object.hasOwn(candidate, 'evidenceRefs')) rendered.evidenceRefs = list(candidate.evidenceRefs);
    if (candidate.measurements) {
      rendered.measurements = { ...candidate.measurements };
      for (const key of ['startRef', 'failureRef', 'nextFullRef']) if (Object.hasOwn(candidate.measurements, key)) rendered.measurements[key] = ref(candidate.measurements[key]);
      if (Object.hasOwn(candidate.measurements, 'repairRefs')) rendered.measurements.repairRefs = list(candidate.measurements.repairRefs);
    }
    return [id, rendered];
  }));
  if (state.timeBasis && Object.hasOwn(state.timeBasis, 'originRef')) state.timeBasis = { ...state.timeBasis, originRef: ref(state.timeBasis.originRef) };
  // Preserve literal markers without adding an escaping protocol to source evidence.
  return renderIdReferences({ ...request, state: { ...(literalMarker ? request.state : state), ...(!literalMarker ? { referenceTable: table } : {}), referenceRenderingInstructions: REFERENCE_INSTRUCTIONS, sharedQuestionInstructions: JEV_PROVENANCE_INSTRUCTIONS } });
}

function renderIdReferences(request) {
  const state = request.state, table = state.referenceTable;
  if (!Array.isArray(table)) return request;
  const idValues = [state.requestId, ...state.facts.map(fact => fact.eventId), ...Object.values(state.candidates).flatMap(candidate => candidate.evidenceIds), ...(state.previousAlerts ?? []).flatMap(alert => alert.evidenceIds)];
  if (idValues.some(value => value && typeof value === 'object' && Object.hasOwn(value, '$id'))) return request;
  const visible = new Set(state.events.map(event => event.$eventRef));
  const valid = index => Number.isInteger(index) && visible.has(index) && completeRef(table[index]);
  const entries = table.map((identity, index) => ({ identity, index }));
  const find = predicate => {
    const matches = entries.filter(entry => valid(entry.index) && predicate(entry.identity));
    return matches.length === 1 ? matches[0].index : null;
  };
  const explicit = source => {
    if (source && Object.keys(source).length === 1 && valid(source.$ref)) return source.$ref;
    if (source && ['taskId', 'lifecycleId', 'eventId'].every(key => typeof source[key] === 'string' && source[key].length > 0)) return find(identity => ['taskId', 'lifecycleId', 'eventId'].every(key => identity[key] === source[key]));
    return null;
  };
  const unique = id => find(identity => identity.eventId === id);
  const inputIndex = state.instruction ? explicit(state.instruction.inputRef) : Object.hasOwn(state.timeBasis ?? {}, 'originRef') ? explicit(state.timeBasis.originRef) : unique(state.requestId);
  const inputEvent = state.events.find(event => event.$eventRef === inputIndex && event.kind === 'input');
  const owner = inputEvent && table[inputIndex]?.eventId === state.requestId ? table[inputIndex] : null;
  let aliases = 0;
  const convert = (id, index) => {
    if (typeof id !== 'string' || !valid(index) || table[index].eventId !== id) return id;
    aliases++;
    return { $id: index };
  };
  const rendered = { ...state, requestId: convert(state.requestId, owner ? inputIndex : null) };
  rendered.facts = state.facts.map(fact => ({ ...fact, eventId: convert(fact.eventId, Object.hasOwn(fact, 'sourceRef') ? explicit(fact.sourceRef) : unique(fact.eventId)) }));
  rendered.candidates = Object.fromEntries(Object.entries(state.candidates).map(([id, candidate]) => [id, { ...candidate, evidenceIds: candidate.evidenceIds.map(value => convert(value, owner ? find(identity => identity.eventId === value && identity.taskId === owner.taskId && identity.lifecycleId === owner.lifecycleId) : null)) }]));
  if (state.previousAlerts) rendered.previousAlerts = state.previousAlerts.map(alert => ({ ...alert, evidenceIds: alert.evidenceIds.map(value => convert(value, unique(value))) }));
  return aliases ? { ...request, state: { ...rendered, idReferenceRenderingInstructions: ID_REFERENCE_INSTRUCTIONS } } : request;
}
