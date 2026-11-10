// Limits describe the retained observation window. They do not certify any
// check: a check still needs its own trigger, evidence and actual decision.
const limits = new Set([
  'Older native records were removed at the capture limit',
  'Some observed excerpts are truncated',
  'Some observed content was omitted for privacy',
  'analysis_evidence_budget',
  'no_applicable_semantic_checks',
  '未识别原生响应材料类型：additional_tools',
]);
const derived = new Set(['event_excerpt_or_capture_gap', 'capture_coverage_unconfirmed', 'extraction_evidence_gap']);

export function toolDeclarationDiagnostic(event) {
  return event.kind === 'gap' && event.data?.sourceKind === 'chatgpt-native' && event.data?.nativeType === 'response_item/additional_tools';
}

export function coverageBlockers(coverage) {
  const gaps = coverage?.gaps ?? [];
  const scoped = gaps.some(gap => limits.has(gap)) || coverage?.extractionGaps?.length > 0;
  const blocked = gaps.filter(gap => !limits.has(gap) && !(scoped && derived.has(gap)));
  if (coverage?.complete === false && !gaps.length) blocked.push('capture_coverage_unconfirmed');
  return blocked;
}

export function checkEvidence(events, request, coverage, ids) {
  const blocked = coverageBlockers(coverage);
  return Object.fromEntries(ids.map(id => {
    const missing = blocked.length ? ['当前执行记录存在实际采集缺口：' + blocked[0]] : [];
    if (id !== 'long_wait' && !request?.text?.trim()) missing.push('缺少可引用的当前请求正文');
    if (['stalled_progress', 'request_drift', 'validation_gap'].includes(id)) {
      if (request?.data?.truncated || request?.data?.redacted) missing.push('当前请求正文截断或已脱敏，无法完整核对目标和限制');
      if (events.some(event => event.contextRole && (event.data?.truncated || event.data?.redacted))) missing.push('继承限制正文不完整');
    }
    if (id === 'request_drift' && events.some(event => ['output', 'tool-start'].includes(event.kind) && event.data?.redacted)) missing.push('操作正文已脱敏，缺少核对请求范围的依据');
    if (['request_drift', 'validation_gap'].includes(id) && coverage.contextGaps?.length) missing.push('当前请求或继承限制的来源尚未完整核实');
    if (['validation_cost', 'validation_gap'].includes(id) && coverage.verificationGaps?.length) missing.push('缺少验证范围、版本或结果证据：' + coverage.verificationGaps.join('；'));
    return [id, missing];
  }));
}
