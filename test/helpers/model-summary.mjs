import assert from 'node:assert/strict';

export const startsSummaryRound = request => request.messages?.[2]?.content.startsWith('在做什么：') === true;

export function summaryLineForRequest(llmRequest, text) {
  const question = llmRequest.messages[2].content;
  const label = question.slice(0, question.indexOf('：') + 1);
  const line = text.split('\n').find(value => value.startsWith(label));
  assert.ok(label && line, 'summary fixture has a line for this request');
  return line;
}

// Only valid controlled model responses use this helper; malformed protocol cases stay literal.
export function withModelSummary(llmRequest, extraction, { legacy = false } = {}) {
  assert.deepEqual(Object.keys(extraction).sort(), ['gaps', 'selectedOptionIds']);
  assert.ok(Array.isArray(extraction.selectedOptionIds) && Array.isArray(extraction.gaps));
  const { evidence } = JSON.parse(llmRequest.messages[1].content);
  const group = evidence.find(group => !group.isCurrentInput && group.options.length) ?? evidence.find(group => group.options.length);
  assert.ok(group, 'controlled summary requires actual supplied evidence');
  const summary = '在做什么：处理当前任务。\n有什么进展：已观察到本轮执行材料。\n卡在哪里：未观察到明确阻碍。';
  const text = legacy ? summary : summaryLineForRequest(llmRequest, summary);
  return { ...extraction, summary: { text, evidenceOptionIds: [group.options[0].optionId] } };
}
