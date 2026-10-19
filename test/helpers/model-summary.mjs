import assert from 'node:assert/strict';

// Only valid controlled model responses use this helper; malformed protocol cases stay literal.
export function withModelSummary(llmRequest, extraction) {
  assert.deepEqual(Object.keys(extraction).sort(), ['gaps', 'selectedOptionIds']);
  assert.ok(Array.isArray(extraction.selectedOptionIds) && Array.isArray(extraction.gaps));
  const { evidence } = JSON.parse(llmRequest.messages[1].content);
  const group = evidence.find(group => !group.isCurrentInput && group.options.length) ?? evidence.find(group => group.options.length);
  assert.ok(group, 'controlled summary requires actual supplied evidence');
  return { ...extraction, summary: { text: '已观察到本轮执行材料，具体检查仍以本轮判断为准。', evidenceOptionIds: [group.options[0].optionId] } };
}
