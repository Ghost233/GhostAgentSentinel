import assert from 'node:assert/strict';

// Only valid controlled model responses use this helper; malformed protocol cases stay literal.
export function withModelSummary(llmRequest, extraction) {
  assert.deepEqual(Object.keys(extraction).sort(), ['gaps', 'selectedOptionIds']);
  assert.ok(Array.isArray(extraction.selectedOptionIds) && Array.isArray(extraction.gaps));
  const { evidence } = JSON.parse(llmRequest.messages[1].content);
  const group = evidence.find(group => !group.isCurrentInput && group.options.length) ?? evidence.find(group => group.options.length);
  assert.ok(group, 'controlled summary requires actual supplied evidence');
  return { ...extraction, summary: { text: '在做什么：处理当前任务。\n有什么进展：已观察到本轮执行材料。\n卡在哪里：未观察到明确阻碍。', evidenceOptionIds: [group.options[0].optionId] } };
}
