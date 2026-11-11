import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createAnalyzer } from '../src/analysis/index.mjs';
import { summaryLineForRequest } from './helpers/model-summary.mjs';
import { decodeJevRequest } from './helpers/jev-wire.mjs';

const now = 1_800_000_000_000;
const observation = {
  task: { id: 'task', lifecycleId: 'run', status: 'running', currentRequestId: 'request' },
  source: { descriptor: { name: 'Controlled observation', gaps: [] } },
  events: [
    { id: 'request', taskId: 'task', taskLifecycleId: 'run', kind: 'input', text: 'Check the requested log.', capturedAt: now - 1000, occurredAt: now - 1000 },
    { id: 'action', taskId: 'task', taskLifecycleId: 'run', kind: 'output', text: 'Checked the requested log and found its latest entry.', capturedAt: now, occurredAt: now },
  ],
  coverage: { complete: true, gaps: [] },
};
const llmPaths = Array(3).fill('/v1/chat/completions');
const analysisPaths = [...llmPaths, '/v1/systemone'];
const summaryText = '在做什么：核对所请求日志。\n有什么进展：已找到最新记录。\n卡在哪里：未观察到明确阻碍。';

async function models(t, { extraction, judgment } = {}) {
  const requests = [];
  const server = createServer(async (request, response) => {
    if (request.method === 'GET') { response.writeHead(404, { 'content-type': 'application/json' }); response.end('{"error":{"code":"not_found"}}'); return; }
    let raw = ''; for await (const chunk of request) raw += chunk;
    const body = JSON.parse(raw);
    requests.push({ path: request.url, body });
    response.setHeader('content-type', 'application/json');
    if (request.url === '/v1/chat/completions') {
      const evidence = JSON.parse(body.messages[1].content).evidence;
      const option = evidence.find(group => group.origin === 'output')?.options.find(option => option.kind === 'action') ?? evidence[0].options[0];
      const content = extraction ? await extraction(evidence, option, body) : { selectedOptionIds: [option.optionId], gaps: [], summary: { text: summaryLineForRequest(body, summaryText), evidenceOptionIds: [option.optionId] } };
      response.end(JSON.stringify({ model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(content) } }] }));
    } else {
      const wire = decodeJevRequest(body);
      const result = judgment ? await judgment(wire) : { answers: Object.fromEntries(Object.keys(wire.questions).map(id => [id, { type: 'choice', choice: 'unknown' }])) };
      response.end(JSON.stringify({ model: body.model, ...result }));
    }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  const config = { llm: { baseUrl, model: 'summary' }, jev: { baseUrl, model: 'judge' }, now: () => now };
  return { requests, config, analyzer: createAnalyzer(config) };
}

test('two successful model calls publish the generated short summary with verified observation references', async t => {
  const { analyzer, requests } = await models(t);
  const result = await analyzer.analyze(observation);
  assert.equal(result.summary.generated.text, summaryText);
  assert.deepEqual(result.summary.generated.evidenceIds, ['action']);
  assert.deepEqual(result.summary.generated.evidenceRefs, [{ taskId: 'task', lifecycleId: 'run', eventId: 'action' }]);
  assert.deepEqual(requests.map(request => request.path), analysisPaths);
  assert.equal(result.checks.length, 6);
  assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'unknown');
  assert.equal(result.checks.find(check => check.id === 'request_drift').judgmentState, 'insufficient');
  assert.ok(['currentTask', 'currentState', 'recentProgress', 'attention'].every(key => typeof result.summary[key] === 'string'));
});

test('three LLM requests reuse identical evidence and supply one merged summary to JEV', async t => {
  const labels = ['在做什么', '有什么进展', '卡在哪里'];
  const lines = summaryText.split('\n');
  const { analyzer, requests } = await models(t, { extraction: (_evidence, option, body) => {
    const question = body.messages.at(-1).content;
    const index = labels.findIndex(label => question.startsWith(label + '：'));
    return { selectedOptionIds: [option.optionId], gaps: [], summary: { text: lines[index], evidenceOptionIds: [option.optionId] } };
  }, judgment: wire => {
    assert.equal(wire.state.executionSummary.text, summaryText);
    assert.ok(wire.state.events.some(event => event.id === 'action' && event.text === observation.events[1].text));
    return { answers: { request_drift: { type: 'choice', choice: 'unknown' }, execution_blocking: { type: 'choice', choice: 'unblocked_1' } } };
  } });
  const result = await analyzer.analyze(observation);
  assert.equal(result.summary.generated.text, summaryText);
  assert.equal(result.summary.generated.blocking.status, 'unblocked');
  assert.deepEqual(requests.map(request => request.path), analysisPaths);
  for (const [index, label] of labels.entries()) {
    const messages = requests[index].body.messages;
    assert.equal(messages.length, 3);
    assert.deepEqual(messages.slice(0, 2), requests[0].body.messages.slice(0, 2));
    assert.ok(messages[2].content.startsWith(label + '：'));
  }
  assert.deepEqual(result.summary.generated.evidenceRefs, [{ taskId: 'task', lifecycleId: 'run', eventId: 'action' }]);
  assert.equal(result.checks.find(check => check.id === 'request_drift').judgmentState, 'insufficient');
});

test('one JEV request decides whether work is blocked and selects its original evidence', async t => {
  for (const [choice, status] of [['blocked_1', 'blocked'], ['unblocked_1', 'unblocked'], ['unknown', 'unknown']]) await t.test(status, async t => {
    const action = status === 'blocked' ? { ...observation.events[1], kind: 'tool-result', text: '读取日志失败：权限被拒绝，无法继续核对。', data: { exitCode: 1 } }
      : { ...observation.events[1], kind: 'tool-start', text: '定向检查通过，正在运行完整测试。' };
    const { analyzer, requests } = await models(t, { judgment: wire => {
      assert.equal(wire.state.executionSummary.text, summaryText);
      assert.ok(wire.state.events.some(event => event.id === 'action' && event.text === action.text));
      assert.deepEqual(wire.state.candidates.execution_blocking.evidenceRefs, [{ taskId: 'task', lifecycleId: 'run', eventId: 'action' }]);
      assert.ok(wire.questions.execution_blocking.criteria[choice]);
      return { answers: { request_drift: { type: 'choice', choice: 'unknown' }, execution_blocking: { type: 'choice', choice } } };
    } });
    const result = await analyzer.analyze({ ...observation, events: [observation.events[0], action] });
    assert.deepEqual(requests.map(request => request.path), analysisPaths);
    assert.equal(result.summary.generated.blocking.status, status);
    if (status !== 'unknown') {
      assert.equal(result.summary.generated.blocking.reason, action.text);
      assert.deepEqual(result.summary.generated.blocking.evidenceRefs, [{ taskId: 'task', lifecycleId: 'run', eventId: 'action' }]);
    }
    assert.equal(result.checks.length, 6);
  });
});

test('an invalid second answer stops the round before a third request or JEV', async t => {
  const { analyzer, requests } = await models(t, { extraction: (_evidence, option, body) => ({
    selectedOptionIds: [option.optionId], gaps: [],
    summary: { text: summaryLineForRequest(body, summaryText), evidenceOptionIds: body.messages[2].content.startsWith('有什么进展：') ? ['foreign-option'] : [option.optionId] },
  }) });
  await assert.rejects(analyzer.analyze(observation), error => error.code === 'invalid_evidence' && error.stage === 'llm');
  assert.deepEqual(requests.map(request => request.path), llmPaths.slice(0, 2));
});

test('an incomplete observation publishes only a cited partial summary and preserves its gaps without a normal judgment', async t => {
  const { analyzer, requests } = await models(t);
  const result = await analyzer.analyze({ ...observation, coverage: { complete: false, gaps: ['missing interval'] } });
  assert.equal(result.summary.generated, undefined);
  assert.equal(result.summary.partial.text, summaryText);
  assert.equal(result.summary.partial.blocking.status, 'unknown');
  assert.equal(result.summary.partial.blocking.reason, '当前执行记录存在采集缺口');
  assert.deepEqual(result.summary.partial.evidenceRefs, [{ taskId: 'task', lifecycleId: 'run', eventId: 'action' }]);
  assert.equal(result.coverage.complete, false);
  assert.ok(result.coverage.gaps.includes('missing interval'));
  assert.ok(result.checks.every(check => check.status === 'unknown'));
  assert.deepEqual(requests.map(request => request.path), llmPaths);
});

test('normal excerpt limits still execute evidence-supported checks through JEV instead of stopping at a partial summary', async t => {
  const { analyzer, requests } = await models(t, { judgment: wire => ({ answers: Object.fromEntries(Object.keys(wire.questions).map(id => [id, { type: 'choice', choice: 'clear' }])) }) });
  const result = await analyzer.analyze({ ...observation,
    events: [observation.events[0], { ...observation.events[1], data: { truncated: true } }],
    coverage: { complete: false, gaps: ['Some observed excerpts are truncated'] },
  });
  assert.deepEqual(requests.map(request => request.path), analysisPaths);
  assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'clear');
  assert.equal(result.coverage.complete, false, 'a supported check does not pretend the original material was exhaustive');
  assert.ok(result.summary.partial, 'the bounded summary keeps its observation scope');
});

test('missing validation evidence blocks only its own checks while progress, drift and failure judgments still execute', async t => {
  const { analyzer, requests } = await models(t, { judgment: wire => ({ answers: Object.fromEntries(Object.keys(wire.questions).map(id => [id, { type: 'choice', choice: 'clear' }])) }) });
  const event = (id, kind, data) => ({ ...observation.events[1], id, kind, data });
  const result = await analyzer.analyze({ ...observation, events: [observation.events[0], observation.events[1],
    event('action2', 'output', {}), event('failure1', 'tool-result', { callId: 'one', exitCode: 1 }),
    event('failure2', 'tool-result', { callId: 'two', exitCode: 1, verificationGaps: ['native_file_change_contents_unverified'] }),
  ] });
  const jev = requests.find(request => request.path === '/v1/systemone');
  assert.ok(jev, 'JEV must run for unaffected questions');
  for (const id of ['request_drift', 'stalled_progress', 'failure_loop']) assert.equal(result.checks.find(check => check.id === id).status, 'clear');
  for (const id of ['validation_cost', 'validation_gap']) {
    const check = result.checks.find(check => check.id === id);
    assert.equal(check.status, 'unknown');
    assert.match(check.reason, /验证.*证据/);
  }
});

test('a clipped current request isolates scope checks without blocking comparison of actual failures', async t => {
  const { analyzer, requests } = await models(t, { judgment: wire => ({ answers: Object.fromEntries(Object.keys(wire.questions).map(id => [id, { type: 'choice', choice: 'clear' }])) }) });
  const result = await analyzer.analyze({ ...observation, events: [{ ...observation.events[0], data: { truncated: true } }, observation.events[1],
    ...['one', 'two'].map(id => ({ ...observation.events[1], id, kind: 'tool-result', data: { callId: id, exitCode: 1 } })),
  ] });
  assert.ok(requests.some(request => request.path === '/v1/systemone'));
  assert.equal(result.checks.find(check => check.id === 'failure_loop').status, 'clear');
  for (const id of ['request_drift', 'stalled_progress', 'validation_gap']) {
    const check = result.checks.find(check => check.id === id);
    assert.equal(check.status, 'unknown'); assert.match(check.reason, /请求正文截断/);
  }
});

test('retained additional_tools diagnostics from an older observer do not poison a new analysis, while unknown material still blocks it', async t => {
  for (const type of ['additional_tools', 'future_unknown_material']) await t.test(type, async t => {
    const { analyzer, requests } = await models(t);
    const reason = '未识别原生响应材料类型：' + type;
    const gap = { ...observation.events[1], id: 'old-diagnostic', kind: 'gap', text: reason, data: { sourceKind: 'chatgpt-native', nativeType: 'response_item/' + type, truncated: true } };
    await analyzer.analyze({ ...observation, events: [observation.events[0], gap, observation.events[1]], coverage: { complete: false, gaps: [reason] } });
    assert.equal(requests.some(request => request.path === '/v1/systemone'), type === 'additional_tools');
    assert.doesNotMatch(JSON.stringify(requests.find(request => request.path === '/v1/chat/completions').body.messages), /response_item\/additional_tools/);
  });
});

test('long observations select recent evidence within the unchanged budget while retaining the request and all inherited limits', async t => {
  const { analyzer, requests } = await models(t);
  const events = [observation.events[0], ...Array.from({ length: 180 }, (_, index) => ({ ...observation.events[1], id: 'action-' + index, text: ('Observed action ' + index + ': ').padEnd(230, 'x'), capturedAt: now - 180 + index }))];
  const inherited = { id: 'limit', taskId: 'parent', taskLifecycleId: 'parent-run', kind: 'input', text: 'Keep the inherited user restriction.', capturedAt: now - 2000, contextRole: 'ancestor-limit', authorizationEligible: true, sourceRef: { taskId: 'parent', lifecycleId: 'parent-run', eventId: 'limit' } };
  const result = await analyzer.analyze({ ...observation, events, instructionContext: { inheritedEvents: [inherited], binding: { origin: 'delegated' }, gaps: [] } });
  assert.equal(requests.length, 4, 'budgeted evidence reaches all three summaries and per-check judgment');
  assert.ok(JSON.stringify(requests.at(-1).body).length <= 24000);
  const judged = decodeJevRequest(requests.at(-1).body);
  assert.ok(judged.state.events.some(event => event.id === events.at(-1).id));
  assert.ok(judged.state.events.some(event => event.id === 'limit'));
  assert.ok(judged.state.events.length < events.length);
  assert.ok(JSON.stringify(requests[0].body).length <= 24000);
  const payload = JSON.parse(requests[0].body.messages[1].content);
  assert.ok(payload.evidence.some(group => group.isCurrentInput && group.text === observation.events[0].text));
  assert.ok(payload.evidence.some(group => group.contextRole === 'ancestor-limit' && group.text === inherited.text));
  assert.ok(payload.evidence.some(group => group.text === events.at(-1).text));
  assert.ok(payload.evidence.length < events.length);
  assert.ok(result.coverage.gaps.includes('analysis_evidence_budget'));
  assert.ok(result.coverage.analysisOmittedEvents > 0);
  assert.equal(result.coverage.complete, false);
  assert.ok(result.summary.partial);
  assert.equal(result.summary.generated, undefined);
  assert.equal(events.length, 181, 'input observations are not pruned or rewritten');
});

test('missing, reordered or empty summary levels fail the model round before judgment', async t => {
  const summaries = [null, [], {}, { text: '', evidenceOptionIds: ['placeholder'] }, { text: '   ', evidenceOptionIds: ['placeholder'] },
    { text: '第一句。第二句。第三句。', evidenceOptionIds: ['placeholder'] }, { text: '材'.repeat(601), evidenceOptionIds: ['placeholder'] },
    { text: '在做什么：核对日志。\n有什么进展：找到记录。', evidenceOptionIds: ['placeholder'] },
    { text: '有什么进展：找到记录。\n在做什么：核对日志。\n卡在哪里：未观察到明确阻碍。', evidenceOptionIds: ['placeholder'] },
    { text: '在做什么：核对日志。\n有什么进展：  \n卡在哪里：未观察到明确阻碍。', evidenceOptionIds: ['placeholder'] },
    { text: summaryText, evidenceOptionIds: ['placeholder'], extra: 'unexpected' }];
  for (const summary of summaries) {
    const { analyzer, requests } = await models(t, { extraction: (_evidence, option) => ({ selectedOptionIds: [option.optionId], gaps: [], summary: summary?.evidenceOptionIds ? { ...summary, evidenceOptionIds: [option.optionId] } : summary }) });
    await assert.rejects(analyzer.analyze(observation), error => error.code === 'invalid_summary' && error.stage === 'llm');
    assert.deepEqual(requests.map(request => request.path), ['/v1/chat/completions']);
  }
});

test('a generated summary requires nonempty unique references from this round catalog', async t => {
  for (const mode of ['empty', 'duplicate', 'unknown', 'event-id', 'wrong-type']) {
    const { analyzer, requests } = await models(t, { extraction: (_evidence, option, body) => ({ selectedOptionIds: [option.optionId], gaps: [], summary: { text: summaryLineForRequest(body, summaryText),
      evidenceOptionIds: mode === 'empty' ? [] : mode === 'duplicate' ? [option.optionId, option.optionId] : mode === 'event-id' ? ['action'] : mode === 'wrong-type' ? [4] : ['foreign-event-option'],
    } }) });
    await assert.rejects(analyzer.analyze(observation), error => error.code === 'invalid_evidence' && error.stage === 'llm');
    assert.deepEqual(requests.map(request => request.path), ['/v1/chat/completions']);
  }
});

test('the previous evidence-only model response is an explicit protocol failure', async t => {
  const { analyzer, requests } = await models(t, { extraction: (_evidence, option) => ({ selectedOptionIds: [option.optionId], gaps: [] }) });
  await assert.rejects(analyzer.analyze(observation), error => error.code === 'invalid_extraction' && error.stage === 'llm');
  assert.deepEqual(requests.map(request => request.path), ['/v1/chat/completions']);
});

test('a pending or invalid judgment cannot publish the summary produced by the first model', async t => {
  let release, entered;
  const pending = new Promise(resolve => { release = resolve; });
  const received = new Promise(resolve => { entered = resolve; });
  let valid = false;
  const { analyzer } = await models(t, { judgment: async wire => {
    if (!valid) { entered(); await pending; return { answers: {} }; }
    return { answers: Object.fromEntries(Object.keys(wire.questions).map(id => [id, { type: 'choice', choice: 'clear' }])) };
  } });
  let settled = false;
  const round = analyzer.analyze(observation).finally(() => { settled = true; });
  const rejection = assert.rejects(round, error => error.code === 'invalid_decision' && error.stage === 'jev');
  await received;
  assert.equal(settled, false);
  release();
  await rejection;
  valid = true;
  assert.equal((await analyzer.analyze(observation)).summary.generated.text, summaryText);
});

test('missing either model or an ineligible evidence round preserves observations without publishing a complete summary', async t => {
  const { config, analyzer, requests } = await models(t);
  for (const missing of ['llm', 'jev']) {
    const result = await createAnalyzer({ ...config, [missing]: {} }).analyze(observation);
    assert.equal(result.summary.generated, undefined);
    assert.deepEqual(result.coverage.dependencies, [`${missing}_unconfigured`]);
  }
  assert.equal(requests.length, 0);
  const old = await analyzer.analyze(observation);
  for (const input of [{ ...observation, events: [observation.events[0]] }, { ...observation, coverage: { complete: false, gaps: ['missing interval'] } }]) {
    const result = await analyzer.analyze(input);
    assert.equal(result.summary.generated, undefined);
    assert.match(result.summary.attention, /不足.*部分观察总结/);
  }
  assert.equal(old.summary.generated.text, summaryText);
});

test('summary option references resolve only to the current request even when previous material has identical text', async t => {
  const oldRequest = { ...observation.events[0], id: 'previous-request', capturedAt: now - 3000 };
  const oldAction = { ...observation.events[1], id: 'previous-action', capturedAt: now - 2000 };
  const { analyzer, requests } = await models(t);
  const result = await analyzer.analyze({ ...observation, events: [oldRequest, oldAction, ...observation.events] });
  assert.deepEqual(result.summary.generated.evidenceRefs, [{ taskId: 'task', lifecycleId: 'run', eventId: 'action' }]);
  const payload = JSON.parse(requests[0].body.messages[1].content);
  assert.equal(payload.evidence.filter(group => group.origin === 'output').length, 1);
  assert.ok(payload.evidence.every(group => !Object.hasOwn(group, 'eventId')));
  assert.ok(!Object.hasOwn(payload.task, 'currentRequestId'));
});

test('summary references retain distinct inherited owners when their local evidence ids collide', async t => {
  const inheritedEvents = ['parent', 'ancestor'].map(owner => ({ id: 'shared-limit', taskId: owner, taskLifecycleId: `${owner}-run`, kind: 'input', text: `${owner} user limit.`, capturedAt: now - 2000, contextRole: 'ancestor-limit', authorizationEligible: true, sourceRef: { taskId: owner, lifecycleId: `${owner}-run`, eventId: 'shared-limit' } }));
  const { analyzer } = await models(t, { extraction: (evidence, option, body) => ({ selectedOptionIds: [option.optionId], gaps: [], summary: { text: summaryLineForRequest(body, summaryText), evidenceOptionIds: [option.optionId, ...evidence.filter(group => group.contextRole === 'ancestor-limit').map(group => group.options[0].optionId)] } }) });
  const result = await analyzer.analyze({ ...observation, instructionContext: { inheritedEvents, binding: { origin: 'delegated' }, gaps: [] } });
  assert.deepEqual(result.summary.generated.evidenceIds, ['action', 'shared-limit']);
  assert.deepEqual(result.summary.generated.evidenceRefs, [{ taskId: 'task', lifecycleId: 'run', eventId: 'action' }, ...inheritedEvents.map(event => event.sourceRef)]);
});

test('a failed second model stays an attributed failure after the first model produced a valid summary', async t => {
  const { analyzer, requests } = await models(t, { judgment: () => ({ error: { code: 'controlled-judge-failure' } }) });
  await assert.rejects(analyzer.analyze(observation), error => error.code === 'http_error' && error.stage === 'jev');
  assert.deepEqual(requests.map(request => request.path), analysisPaths);
});

test('cancelling during judgment seals the generated text and a later round remains independent', async t => {
  let release, entered, calls = 0;
  const pending = new Promise(resolve => { release = resolve; });
  const received = new Promise(resolve => { entered = resolve; });
  const { analyzer } = await models(t, { judgment: async wire => {
    if (calls++ === 0) { entered(); await pending; }
    return { answers: Object.fromEntries(Object.keys(wire.questions).map(id => [id, { type: 'choice', choice: 'unknown' }])) };
  } });
  const controller = new AbortController();
  const round = analyzer.analyze(observation, { signal: controller.signal });
  await received;
  controller.abort(new DOMException('Cancelled observation round', 'AbortError'));
  try { await assert.rejects(round, error => error.name === 'AbortError'); } finally { release(); }
  const next = await analyzer.analyze(observation);
  assert.equal(next.summary.generated.text, summaryText);
  assert.equal(next.checks.find(check => check.id === 'request_drift').status, 'unknown');
});
