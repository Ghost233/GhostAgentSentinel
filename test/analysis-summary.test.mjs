import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createAnalyzer } from '../src/analysis/index.mjs';
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
const summaryText = '已核对所请求日志并找到最新记录。';

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
      const content = extraction ? await extraction(evidence, option) : { selectedOptionIds: [option.optionId], gaps: [], summary: { text: summaryText, evidenceOptionIds: [option.optionId] } };
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
  assert.deepEqual(result.summary.generated, { text: summaryText, evidenceIds: ['action'], evidenceRefs: [{ taskId: 'task', lifecycleId: 'run', eventId: 'action' }] });
  assert.deepEqual(requests.map(request => request.path), ['/v1/chat/completions', '/v1/systemone']);
  assert.equal(result.checks.length, 6);
  assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'unknown');
  assert.equal(result.checks.find(check => check.id === 'request_drift').judgmentState, 'insufficient');
  assert.ok(['currentTask', 'currentState', 'recentProgress', 'attention'].every(key => typeof result.summary[key] === 'string'));
});

test('invalid short-summary structure or sentence count fails the model round before judgment', async t => {
  const summaries = [null, [], {}, { text: '', evidenceOptionIds: ['placeholder'] }, { text: '   ', evidenceOptionIds: ['placeholder'] },
    { text: '第一句。第二句。第三句。', evidenceOptionIds: ['placeholder'] }, { text: '材'.repeat(601), evidenceOptionIds: ['placeholder'] },
    { text: summaryText, evidenceOptionIds: ['placeholder'], extra: 'unexpected' }];
  for (const summary of summaries) {
    const { analyzer, requests } = await models(t, { extraction: (_evidence, option) => ({ selectedOptionIds: [option.optionId], gaps: [], summary: summary?.evidenceOptionIds ? { ...summary, evidenceOptionIds: [option.optionId] } : summary }) });
    await assert.rejects(analyzer.analyze(observation), error => error.code === 'invalid_summary' && error.stage === 'llm');
    assert.deepEqual(requests.map(request => request.path), ['/v1/chat/completions']);
  }
});

test('a generated summary requires nonempty unique references from this round catalog', async t => {
  for (const mode of ['empty', 'duplicate', 'unknown', 'event-id', 'wrong-type']) {
    const { analyzer, requests } = await models(t, { extraction: (_evidence, option) => ({ selectedOptionIds: [option.optionId], gaps: [], summary: { text: summaryText,
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
    assert.match(result.summary.attention, /不足.*完整总结未发布/);
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
  const { analyzer } = await models(t, { extraction: (evidence, option) => ({ selectedOptionIds: [option.optionId], gaps: [], summary: { text: '已观察到当前操作及两项祖先用户限制。', evidenceOptionIds: [option.optionId, ...evidence.filter(group => group.contextRole === 'ancestor-limit').map(group => group.options[0].optionId)] } }) });
  const result = await analyzer.analyze({ ...observation, instructionContext: { inheritedEvents, binding: { origin: 'delegated' }, gaps: [] } });
  assert.deepEqual(result.summary.generated.evidenceIds, ['action', 'shared-limit']);
  assert.deepEqual(result.summary.generated.evidenceRefs, [{ taskId: 'task', lifecycleId: 'run', eventId: 'action' }, ...inheritedEvents.map(event => event.sourceRef)]);
});

test('a failed second model stays an attributed failure after the first model produced a valid summary', async t => {
  const { analyzer, requests } = await models(t, { judgment: () => ({ error: { code: 'controlled-judge-failure' } }) });
  await assert.rejects(analyzer.analyze(observation), error => error.code === 'http_error' && error.stage === 'jev');
  assert.deepEqual(requests.map(request => request.path), ['/v1/chat/completions', '/v1/systemone']);
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
