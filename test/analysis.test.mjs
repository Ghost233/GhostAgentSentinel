import assert from 'node:assert/strict';
import test from 'node:test';
import { createServer } from 'node:http';
import { createAnalyzer } from '../src/analysis/index.mjs';
import { qualityCases } from '../probes/modeldeck/quality-cases.mjs';
import { decodeJevRequest } from './helpers/jev-wire.mjs';
import { withModelSummary } from './helpers/model-summary.mjs';

const at = 1_800_000_000_000;
const task = { id: 'a', lifecycleId: 'life-a', title: 'Add a log', status: 'running' };
const input = {
  task,
  source: { descriptor: { name: 'DSH', gaps: [] } },
  events: [{ id: 'request', taskId: 'a', taskLifecycleId: 'life-a', kind: 'input', occurredAt: at, capturedAt: at, text: 'Add a log' }],
  coverage: { complete: true, gaps: [] },
};

test('unconfigured inference preserves semantic checks as unknown without fabricating normality', async () => {
  const result = await createAnalyzer({ now: () => at }).analyze(input);
  assert.equal(result.quality, 'unvalidated');
  assert.equal(result.checks.length, 6);
  assert.ok(result.checks.filter(check => check.id !== 'long_wait').every(check => check.status === 'unknown'));
  assert.equal(result.checks.find(check => check.id === 'long_wait').status, 'clear');
  assert.deepEqual(result.coverage.dependencies, ['llm_unconfigured', 'jev_unconfigured']);
  assert.match(result.summary.attention, /未配置/);
});

test('a core-classified delegated user-labelled input never offers authorization to the extractor', async t => {
  const delegated = { ...input.events[0], authorizationEligible: false };
  const models = await withModels(t, { facts: [], gaps: [] });
  await models.analyzer().analyze({ ...input, events: [delegated] });
  const groups = JSON.parse(models.requests.find(request => request.path === '/v1/chat/completions').body.messages[1].content).evidence;
  assert.ok(groups.every(group => group.options.every(option => option.kind !== 'authorization')));
});

async function withModels(t, extraction, choices = {}, handler) {
  const requests = [];
  let fixtureEvents = new Map();
  let rawFixtureEvents = [];
  const server = createServer(async (req, res) => {
    if (req.method === 'GET') { res.writeHead(404, { 'content-type': 'application/json' }); res.end('{"error":{"code":"not_found"}}'); return; }
    let body = '';
    for await (const chunk of req) body += chunk;
    const wire = JSON.parse(body);
    const json = req.url === '/v1/systemone' ? decodeJevRequest(wire) : wire;
    requests.push({ path: req.url, body: json });
    if (handler && await handler(req, res, json)) return;
    res.setHeader('content-type', 'application/json');
    if (req.url === '/v1/chat/completions') {
      const evidence = JSON.parse(json.messages[1].content).evidence;
      const selectedOptionIds = extraction.facts.map(fact => {
        const fixtureEvent = fixtureEvents.get(fact.eventId);
        const unambiguous = fixtureEvent && rawFixtureEvents.filter(event => event.text === fixtureEvent.text && event.kind === fixtureEvent.kind).length === 1;
        const eligible = unambiguous && fact.quote?.trim() && fixtureEvent.text.includes(fact.quote);
        const matching = eligible ? evidence.filter(entry => entry.text === fixtureEvent.text && entry.origin === fixtureEvent.kind && entry.options.some(option => option.kind === fact.kind)) : [];
        return matching.length === 1 ? matching[0].options.find(option => option.kind === fact.kind).optionId : `invalid-fixture:${fact.eventId}:${fact.kind}`;
      });
      res.end(JSON.stringify({ model: 'extractor', choices: [{ message: { content: JSON.stringify(withModelSummary(json, { selectedOptionIds, gaps: extraction.gaps })) }, finish_reason: 'stop' }], usage: { prompt_tokens: 12, completion_tokens: 8 } }));
    } else {
      const answers = Object.fromEntries(Object.keys(json.questions).map(id => [id, { type: 'choice', choice: choices[id] ?? 'unknown', confidence: 0.98 }]));
      res.end(JSON.stringify({ model: 'judge', answers, usage: { input_tokens: 20, output_tokens: 0 } }));
    }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  return { requests, config: { llm: { baseUrl, model: 'extractor' }, jev: { baseUrl, model: 'judge' }, now: () => at },
    analyzer(extra = {}) {
      const analyzer = createAnalyzer({ ...this.config, ...extra });
      return { analyze(input, options) { rawFixtureEvents = input.events; fixtureEvents = new Map(input.events.map(event => [event.id, event])); return analyzer.analyze(input, options); } };
    },
  };
}

function event(id, kind, text, data, offset = 0) {
  const result = { id, taskId: task.id, taskLifecycleId: task.lifecycleId, kind, text, occurredAt: at + offset, capturedAt: at + offset, data };
  return result;
}

test('six checks distinguish a proved absent trigger from triggered clear or semantic unknown', async t => {
  const models = await withModels(t, { facts: [], gaps: [] }, { request_drift: 'clear' })
  const observed = { ...input, task: { ...task, currentRequestId: 'request' }, events: [input.events[0], event('action', 'output', 'Read the requested log')] }
  let result = await models.analyzer().analyze(observed)
  assert.equal(result.checks.find(check => check.id === 'request_drift').applicability.state, 'triggered')
  assert.equal(result.checks.find(check => check.id === 'request_drift').judgmentState, 'clear')
  for (const id of ['stalled_progress', 'failure_loop', 'validation_cost', 'validation_gap']) {
    const check = result.checks.find(check => check.id === id)
    assert.equal(check.applicability.state, 'not-triggered', id)
    assert.ok(check.applicability.evidenceIds.includes('request'), 'an absent trigger is scoped to retained evidence')
    assert.equal(check.judgmentState, 'not-triggered')
  }
  const unknownModels = await withModels(t, { facts: [], gaps: [] }, { request_drift: 'unknown' })
  result = await unknownModels.analyzer().analyze(observed)
  assert.equal(result.checks.find(check => check.id === 'request_drift').applicability.state, 'triggered')
  assert.equal(result.checks.find(check => check.id === 'request_drift').judgmentState, 'insufficient')
  result = await models.analyzer().analyze({ ...observed, coverage: { complete: false, gaps: ['missing observation'] } })
  assert.ok(result.checks.every(check => check.applicability.state === 'unknown'))
})

test('native user waits exclude only their recorded intervals from the matching long tool wait', async () => {
  const native = (id, type, offset, extra = {}) => event(id, 'status', id, { nativeType: type, approvalId: 'approval', callId: 'matching', ...extra }, offset)
  const observed = { ...input, task: { ...task, currentRequestId: 'request' }, events: [event('request', 'input', 'Approve then continue', {}, -600_000), event('start', 'tool-start', 'Tool start', { callId: 'matching' }, -400_000), native('asked', 'approval/asked', -300_000)] }
  const analyzer = createAnalyzer({ now: () => at })
  let result = await analyzer.analyze(observed)
  assert.equal(result.checks.find(check => check.id === 'long_wait').status, 'clear', 'a hundred seconds of work plus a proved user wait is not a four-hundred-second tool delay')
  result = await analyzer.analyze({ ...observed, events: [...observed.events, native('decided', 'approval/decided', -20_000)] })
  assert.equal(result.checks.find(check => check.id === 'long_wait').status, 'clear', 'the closed user waiting interval remains excluded')
  result = await analyzer.analyze({ ...observed, events: [...observed.events, event('unrelated-start', 'tool-start', 'Another stuck tool', { callId: 'other' }, -360_000)] })
  assert.equal(result.checks.find(check => check.id === 'long_wait').status, 'attention')
  assert.deepEqual(result.checks.find(check => check.id === 'long_wait').evidenceIds, ['unrelated-start'])
  result = await analyzer.analyze({ ...observed, events: [observed.events[0], event('independently-overdue', 'tool-start', 'Already overdue before asking', { callId: 'matching' }, -700_000), native('asked', 'approval/asked', -300_000)] })
  assert.equal(result.checks.find(check => check.id === 'long_wait').status, 'attention', 'a wait does not erase an independently overdue tool before its start')
})

test('missing outcome metadata on repeated completed calls cannot prove the failure-loop trigger absent', async t => {
  const models = await withModels(t, { facts: [], gaps: [] }, { request_drift: 'clear', stalled_progress: 'clear' })
  const result = await models.analyzer().analyze({ ...input, events: [...input.events,
    event('one-start', 'tool-start', 'First execution', { callId: 'one' }), event('one-result', 'tool-result', 'First opaque outcome', { callId: 'one' }),
    event('two-start', 'tool-start', 'Second execution', { callId: 'two' }), event('two-result', 'tool-result', 'Second opaque outcome', { callId: 'two' }),
  ] })
  const check = result.checks.find(check => check.id === 'failure_loop')
  assert.equal(check.status, 'unknown')
  assert.equal(check.applicability.state, 'unknown')
  assert.equal(check.judgmentState, 'insufficient')
})

test('a semantic clear decision does not recover an older issue unless a separate related-result judgment confirms it', async t => {
  const observed = { ...input, task: { ...task, currentRequestId: 'request', activeAlerts: [{ id: 'old-alert', checkId: 'request_drift', reason: 'Changed an unrelated module', evidenceIds: ['action'] }] }, events: [...input.events, event('action', 'output', 'Changed an unrelated module'), event('new-result', 'tool-result', 'Restored the unrelated module and verified the requested change', { callId: 'repair', exitCode: 0 })] }
  const positive = await withModels(t, { facts: [], gaps: [] }, { request_drift: 'clear', recover_request_drift: 'clear' })
  let result = await positive.analyzer().analyze(observed)
  const clear = result.checks.find(check => check.id === 'request_drift')
  assert.equal(clear.recoveryFor, 'old-alert')
  assert.deepEqual(clear.recoveryEvidenceIds, ['new-result'])
  assert.equal(positive.requests.find(request => request.path === '/v1/systemone').body.state.previousAlerts[0].id, 'old-alert')
  const negative = await withModels(t, { facts: [], gaps: [] }, { request_drift: 'clear', recover_request_drift: 'unknown' })
  result = await negative.analyzer().analyze(observed)
  assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'clear')
  assert.equal(result.checks.find(check => check.id === 'request_drift').recoveryFor, undefined)
})

test('an empty current request stays semantically unknown without asking a model to quote its title', async t => {
  for (const text of ['', ' \t\n']) {
    const models = await withModels(t, { facts: [{ kind: 'request', eventId: 'request', quote: task.title }], gaps: [] });
    const result = await models.analyzer().analyze({ ...input, task: { ...task, currentRequestId: 'request' }, events: [event('request', 'input', text), event('action', 'output', 'Observed activity without a visible request')] });
    assert.equal(models.requests.length, 0);
    assert.ok(result.checks.filter(check => check.id !== 'long_wait').every(check => check.status === 'unknown' && /正文/.test(check.reason)));
    assert.equal(result.coverage.complete, false);
    assert.ok(result.coverage.gaps.includes('current_request_text_missing'));
    assert.match(result.summary.currentTask, /正文/);
    assert.match(result.summary.attention, /正文/);
  }
});

test('a nonempty current request with the same title still uses verified Chat and JEV evidence', async t => {
  const action = event('action', 'output', 'I am adding the requested log');
  const models = await withModels(t, { facts: [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'action', eventId: action.id, quote: action.text }], gaps: [] }, { request_drift: 'clear' });
  const result = await models.analyzer().analyze({ ...input, task: { ...task, currentRequestId: 'request' }, events: [...input.events, action] });
  assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'clear');
  assert.deepEqual(models.requests.map(request => request.path), ['/v1/chat/completions', '/v1/systemone']);
  assert.equal(result.coverage.complete, true);
});

test('an empty current request preserves a grounded long tool wait before semantic early return', async t => {
  const waiting = event('waiting', 'tool-start', 'A tool remains in progress', { callId: 'pending' }, -360_000);
  const models = await withModels(t, { facts: [{ kind: 'request', eventId: 'request', quote: task.title }], gaps: [] });
  const result = await models.analyzer().analyze({ ...input, task: { ...task, currentRequestId: 'request' }, events: [event('request', 'input', '', {}, -400_000), waiting] });
  assert.equal(models.requests.length, 0);
  assert.equal(result.checks.find(check => check.id === 'long_wait').status, 'attention');
  assert.deepEqual(result.checks.find(check => check.id === 'long_wait').evidenceIds, [waiting.id]);
  assert.ok(result.checks.filter(check => check.id !== 'long_wait').every(check => check.status === 'unknown'));
  assert.ok(result.coverage.gaps.includes('current_request_text_missing'));
});

test('verified request and action quotes reach public JEV and can raise first-round drift', async t => {
  const extraction = { facts: [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'action', eventId: 'action', quote: 'I am redesigning the database' }], gaps: [] };
  const models = await withModels(t, extraction, { request_drift: 'attention' });
  const result = await models.analyzer().analyze({ ...input, events: [...input.events, event('action', 'output', 'I am redesigning the database')] });
  const drift = result.checks.find(check => check.id === 'request_drift');
  assert.equal(drift.status, 'attention');
  assert.deepEqual(drift.evidenceIds, ['request', 'action']);
  assert.equal(models.requests.length, 2);
  assert.equal(models.requests[0].path, '/v1/chat/completions');
  assert.equal(models.requests[0].body.stream, false);
  assert.equal(models.requests[1].path, '/v1/systemone');
  const state = models.requests[1].body.state;
  assert.equal(state.facts[1].quote, undefined);
  assert.equal(state.events.find(event => event.id === state.facts[1].eventId).text, 'I am redesigning the database');
  assert.equal(result.usage.llm.prompt_tokens, 12);
  assert.equal(result.quality, 'unvalidated');
});

test('JEV receives only evidence selectors while its attention result retains the local UI suspicion', async t => {
  const action = event('action', 'output', 'I am redesigning the database');
  const models = await withModels(t, { facts: [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'action', eventId: action.id, quote: action.text }], gaps: [] }, { request_drift: 'attention' });
  const result = await models.analyzer().analyze({ ...input, events: [...input.events, action] });
  const selector = models.requests.find(request => request.path === '/v1/systemone').body.state.candidates.request_drift;
  assert.deepEqual(selector, { evidenceIds: ['request', action.id] });
  const drift = result.checks.find(check => check.id === 'request_drift');
  assert.equal(drift.status, 'attention');
  assert.equal(drift.reason, '最新动作疑似超出当前请求；请核对是否有必要前置或新授权');
  assert.deepEqual(drift.evidenceIds, ['request', action.id]);
});

test('raw current activity qualifies drift review with input-only or empty valid selections', async t => {
  const activity = event('activity', 'output', 'Observed current action');
  for (const selection of ['input-only', 'empty']) for (const decision of ['attention', 'clear', 'unknown']) {
    const models = await withModels(t, { facts: selection === 'input-only' ? [{ kind: 'context', eventId: 'request', quote: 'Add a log' }] : [], gaps: [] }, { request_drift: decision });
    const result = await models.analyzer().analyze({ ...input, events: [...input.events, activity] });
    assert.equal(result.checks.find(check => check.id === 'request_drift').status, decision);
    const jev = models.requests.find(request => request.path === '/v1/systemone').body;
    assert.ok(jev.questions.request_drift);
    assert.ok(!jev.state.facts.some(fact => fact.kind === 'action'));
    assert.deepEqual(result.checks.find(check => check.id === 'request_drift').evidenceIds, ['request', activity.id]);
  }
});

test('drift review remains unavailable without nonempty current activity, request or coverage', async t => {
  const activity = event('activity', 'tool-start', 'Observed tool action');
  for (const missing of ['activity', 'empty-activity', 'result-only', 'request', 'empty-request', 'expired-request', 'coverage']) {
    const current = missing === 'expired-request' ? event('request', 'input', 'Add a log', {}, -86_400_000) : event('request', 'input', missing === 'empty-request' ? '' : 'Add a log');
    const events = [current, ...(missing === 'activity' ? [] : [missing === 'empty-activity' ? { ...activity, text: '  ' } : missing === 'result-only' ? event('result', 'tool-result', 'Completed tool') : activity])];
    const models = await withModels(t, { facts: [], gaps: [] }, { request_drift: 'attention' });
    const result = await models.analyzer().analyze({ ...input, task: { ...task, currentRequestId: missing === 'request' ? 'absent-input' : current.id }, events, coverage: missing === 'coverage' ? { complete: false, gaps: ['Capture gap'] } : input.coverage });
    assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'unknown', missing);
    assert.ok(!models.requests.some(request => request.path === '/v1/systemone' && request.body.questions.request_drift), missing);
  }
});

test('out-of-order inputs and same-time backfill use the core request identity', async t => {
  const old = event('old-input', 'input', 'Old request', {}, -100);
  const current = event('current-input', 'input', 'Current request');
  const backfill = event('backfill-input', 'input', 'Backfilled request');
  const action = event('action', 'output', 'Current action');
  const models = await withModels(t, { facts: [{ kind: 'request', eventId: current.id, quote: current.text }, { kind: 'action', eventId: action.id, quote: action.text }], gaps: [] }, { request_drift: 'clear' });
  const result = await models.analyzer().analyze({ ...input, task: { ...task, currentRequestId: current.id }, events: [current, action, backfill, old] });
  assert.equal(result.summary.currentTask, 'Current request');
  assert.equal(JSON.parse(models.requests[0].body.messages[1].content).evidence.find(entry => entry.isCurrentInput).text, current.text);
  assert.equal(models.requests[1].body.state.request, 'Current request');
  assert.deepEqual(result.checks.find(check => check.id === 'request_drift').evidenceIds, ['current-input', 'action', 'backfill-input']);
});

test('a missing retained request anchor stays unknown rather than selecting an older input', async t => {
  const old = event('old-input', 'input', 'Old request');
  const action = event('action', 'output', 'Old action');
  const models = await withModels(t, { facts: [{ kind: 'request', eventId: old.id, quote: old.text }, { kind: 'action', eventId: action.id, quote: action.text }], gaps: [] }, { request_drift: 'attention' });
  const result = await models.analyzer().analyze({ ...input, task: { ...task, currentRequestId: 'expired-input', currentRequest: 'Current request' }, events: [old, action] });
  assert.equal(result.summary.currentTask, 'Current request');
  assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'unknown');
  assert.equal(models.requests.filter(request => request.path === '/v1/systemone').length, 0);
  assert.ok(result.coverage.gaps.includes('current_request_evidence_missing'));
});

test('independent analysis without a core anchor falls back to stable capture-time order', async t => {
  const old = event('old-input', 'input', 'Old request', {}, -100);
  const current = event('current-input', 'input', 'Current request');
  const models = await withModels(t, { facts: [], gaps: [] });
  const result = await models.analyzer().analyze({ ...input, events: [current, old] });
  assert.equal(result.summary.currentTask, 'Current request');
});

test('old gaps and truncated history do not poison the new core-anchored request or its budget', async t => {
  const current = event('current-request', 'input', 'Add a log');
  const action = event('current-action', 'output', 'Redesign the unrelated database');
  const events = [
    event('expired-old', 'output', 'expired material', {}, -86_400_000),
    event('old-input', 'input', 'Earlier request', {}, -5000),
    event('old-truncated', 'output', 'x'.repeat(30_000), { truncated: true, redacted: true }, -4000),
    event('old-gap', 'gap', 'Old capture gap', {}, -3000),
    current, action,
  ];
  const models = await withModels(t, { facts: [{ kind: 'request', eventId: current.id, quote: current.text }, { kind: 'action', eventId: action.id, quote: action.text }], gaps: [] }, { request_drift: 'attention' });
  const result = await models.analyzer().analyze({ ...input, task: { ...task, currentRequestId: current.id, gaps: ['Old capture gap'] }, events });
  assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'attention');
  assert.equal(result.coverage.complete, true);
  assert.deepEqual(JSON.parse(models.requests[0].body.messages[1].content).evidence.map(entry => entry.text), [current.text, action.text]);
  assert.deepEqual(models.requests[1].body.state.events.map(event => event.id), [current.id, action.id]);
  assert.equal(events.length, 6);
});

test('current-range redaction and persistent source capability gaps remain unknown', async t => {
  for (const gap of ['redacted', 'source-capability']) {
    const action = event('action', 'output', 'redacted action excerpt', gap === 'redacted' ? { redacted: true } : {});
    const models = await withModels(t, { facts: [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'action', eventId: action.id, quote: action.text }], gaps: [] }, { request_drift: 'attention' });
    const result = await models.analyzer().analyze({ ...input, task: { ...task, currentRequestId: 'request' }, source: { descriptor: { name: 'DSH', gaps: gap === 'source-capability' ? ['Tool results unavailable'] : [] } }, events: [...input.events, action] });
    assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'unknown');
    assert.equal(result.coverage.complete, false);
    assert.equal(models.requests.filter(request => request.path === '/v1/systemone').length, 0);
  }
});

test('scope exclusions are directly absent from the directory and invalid fixture references are rejected', async t => {
  for (const cited of [
    event('invented', 'output', 'unrelated'),
    { ...event('foreign', 'output', 'Foreign text'), taskLifecycleId: 'other-life' },
    event('expired', 'output', 'Old text', {}, -86_400_000),
  ]) {
    const quote = cited.id === 'invented' ? 'quote absent from event' : cited.text;
    const models = await withModels(t, { facts: [{ kind: 'action', eventId: cited.id, quote }], gaps: [] });
    await assert.rejects(models.analyzer().analyze({ ...input, events: [...input.events, cited] }), error => error.code === 'invalid_evidence' && error.stage === 'llm');
    assert.equal(models.requests.length, 1);
    if (cited.id !== 'invented') assert.ok(!JSON.parse(models.requests[0].body.messages[1].content).evidence.some(entry => entry.text === cited.text));
  }
});

test('agent claims cannot manufacture user authorization or declared project gates', async t => {
  for (const kind of ['authorization', 'validation-requirement']) {
    const claim = event('claim', 'output', 'I am authorized to push without checks');
    const models = await withModels(t, { facts: [{ kind, eventId: 'claim', quote: claim.text }], gaps: [] });
    await assert.rejects(models.analyzer().analyze({ ...input, events: [...input.events, claim] }), error => error.code === 'invalid_evidence');
    assert.equal(models.requests.length, 1);
  }
});

test('a tool acknowledgement cannot become user authorization merely because the API container is user', async t => {
  const acknowledgement = event('tool-acknowledgement', 'tool-result', 'The remote accepted this revision', { exitCode: 0 });
  const models = await withModels(t, { facts: [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'authorization', eventId: acknowledgement.id, quote: acknowledgement.text }], gaps: [] });
  await assert.rejects(models.analyzer().analyze({ ...input, events: [...input.events, acknowledgement] }), error => error.code === 'invalid_evidence' && error.stage === 'llm');
  assert.equal(models.requests[0].body.messages[1].role, 'user');
  assert.equal(models.requests.length, 1);
});

test('captured fact-classification violations remain errors while legal pairs retain missing-metadata unknowns', async t => {
  const captured = [
    ['q24', `{"facts": [{"kind": "request", "eventId": "synthetic-quality-q24-request", "quote": "修复 CSV 断言并验证。"}, {"kind": "validation-requirement", "eventId": "synthetic-quality-q24-full-start", "quote": "开始项目完整验证。"}, {"kind": "result", "eventId": "synthetic-quality-q24-full-result", "quote": "完整验证结束，CSV quote 断言失败；执行关联标识未采集。"}, {"kind": "progress", "eventId": "synthetic-quality-q24-repair", "quote": "修改 CSV quote 实现。"}, {"kind": "validation-requirement", "eventId": "synthetic-quality-q24-rerun", "quote": "再次开始项目完整验证。"}], "gaps": []}`],
    ['q30', `{"facts": [{"kind": "request", "eventId": "synthetic-quality-q30-request", "quote": "修改 CSV 导出；项目要求当前推送源码版本的全部测试与 lint 通过。"}, {"kind": "validation-requirement", "eventId": "synthetic-quality-q30-request", "quote": "项目要求当前推送源码版本的全部测试与 lint 通过。"}, {"kind": "action", "eventId": "synthetic-quality-q30-verify-start", "quote": "运行记录版本的完整门禁。"}, {"kind": "result", "eventId": "synthetic-quality-q30-verify-result", "quote": "记录版本的全部测试与 lint 通过。"}, {"kind": "action", "eventId": "synthetic-quality-q30-action", "quote": "执行推送命令，采集未取得被推送源码身份。"}, {"kind": "tool-start", "eventId": "synthetic-quality-q30-push", "quote": "推送命令开始。"}, {"kind": "tool-result", "eventId": "synthetic-quality-q30-push-result", "quote": "推送命令结束，源码版本字段未采集。"}], "gaps": []}`],
  ];
  const cases = qualityCases(at);
  for (const [id, content] of captured) {
    const sample = cases.find(sample => sample.id === id);
    const illegal = await withModels(t, null, {}, async (req, res) => {
      if (req.url !== '/v1/chat/completions') return false;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ model: 'extractor', choices: [{ message: { content }, finish_reason: 'stop' }] }));
      return true;
    });
    await assert.rejects(illegal.analyzer().analyze(sample.input), error => error.code === 'invalid_extraction' && error.stage === 'llm');
    assert.equal(illegal.requests.filter(request => request.path === '/v1/systemone').length, 0);

    // A separate legal fixture response, not an automatic repair of the captured response.
    const facts = sample.input.events.map(event => ({ kind: event.kind === 'input' ? 'request' : event.kind === 'tool-result' ? 'result' : 'action', eventId: event.id, quote: event.text }));
    if (id === 'q30') facts.push({ kind: 'validation-requirement', eventId: sample.input.events[0].id, quote: '项目要求当前推送源码版本的全部测试与 lint 通过。' });
    const gateMaterial = event('project-gate-material', 'tool-result', 'Project policy requires local verification before publication');
    const legalInput = id === 'q24' ? { ...sample.input, events: [...sample.input.events, { ...gateMaterial, taskId: sample.input.task.id, taskLifecycleId: sample.input.task.lifecycleId }] } : sample.input;
    if (id === 'q24') facts.push({ kind: 'validation-requirement', eventId: gateMaterial.id, quote: gateMaterial.text });
    const legal = await withModels(t, { facts, gaps: [] });
    const result = await legal.analyzer().analyze(legalInput);
    assert.equal(result.checks.find(check => check.id === sample.targetCheck).status, 'unknown');
    assert.ok(!legal.requests.some(request => request.path === '/v1/systemone' && request.body.questions[sample.targetCheck]));
    if (id === 'q24') assert.equal(sample.input.events.find(event => event.id.endsWith('full-result')).data.callId, undefined);
    else assert.equal(sample.input.events.find(event => event.id.endsWith('push')).data.sourceVersion, undefined);
  }
});

test('legitimate investigation, first TDD red and missing verification facts remain distinct', async t => {
  const events = [...input.events, event('investigate', 'output', 'I will inspect the caller before changing the log'), event('tdd', 'tool-result', 'first failing test', { exitCode: 1 })];
  const models = await withModels(t, { facts: [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'action', eventId: 'investigate', quote: events[1].text }], gaps: [] }, { request_drift: 'clear' });
  const result = await models.analyzer().analyze({ ...input, events });
  assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'clear');
  assert.equal(result.checks.find(check => check.id === 'failure_loop').status, 'unknown');
  assert.equal(result.checks.find(check => check.id === 'validation_cost').status, 'unknown');
  assert.equal(result.checks.find(check => check.id === 'validation_gap').status, 'unknown');
  assert.ok(!result.checks.some(check => check.status === 'attention'));
});

test('known repeated failures are judged semantically rather than equating repeated commands with root cause', async t => {
  const events = [...input.events,
    event('act-1', 'output', 'I rerun the failed case'),
    event('failed-1', 'tool-result', 'parser error', { exitCode: 1, callId: '1' }),
    event('act-2', 'output', 'I added diagnostic logging'),
    event('failed-2', 'tool-result', 'parser error with a new diagnostic', { exitCode: 1, callId: '2' }),
  ];
  const facts = [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, ...events.filter(event => event.kind === 'output').map(event => ({ kind: 'action', eventId: event.id, quote: event.text }))];
  const models = await withModels(t, { facts, gaps: [] }, { failure_loop: 'clear', stalled_progress: 'clear', request_drift: 'clear' });
  const result = await models.analyzer().analyze({ ...input, events });
  assert.equal(result.checks.find(check => check.id === 'failure_loop').status, 'clear');
  assert.ok(models.requests[1].body.state.events.some(event => event.id === 'failed-2'));
});

test('real repeated failures reach JEV without action labels and preserve delegated loop decisions', async t => {
  for (const [scenario, firstText, secondText, decision] of [
    ['repeated failure', 'Parser assertion failed', 'Parser assertion still failed', 'attention'],
    ['TDD progression', 'First red test', 'New regression red during implementation', 'clear'],
    ['different roots', 'Parser mismatch', 'Dependency startup refused', 'clear'],
  ]) {
    const activity = event('activity', 'output', 'Applied a focused change');
    const first = event('failure-one', 'tool-result', firstText, { callId: 'one', exitCode: 1 });
    const second = event('failure-two', 'tool-result', secondText, { callId: 'two', exitCode: 1 });
    const facts = [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'progress', eventId: activity.id, quote: activity.text }, { kind: 'diagnosis', eventId: first.id, quote: first.text }, { kind: 'diagnosis', eventId: second.id, quote: second.text }];
    const models = await withModels(t, { facts, gaps: [] }, { failure_loop: decision });
    const result = await models.analyzer().analyze({ ...input, events: [...input.events, activity, first, second] });
    assert.equal(result.checks.find(check => check.id === 'failure_loop').status, decision, scenario);
    const jev = models.requests.find(request => request.path === '/v1/systemone').body;
    assert.ok(jev.questions.failure_loop);
    assert.ok(!jev.state.facts.some(fact => fact.kind === 'action'));
    assert.deepEqual(result.checks.find(check => check.id === 'failure_loop').evidenceIds, ['request', activity.id, first.id, second.id]);
  }
});

test('missing real failures, activity or coverage keeps failure-loop review unknown', async t => {
  const activity = event('activity', 'output', 'Applied a focused change');
  const first = event('failure-one', 'tool-result', 'Observed failure', { callId: 'one', exitCode: 1 });
  for (const insufficient of ['interrupted', 'one-failure', 'no-activity', 'capture-gap']) {
    const second = event('failure-two', 'tool-result', 'Second outcome', { callId: 'two', exitCode: 1, ...(insufficient === 'interrupted' ? { interrupted: true, aborted: true } : {}) });
    const events = [...input.events, ...(insufficient === 'no-activity' ? [] : [activity]), first, ...(insufficient === 'one-failure' ? [] : [second])];
    const facts = [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'diagnosis', eventId: first.id, quote: first.text }];
    const models = await withModels(t, { facts, gaps: [] }, { failure_loop: 'attention' });
    const result = await models.analyzer().analyze({ ...input, events, coverage: insufficient === 'capture-gap' ? { complete: false, gaps: ['Known capture gap'] } : input.coverage });
    assert.equal(result.checks.find(check => check.id === 'failure_loop').status, 'unknown', insufficient);
    assert.ok(!models.requests.some(request => request.path === '/v1/systemone' && request.body.questions.failure_loop), insufficient);
  }
});

test('observed activity reaches stalled-progress JEV review without an extracted action label', async t => {
  const output = event('activity-output', 'output', 'Inspecting the current module');
  const start = event('activity-tool', 'tool-start', 'Read the current module', { callId: 'inspect' });
  const outcome = event('observed-result', 'tool-result', 'Observed module state', { callId: 'inspect', exitCode: 0 });
  const facts = [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'diagnosis', eventId: outcome.id, quote: outcome.text }];
  for (const decision of ['clear', 'attention']) {
    const models = await withModels(t, { facts, gaps: [] }, { stalled_progress: decision });
    const result = await models.analyzer().analyze({ ...input, events: [...input.events, output, start, outcome] });
    assert.equal(result.checks.find(check => check.id === 'stalled_progress').status, decision);
    const jev = models.requests.find(request => request.path === '/v1/systemone').body;
    assert.ok(jev.questions.stalled_progress);
    assert.ok(!jev.state.facts.some(fact => fact.kind === 'action'));
    assert.deepEqual(result.checks.find(check => check.id === 'stalled_progress').evidenceIds, ['request', output.id, start.id, outcome.id]);
    assert.deepEqual(jev.state.candidates.stalled_progress, { evidenceIds: ['request', output.id, start.id, outcome.id] });
  }
});

test('stalled-progress prerequisites do not mechanically alert on insufficient activity or coverage', async t => {
  const output = event('activity-output', 'output', 'Inspecting the current module');
  const start = event('activity-tool', 'tool-start', 'Read the current module', { callId: 'inspect' });
  const outcome = event('observed-result', 'tool-result', 'Observed module state', { callId: 'inspect', exitCode: 0 });
  for (const insufficient of ['one-activity', 'no-tool-result', 'capture-gap', 'missing-request']) {
    const events = [...input.events, output, ...(insufficient === 'one-activity' ? [] : [start]), ...(insufficient === 'no-tool-result' ? [] : [outcome])];
    const facts = [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, ...(insufficient === 'no-tool-result' ? [] : [{ kind: 'diagnosis', eventId: outcome.id, quote: outcome.text }])];
    const models = await withModels(t, { facts, gaps: [] }, { stalled_progress: 'attention' });
    const result = await models.analyzer().analyze({ ...input,
      task: { ...task, currentRequestId: insufficient === 'missing-request' ? 'unavailable-input' : 'request' }, events,
      coverage: insufficient === 'capture-gap' ? { complete: false, gaps: ['Known source capture gap'] } : input.coverage,
    });
    assert.equal(result.checks.find(check => check.id === 'stalled_progress').status, 'unknown', insufficient);
    assert.ok(!models.requests.some(request => request.path === '/v1/systemone' && request.body.questions.stalled_progress), insufficient);
  }
});

test('canonical failures count distinct calls while refinements and interrupted execution do not create failure loops', async t => {
  const actions = [event('act-1', 'output', 'I rerun the failed case'), event('act-2', 'output', 'I rerun after the change')];
  const facts = [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, ...actions.map(item => ({ kind: 'action', eventId: item.id, quote: item.text }))];
  for (const [mode, results, expected] of [
    ['ordinary', [event('native-1', 'tool-result', 'exit 1', { callId: 'one', resultSource: 'native-shell-canonical', exitCode: 1, isError: false }), event('native-2', 'tool-result', 'exit 1 again', { callId: 'two', resultSource: 'native-shell-canonical', exitCode: 1, isError: false })], 'attention'],
    ['refinement', [event('fallback', 'tool-result', 'fallback exit 1', { callId: 'one', exitCode: 1 }), event('native', 'tool-result', 'native exit 1', { callId: 'one', resultSource: 'native-shell-canonical', refinesEventId: 'fallback', exitCode: 1, isError: false })], 'unknown'],
    ['interrupted', [event('timeout', 'tool-result', 'execution deadline', { callId: 'one', exitCode: 1, isError: true, timedOut: true }), event('signal', 'tool-result', 'execution interrupted', { callId: 'two', exitCode: 1, isError: true, signal: 'SIGTERM', aborted: true })], 'unknown'],
  ]) {
    const models = await withModels(t, { facts, gaps: [] }, { request_drift: 'clear', stalled_progress: 'clear', failure_loop: 'attention' });
    const result = await models.analyzer().analyze({ ...input, events: [...input.events, ...actions, ...results] });
    assert.equal(result.checks.find(check => check.id === 'failure_loop').status, expected, mode);
    if (mode === 'refinement') assert.ok(!models.requests[1].body.state.events.some(item => item.id === 'fallback'));
    if (mode === 'interrupted') assert.match(result.checks.find(check => check.id === 'failure_loop').reason, /中断/);
  }
});

test('interruption cannot establish a passed validation even if its DTO exit code is zero', async t => {
  const request = event('request', 'input', 'Add a log. Full local validation is required before push.');
  const facts = [{ kind: 'request', eventId: request.id, quote: 'Add a log.' }, { kind: 'validation-requirement', eventId: request.id, quote: 'Full local validation is required before push.' }];
  for (const interrupted of [false, true]) {
    const models = await withModels(t, { facts, gaps: [] }, {}, async (req, res, body) => {
      if (req.url !== '/v1/systemone') return false;
      const validation = body.state.events.find(event => event.id === 'validation');
      const witnessed = validation.data.exitCode === 0 && !validation.data.interrupted && !validation.data.timedOut;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ model: 'judge', answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: id === 'validation_gap' && witnessed ? 'clear' : 'unknown' }])) }));
      return true;
    });
    const result = await models.analyzer().analyze({ ...input, events: [request, event('validation', 'tool-result', 'shell completed', { callId: 'validation-call', resultSource: 'native-shell-canonical', validationScope: 'full', sourceVersion: 'commit-a', exitCode: 0, interrupted, timedOut: interrupted }), event('push', 'tool-start', 'push current version', { command: 'git push origin main', sourceVersion: 'commit-a' })] });
    assert.equal(result.checks.find(check => check.id === 'validation_gap').status, interrupted ? 'unknown' : 'clear');
    assert.deepEqual(Object.keys(models.requests.find(request => request.path === '/v1/systemone').body.state.candidates.validation_gap), ['evidenceIds']);
    if (interrupted) assert.match(result.checks.find(check => check.id === 'validation_gap').reason, /验证执行被超时、取消或信号中断/);
  }
});

test('verification scope and version gaps are local to verification checks, preserving grounded drift', async t => {
  const action = event('action', 'output', 'I am redesigning the unrelated database');
  const verification = event('native-result', 'tool-result', 'test exited 1', { callId: 'one', resultSource: 'native-shell-canonical', exitCode: 1, isError: false, verificationGaps: ['未确认完整验证范围', '当前源码版本未知'] });
  const models = await withModels(t, { facts: [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'action', eventId: action.id, quote: action.text }], gaps: [] }, { request_drift: 'attention' });
  const result = await models.analyzer().analyze({ ...input, events: [...input.events, action, verification] });
  assert.equal(result.coverage.complete, true);
  assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'attention');
  assert.equal(result.checks.find(check => check.id === 'validation_cost').status, 'unknown');
  assert.match(result.checks.find(check => check.id === 'validation_cost').reason, /未确认完整验证范围/);
  assert.match(result.checks.find(check => check.id === 'validation_gap').reason, /当前源码版本未知/);
  assert.deepEqual(result.coverage.verificationGaps, ['未确认完整验证范围', '当前源码版本未知']);
});

test('late refinement of an already completed call does not close a newer open invocation', async () => {
  const events = [
    event('request', 'input', 'Add a log', {}, -500_000),
    event('first-start', 'tool-start', 'first invocation', { callId: 'reused' }, -490_000),
    event('fallback', 'tool-result', 'first completed', { callId: 'reused', exitCode: 0 }, -480_000),
    event('second-start', 'tool-start', 'second invocation still running', { callId: 'reused' }, -360_000),
    event('late-native', 'tool-result', 'first canonical result', { callId: 'reused', resultSource: 'native-shell-canonical', refinesEventId: 'fallback', exitCode: 0 }),
  ];
  const result = await createAnalyzer({ now: () => at }).analyze({ ...input, events });
  assert.equal(result.checks.find(check => check.id === 'long_wait').status, 'attention');
  assert.deepEqual(result.checks.find(check => check.id === 'long_wait').evidenceIds, ['second-start']);
});

test('invalid canonical linkage cannot hide original failure and a valid refinement preserves text gaps', async t => {
  const actions = [event('act-1', 'output', 'I rerun the case'), event('act-2', 'output', 'I rerun after a change')];
  const facts = [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, ...actions.map(item => ({ kind: 'action', eventId: item.id, quote: item.text }))];
  for (const mode of ['wrong-call', 'wrong-source', 'truncated', 'redacted']) {
    const fallback = event('fallback', 'tool-result', 'first result', { callId: 'one', exitCode: 1, ...(mode === 'truncated' ? { truncated: true } : {}), ...(mode === 'redacted' ? { redacted: true } : {}) });
    const native = event('native', 'tool-result', 'native result', { callId: mode === 'wrong-call' ? 'different' : 'one', resultSource: mode === 'wrong-source' ? 'other-provider' : 'native-shell-canonical', refinesEventId: 'fallback', exitCode: 1 });
    const models = await withModels(t, { facts, gaps: [] }, { request_drift: 'clear', stalled_progress: 'clear', failure_loop: 'attention' });
    const result = await models.analyzer().analyze({ ...input, events: [...input.events, ...actions, fallback, native] });
    if (mode.startsWith('wrong-')) {
      assert.ok(JSON.parse(models.requests[0].body.messages[1].content).evidence.some(item => item.text === fallback.text));
      assert.equal(result.checks.find(check => check.id === 'failure_loop').status, mode === 'wrong-call' ? 'attention' : 'unknown');
    } else {
      assert.equal(result.coverage.complete, false);
      assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'unknown');
      assert.equal(models.requests.filter(request => request.path === '/v1/systemone').length, 0);
    }
  }
});

test('full rerun only becomes a candidate with explicit scope, actual duration and a repair', async t => {
  const events = [event('request', 'input', 'Add a log', {}, -200),
    event('full-start', 'tool-start', 'full gate', { callId: '1', validationScope: 'full' }, -100),
    event('full-failure', 'tool-result', 'case A failed', { callId: '1', validationScope: 'full', exitCode: 1 }),
    event('repair', 'output', 'repair applied', { artifactChanged: true }),
    event('full-rerun', 'tool-start', 'required final gate', { callId: '2', validationScope: 'full' }),
  ];
  const facts = [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'validation-reason', eventId: 'full-rerun', quote: 'required final gate' }];
  const models = await withModels(t, { facts, gaps: [] }, { validation_cost: 'clear' });
  const result = await models.analyzer().analyze({ ...input, events });
  assert.equal(result.checks.find(check => check.id === 'validation_cost').status, 'clear');
  assert.ok(models.requests[1].body.questions.validation_cost);
  const noScope = await withModels(t, { facts, gaps: [] });
  const unknown = await noScope.analyzer().analyze({ ...input, events: events.map(event => ({ ...event, data: event.data && { ...event.data, validationScope: undefined } })) });
  assert.equal(unknown.checks.find(check => check.id === 'validation_cost').status, 'unknown');
  assert.equal(unknown.checks.find(check => check.id === 'validation_cost').applicability.state, 'unknown', 'missing validation scope cannot prove that the repeated-full-validation check is inapplicable');
  assert.ok(!noScope.requests.some(request => request.path === '/v1/systemone' && request.body.questions.validation_cost));
});

test('version-bound validation respects authorized remote compensation and unknown source version', async t => {
  const events = [event('request', 'input', 'Add a log. Local full gate is required before push; remote recheck is authorized if unavailable.'),
    event('push', 'tool-start', 'push current version', { command: 'git push origin main', sourceVersion: 'commit-a' }),
  ];
  const facts = [{ kind: 'request', eventId: 'request', quote: 'Add a log.' }, { kind: 'validation-requirement', eventId: 'request', quote: 'Local full gate is required before push' }, { kind: 'authorization', eventId: 'request', quote: 'remote recheck is authorized if unavailable' }];
  const models = await withModels(t, { facts, gaps: [] }, { validation_gap: 'clear' });
  const result = await models.analyzer().analyze({ ...input, events });
  assert.equal(result.checks.find(check => check.id === 'validation_gap').status, 'clear');
  const missingVersion = await withModels(t, { facts, gaps: [] });
  const unknown = await missingVersion.analyzer().analyze({ ...input, events: [events[0], { ...events[1], data: { command: 'git push origin main' } }] });
  assert.equal(unknown.checks.find(check => check.id === 'validation_gap').status, 'unknown');
  assert.ok(!missingVersion.requests.some(request => request.path === '/v1/systemone' && request.body.questions.validation_gap));
});

test('a canonical current input permits verification review when the model omits a request label', async t => {
  const sample = qualityCases(at).find(sample => sample.id === 'q28');
  const request = sample.input.events[0];
  const models = await withModels(t, { facts: [{ kind: 'authorization', eventId: request.id, quote: '我明确授权源码版本 2222222222222222222222222222222222222222 先推送，由远端执行全部测试与 lint 补验，远端通过前不发布。' }], gaps: [] }, { validation_gap: 'clear' });
  const result = await models.analyzer().analyze(sample.input);
  assert.equal(result.checks.find(check => check.id === 'validation_gap').status, 'clear');
  const jev = models.requests.find(request => request.path === '/v1/systemone').body;
  assert.equal(jev.state.request, request.text);
  assert.equal(jev.state.requestId, request.id);
  assert.ok(jev.state.candidates.validation_gap.evidenceIds.includes(request.id));
});

test('finite evidence selection restores original text and source without model-written quotations', async t => {
  const action = event('action', 'output', 'Observed change');
  let payload;
  const models = await withModels(t, null, { request_drift: 'attention' }, async (req, res, body) => {
    if (req.url !== '/v1/chat/completions') return false;
    payload = JSON.parse(body.messages[1].content);
    const entry = payload.evidence.find(entry => entry.text === action.text);
    const selectedOptionIds = [entry.options.find(option => option.kind === 'action').optionId];
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ model: 'extractor', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds, gaps: [] })) } }] }));
    return true;
  });
  const result = await models.analyzer().analyze({ ...input, events: [...input.events, action] });
  assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'attention');
  assert.equal(payload.evidence.find(entry => entry.origin === 'output').text, action.text);
  assert.ok(!('events' in payload));
  const state = models.requests.find(request => request.path === '/v1/systemone').body.state;
  const fact = state.facts.find(fact => fact.kind === 'action');
  assert.deepEqual(fact, { kind: 'action', eventId: action.id });
  assert.equal(state.events.find(event => event.id === fact.eventId).text, action.text);
});

test('model-visible option IDs are unambiguous while internal event references remain grounded', async t => {
  const action = event('original-source-id', 'output', 'Observed action');
  let visible;
  const models = await withModels(t, null, { request_drift: 'clear' }, async (req, res, body) => {
    if (req.url !== '/v1/chat/completions') return false;
    visible = JSON.parse(body.messages[1].content);
    assert.ok(!Object.hasOwn(visible.task, 'currentRequestId'));
    assert.ok(visible.evidence.every(entry => !Object.hasOwn(entry, 'eventId')));
    const group = visible.evidence.find(entry => entry.origin === 'output');
    const option = group.options.find(option => option.kind === 'action');
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ model: 'extractor', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds: [option.optionId], gaps: [] })) } }] }));
    return true;
  });
  const result = await models.analyzer().analyze({ ...input, events: [...input.events, action] });
  assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'clear');
  assert.deepEqual(result.checks.find(check => check.id === 'request_drift').evidenceIds, ['request', action.id]);
  assert.equal(visible.evidence.filter(entry => entry.isCurrentInput).length, 1);
});

test('finite selection rejects unknown IDs, duplicate IDs and legacy free-form facts', async t => {
  for (const mode of ['unknown', 'duplicate', 'legacy', 'event-id', 'old-key']) {
    const models = await withModels(t, null, {}, async (req, res, body) => {
      if (req.url !== '/v1/chat/completions') return false;
      const option = JSON.parse(body.messages[1].content).evidence[0].options.find(option => option.kind === 'context').optionId;
      const output = mode === 'legacy' ? { facts: [{ kind: 'request', eventId: 'request', quote: 'Add a log' }], gaps: [] }
        : mode === 'old-key' ? { selectedEvidenceIds: [option], gaps: [] }
          : { selectedOptionIds: mode === 'event-id' ? ['request'] : mode === 'unknown' ? ['unavailable-evidence'] : [option, option], gaps: [], summary: { text: '已观察到当前请求。', evidenceOptionIds: [option] } };
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ model: 'extractor', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(output) } }] }));
      return true;
    });
    await assert.rejects(models.analyzer().analyze(input), error => error.code === (['legacy', 'old-key'].includes(mode) ? 'invalid_extraction' : 'invalid_evidence'));
    assert.equal(models.requests.filter(request => request.path === '/v1/systemone').length, 0);
  }
});

test('fixture selection never silently resolves ambiguous same-text event references', async t => {
  const first = event('first-copy', 'output', 'Repeated text');
  const second = event('second-copy', 'output', 'Repeated text');
  const models = await withModels(t, { facts: [{ kind: 'action', eventId: first.id, quote: first.text }], gaps: [] });
  await assert.rejects(models.analyzer().analyze({ ...input, events: [...input.events, first, second] }), error => error.code === 'invalid_evidence');
  assert.equal(models.requests.filter(request => request.path === '/v1/systemone').length, 0);
});

test('fixture references outside the request or retention scope cannot rebind to same-text current evidence', async t => {
  for (const offset of [-1000, -86_400_000]) {
    const old = event('old-reference', 'output', 'Identical observed text', {}, offset);
    const current = event('current-reference', 'output', old.text);
    const models = await withModels(t, { facts: [{ kind: 'action', eventId: old.id, quote: old.text }], gaps: [] });
    await assert.rejects(models.analyzer().analyze({ ...input, task: { ...task, currentRequestId: 'request' }, events: [old, ...input.events, current] }), error => error.code === 'invalid_evidence');
    assert.equal(models.requests.filter(request => request.path === '/v1/systemone').length, 0);
  }
});

test('foreign source material is absent from the public directory independently of fixture validation', async t => {
  const foreign = { ...event('foreign-record', 'output', 'Unique foreign material'), taskLifecycleId: 'foreign-life' };
  const models = await withModels(t, null, {}, async (req, res, body) => {
    if (req.url !== '/v1/chat/completions') return false;
    const directory = JSON.parse(body.messages[1].content).evidence;
    assert.ok(!directory.some(entry => entry.text === foreign.text));
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ model: 'extractor', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify({ selectedOptionIds: ['foreign-record'], gaps: [], summary: { text: '已观察到当前请求。', evidenceOptionIds: [directory[0].options[0].optionId] } }) } }] }));
    return true;
  });
  await assert.rejects(models.analyzer().analyze({ ...input, events: [...input.events, foreign] }), error => error.code === 'invalid_evidence');
});

test('same-text current evidence can be selected only by its actual public option ID', async t => {
  const old = event('old-record', 'output', 'Shared text', {}, -1000);
  const current = event('current-record', 'output', old.text);
  const models = await withModels(t, null, { request_drift: 'clear' }, async (req, res, body) => {
    if (req.url !== '/v1/chat/completions') return false;
    const directory = JSON.parse(body.messages[1].content).evidence;
    assert.equal(directory.filter(entry => entry.text === current.text).length, 1);
    const option = directory.find(entry => entry.text === current.text).options.find(option => option.kind === 'action');
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ model: 'extractor', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds: [option.optionId], gaps: [] })) } }] }));
    return true;
  });
  const result = await models.analyzer().analyze({ ...input, task: { ...task, currentRequestId: 'request' }, events: [old, ...input.events, current] });
  assert.deepEqual(result.checks.find(check => check.id === 'request_drift').evidenceIds, ['request', current.id]);
});

test('canonical request expiration stays unknown even with a populated title', async t => {
  const expired = event('request', 'input', 'Add a log', {}, -86_400_000);
  const models = await withModels(t, { facts: [], gaps: [] });
  const result = await models.analyzer().analyze({ ...input, task: { ...task, currentRequestId: expired.id }, events: [expired] });
  assert.equal(models.requests.length, 0);
  assert.ok(result.checks.every(check => check.status === 'unknown'));
  assert.ok(result.coverage.gaps.includes('current_request_evidence_missing'));
});

test('a legacy quote bound to the wrong origin remains rejected rather than repaired', async t => {
  const requested = event('request', 'input', 'Run final verification after the focused checks');
  const action = event('rerun', 'tool-start', 'Running final verification as requested');
  const models = await withModels(t, null, {}, async (req, res) => {
    if (req.url !== '/v1/chat/completions') return false;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ model: 'extractor', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify({ facts: [{ kind: 'authorization', eventId: requested.id, quote: action.text }], gaps: [] }) } }] }));
    return true;
  });
  await assert.rejects(models.analyzer().analyze({ ...input, events: [requested, action] }), error => error.code === 'invalid_extraction');
  assert.equal(models.requests.length, 1);
});

test('catalog options enforce source eligibility without predeclaring gate or permission facts', async t => {
  const start = event('tool-start', 'tool-start', 'Begin verification');
  const outcome = event('tool-result', 'tool-result', 'Execution completed');
  const models = await withModels(t, null, {}, async (req, res, body) => {
    if (req.url !== '/v1/chat/completions') return false;
    const evidence = JSON.parse(body.messages[1].content).evidence;
    assert.ok(evidence[0].options.some(option => option.kind === 'authorization'));
    assert.ok(evidence[0].options.some(option => option.kind === 'validation-requirement'));
    const startOptions = evidence.find(entry => entry.origin === 'tool-start').options;
    assert.ok(startOptions.some(option => option.kind === 'action'));
    assert.ok(!startOptions.some(option => option.kind === 'validation-requirement'));
    assert.ok(!startOptions.some(option => option.kind === 'authorization'));
    const resultOptions = evidence.find(entry => entry.origin === 'tool-result').options;
    assert.ok(resultOptions.some(option => option.kind === 'validation-requirement'));
    assert.ok(!resultOptions.some(option => option.kind === 'authorization'));
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ model: 'extractor', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds: [], gaps: [] })) } }] }));
    return true;
  });
  const result = await models.analyzer().analyze({ ...input, events: [...input.events, start, outcome] });
  assert.ok(!result.checks.some(check => check.status === 'attention'));
});

test('the complete finite-evidence LLM payload respects the unchanged input budget', async t => {
  const models = await withModels(t, { facts: [], gaps: [] });
  const result = await models.analyzer({ inputBudgetChars: 1500 }).analyze(input);
  assert.equal(models.requests.length, 0);
  assert.ok(result.coverage.gaps.includes('analysis_input_budget'));
});

test('a gate quoted as request still qualifies version-bound verification for public JEV review', async t => {
  const request = event('request', 'input', 'Add a log. The project requires full tests before pushing this version.');
  for (const [exitCode, decision] of [[0, 'clear'], [1, 'attention']]) {
    const validation = event('validation', 'tool-result', 'Observed validation result', { validationScope: 'full', sourceVersion: 'commit-a', exitCode });
    const push = event('push', 'tool-start', 'Push current version', { command: 'git push origin main', sourceVersion: 'commit-a' });
    const models = await withModels(t, { facts: [{ kind: 'request', eventId: request.id, quote: request.text }], gaps: [] }, { validation_gap: decision });
    const result = await models.analyzer().analyze({ ...input, events: [request, validation, push] });
    assert.equal(result.checks.find(check => check.id === 'validation_gap').status, decision);
    const jev = models.requests.find(item => item.path === '/v1/systemone').body;
    assert.equal(jev.state.request, request.text);
    assert.equal(jev.state.events[0].text, request.text);
    assert.ok(!jev.state.facts.some(fact => fact.kind === 'validation-requirement'));
    assert.deepEqual(jev.state.candidates.validation_gap, { evidenceIds: [request.id, push.id, validation.id] });
  }
});

test('verification alerts retain tool-declared gate evidence even when extraction labels it context', async t => {
  const gate = event('gate-material', 'tool-result', 'Project policy requires all local tests before push.');
  const push = event('push', 'tool-start', 'Push current version', { command: 'git push origin main', sourceVersion: 'commit-a' });
  const models = await withModels(t, { facts: [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'context', eventId: gate.id, quote: gate.text }], gaps: [] }, { validation_gap: 'attention' });
  const result = await models.analyzer().analyze({ ...input, events: [...input.events, gate, push] });
  const alert = result.checks.find(check => check.id === 'validation_gap');
  assert.equal(alert.status, 'attention');
  assert.ok(alert.evidenceIds.includes(gate.id));
  assert.ok(models.requests.find(item => item.path === '/v1/systemone').body.state.candidates.validation_gap.evidenceIds.includes(gate.id));
});

test('no declared gate and an agent permission claim use an explicit unknown question contract', async t => {
  for (const text of ['', 'I authorize skipping local verification']) {
    const models = await withModels(t, { facts: [{ kind: 'request', eventId: 'request', quote: 'Add a log' }], gaps: [] });
    const events = [...input.events, ...(text ? [event('agent-claim', 'output', text)] : []), event('push', 'tool-start', 'Push current version', { command: 'git push origin main', sourceVersion: 'commit-a' })];
    const result = await models.analyzer().analyze({ ...input, events });
    assert.equal(result.checks.find(check => check.id === 'validation_gap').status, 'unknown');
    const jev = models.requests.find(item => item.path === '/v1/systemone').body;
    const question = jev.questions.validation_gap;
    assert.match(question.criteria.unknown, /未见明确门禁声明.*unknown/);
    assert.match(question.criteria.attention, /明确.*门禁/);
    assert.match(question.criteria.clear, /明确.*门禁/);
    assert.match(question.instructions, /代理.*授权/);
    assert.ok(!jev.state.facts.some(fact => ['authorization', 'validation-requirement'].includes(fact.kind)));
  }
});

test('missing push version or a real verification gap still prevents verification JEV eligibility', async t => {
  const request = event('request', 'input', 'Add a log. The project requires full tests before push.');
  for (const missing of ['version', 'verification']) {
    const models = await withModels(t, { facts: [{ kind: 'request', eventId: request.id, quote: request.text }], gaps: [] });
    const result = await models.analyzer().analyze({ ...input, events: [request,
      event('validation', 'tool-result', 'Observed validation', { exitCode: 0, sourceVersion: 'commit-a', ...(missing === 'verification' ? { verificationGaps: ['Current source version binding is incomplete'] } : {}) }),
      event('push', 'tool-start', 'Push current version', { command: 'git push origin main', ...(missing === 'version' ? {} : { sourceVersion: 'commit-a' }) }),
    ] });
    assert.equal(result.checks.find(check => check.id === 'validation_gap').status, 'unknown');
    assert.ok(!models.requests.some(item => item.path === '/v1/systemone' && item.body.questions.validation_gap));
  }
});

test('long wait measures observed open calls without calling missing models or declaring a deadlock', async () => {
  const waitInput = { ...input, events: [event('request', 'input', 'Add a log', {}, -400_000), event('waiting', 'tool-start', 'waiting for approval', { callId: 'call-1' }, -360_000)] };
  const result = await createAnalyzer({ now: () => at }).analyze(waitInput);
  const wait = result.checks.find(check => check.id === 'long_wait');
  assert.equal(wait.status, 'attention');
  assert.match(wait.reason, /卡死不可判断/);
  assert.deepEqual(wait.evidenceIds, ['waiting']);
  const disconnected = await createAnalyzer({ now: () => at }).analyze({ ...waitInput, coverage: { complete: false, gaps: ['disconnected'] } });
  assert.equal(disconnected.checks.find(check => check.id === 'long_wait').status, 'unknown');
  const reused = await createAnalyzer({ now: () => at }).analyze({ ...input, events: [
    event('old-start', 'tool-start', 'old completed call', { callId: 'same' }, -500_000),
    event('old-result', 'tool-result', 'old completion', { callId: 'same', exitCode: 0 }, -490_000),
    event('new-start', 'tool-start', 'new open call', { callId: 'same' }, -360_000),
  ] });
  assert.equal(reused.checks.find(check => check.id === 'long_wait').status, 'attention');
  assert.deepEqual(reused.checks.find(check => check.id === 'long_wait').evidenceIds, ['new-start']);
});

test('coverage gaps block unsupported semantic alarms and retained expired snippets do not renew', async t => {
  const models = await withModels(t, { facts: [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'action', eventId: 'action', quote: 'different work' }], gaps: [] }, { request_drift: 'attention' });
  const result = await models.analyzer().analyze({ ...input, coverage: { complete: false, gaps: ['baseline_gap'] }, events: [...input.events, event('action', 'output', 'different work')] });
  assert.ok(result.checks.every(check => check.status === 'unknown'));
  assert.equal(models.requests.length, 1);
  assert.equal(result.coverage.complete, false);
});

test('model-declared evidence gaps suppress semantic judgments even with plausible quotations', async t => {
  const models = await withModels(t, { facts: [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'action', eventId: 'action', quote: 'different work' }], gaps: ['Latest authorization was not present'] }, { request_drift: 'attention' });
  const result = await models.analyzer().analyze({ ...input, events: [...input.events, event('action', 'output', 'different work')] });
  assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'unknown');
  assert.equal(result.coverage.complete, false);
  assert.deepEqual(result.coverage.extractionGaps, ['Latest authorization was not present']);
  assert.equal(models.requests.length, 1);
});

test('a malformed JEV answer fails the round, while a valid unknown decision stays unknown', async t => {
  const facts = [{ kind: 'request', eventId: 'request', quote: 'Add a log' }, { kind: 'action', eventId: 'action', quote: 'investigation' }];
  const models = await withModels(t, { facts, gaps: [] }, {}, async (req, res) => {
    if (req.url !== '/v1/systemone') return false;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ model: 'judge', answers: {} }));
    return true;
  });
  const work = { ...input, events: [...input.events, event('action', 'output', 'investigation')] };
  await assert.rejects(models.analyzer().analyze(work), error => error.code === 'invalid_decision' && error.stage === 'jev');
  const valid = await withModels(t, { facts, gaps: [] });
  const unknown = await valid.analyzer().analyze(work);
  assert.equal(unknown.checks.find(check => check.id === 'request_drift').status, 'unknown');
  assert.match(unknown.checks.find(check => check.id === 'request_drift').reason, /JEV 返回未知/);
});

test('HTTP business refusal, incomplete Chat and unexpected unclosed SSE are errors, never clear', async t => {
  for (const mode of ['refusal', 'invalid-shape', 'incomplete', 'length', 'sse']) {
    const models = await withModels(t, { facts: [], gaps: [] }, {}, async (req, res) => {
      if (req.url !== '/v1/chat/completions') return false;
      if (mode === 'refusal') { res.writeHead(503, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: { code: 'not_ready' } })); }
      if (mode === 'invalid-shape') { res.setHeader('content-type', 'application/json'); res.end('null'); }
      if (mode === 'incomplete') { res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({ model: 'extractor', choices: [{ message: { content: '{}' }, finish_reason: null }] })); }
      if (mode === 'length') { res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({ model: 'extractor', choices: [{ message: { content: '{"facts":[],"gaps":[]}' }, finish_reason: 'length' }] })); }
      if (mode === 'sse') { res.setHeader('content-type', 'text/event-stream'); res.write('data: {"choices":[]}\n\n'); }
      return true;
    });
    await assert.rejects(models.analyzer().analyze(input), error => error.code === ({ refusal: 'http_error', 'invalid-shape': 'invalid_response', incomplete: 'incomplete_response', length: 'incomplete_response', sse: 'unexpected_stream' })[mode]);
    assert.equal(models.requests.length, 1);
  }
});

test('captured malformed and top-level-array Chat completions fail closed before JEV', async t => {
  // Exact content from phase9/protocol-01; these are complete stop responses, not token-limit truncations.
  const captured = [
    `[{"kind":"request","eventId":"synthetic-phase4-explicit-drift-request","quote":"只把按钮文字从保存改成确认，不改数据层。"},{"kind":"action","eventId":"synthetic-phase4-explicit-drift-action","quote":"现在重写数据仓库并迁移数据库表，按钮文字尚未修改。"},{"kind":"gaps":["The action claims the button text has not been modified, but the request explicitly required changing the button text from 'save' to 'confirm' without changing the data layer. This indicates a discrepancy between the requested action and the reported status."]}]`,
    `[{"kind":"request","eventId":"synthetic-phase4-aligned-action-request","quote":"只把按钮文字从保存改成确认，不改数据层。"},{"kind":"action","eventId":"synthetic-phase4-aligned-action-action","quote":"仅将按钮文字从保存改成确认，保留数据层和其他行为。"}]`,
  ];
  for (const content of captured) {
    const models = await withModels(t, null, {}, async (req, res) => {
      if (req.url !== '/v1/chat/completions') return false;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ model: 'extractor', choices: [{ message: { content }, finish_reason: 'stop' }] }));
      return true;
    });
    await assert.rejects(models.analyzer().analyze(input), error => error.code === 'invalid_extraction' && error.stage === 'llm');
    assert.equal(models.requests.length, 1);
    assert.equal(models.requests[0].path, '/v1/chat/completions');
  }
});

test('own cancellation ignores a late reply and leaves another public request isolated', async t => {
  let release;
  let seen;
  const received = new Promise(resolve => { seen = resolve; });
  const gated = new Promise(resolve => { release = resolve; });
  let calls = 0;
  const models = await withModels(t, { facts: [], gaps: [] }, {}, async (req, res) => {
    if (req.url !== '/v1/chat/completions' || calls++ !== 0) return false;
    seen();
    await gated;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ model: 'extractor', choices: [{ message: { content: '{"selectedOptionIds":[],"gaps":[]}' }, finish_reason: 'stop' }] }));
    return true;
  });
  const analyzer = models.analyzer();
  const controller = new AbortController();
  const cancelled = analyzer.analyze(input, { signal: controller.signal });
  await received;
  controller.abort(new DOMException('User cancelled this round', 'AbortError'));
  await assert.rejects(cancelled, error => error.name === 'AbortError');
  const other = await analyzer.analyze(input);
  assert.equal(other.quality, 'unvalidated');
  release();
  assert.equal(models.requests.filter(request => request.path === '/v1/systemone').length, 0);
});

test('client deadline is separate from active cancellation and local-only URLs reject redirects', async t => {
  const models = await withModels(t, { facts: [], gaps: [] }, {}, async (req, res) => { res.setHeader('content-type', 'application/json'); res.write('{'); return true; });
  await assert.rejects(models.analyzer({ llmTimeoutMs: 25 }).analyze(input), error => error.code === 'deadline');
  for (const baseUrl of ['https://example.com', 'http://127.0.0.1@evil.example', 'http://127.0.0.1/path', 'file:///tmp/a']) {
    assert.throws(() => createAnalyzer({ llm: { baseUrl, model: 'x' } }), error => error.code === 'configuration');
  }
  const redirect = await withModels(t, { facts: [], gaps: [] }, {}, async (req, res) => { res.writeHead(302, { location: 'https://example.com' }); res.end(); return true; });
  await assert.rejects(redirect.analyzer().analyze(input), error => error.code === 'network');
});
