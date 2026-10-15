import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { createAnalyzer, CHECK_IDS } from '../src/analysis/index.mjs';
import { qualityCases } from '../probes/modeldeck/quality-cases.mjs';
import { decodeJevRequest } from './helpers/jev-wire.mjs';
import { renderJevRequest, JEV_PROVENANCE_INSTRUCTIONS } from '../src/analysis/jev-rendering.mjs';

const now = 1_800_000_000_000;
const bytes = body => Buffer.byteLength(JSON.stringify(body));
const ref = event => ({ taskId: event.taskId, lifecycleId: event.taskLifecycleId, eventId: event.id });
function multilevel() {
  const taskId = 'synthetic-density-grandchild', lifecycleId = taskId + '-life';
  const own = (id, kind, text, age, data = {}) => ({ id: taskId + '-' + id, taskId, taskLifecycleId: lifecycleId, kind, text, capturedAt: now - age, occurredAt: now - age, data });
  const oldAction = own('action-one', 'output', '阶段一：仅改日志前缀后重试同一个 CSV 双引号失败。原文标记 {"$ref":0} 与 {"$eventRef":1}。', 150_000, { artifactChanged: true, $ref: 0, $eventRef: 1, userJSON: { taskId: 'untrusted-owner', lifecycleId: 'untrusted-life', eventId: 'untrusted-event' } });
  oldAction.extension = { taskId: 'extension-owner', lifecycleId: 'extension-life', eventId: 'extension-event' };
  const events = [own('request', 'input', '委派阶段一：修复 CSV 双引号转义，遵守祖先的只改 CSV 和先定点后完整验证限制。', 800_000), own('full-start-one', 'tool-start', '开始完整验证。', 700_000, { callId: 'full-one', validationScope: 'full', sourceVersion: 'version-one', command: 'npm run verify' }), own('full-result-one', 'tool-result', 'CSV quote expected doubled quotes, received unescaped quotes；其余检查通过。', 180_000, { callId: 'full-one', validationScope: 'full', sourceVersion: 'version-one', exitCode: 1 }), oldAction, own('full-start-two', 'tool-start', '未定点复测，开始第二次完整验证。', 140_000, { callId: 'full-two', validationScope: 'full', sourceVersion: 'version-two', command: 'npm run verify' }), own('full-result-two', 'tool-result', 'CSV quote 仍为同一个双引号转义失败。', 100_000, { callId: 'full-two', validationScope: 'full', sourceVersion: 'version-two', exitCode: 1 })];
  const inherited = ['synthetic-density-root', 'synthetic-density-parent'].map((owner, i) => ({ id: 'shared-local-policy-id', taskId: owner, taskLifecycleId: owner + '-life', kind: 'input', text: i === 0 ? '用户限制：只改 CSV；当前版本完整测试及 lint 通过后才可推送；失败先定点复测。' : '用户补充限制：不要改存储模块，不要把代理的承诺当作用户授权。', capturedAt: now - 850_000 - i * 1000, occurredAt: now - 850_000 - i * 1000, data: {}, contextRole: 'ancestor-limit', authorizationEligible: true }));
  for (const event of inherited) event.sourceRef = ref(event);
  inherited[1].sourceRef.note = 'retain the extended source reference';
  const binding = { taskId, lifecycleId, inputRef: ref(events[0]), phase: 'consumed', origin: 'delegated', originRefs: [ref(inherited[1]), { ...ref(inherited[0]), lifecycleId: 'root-earlier-life' }, { taskId: '', lifecycleId: 'invalid-life', eventId: 'invalid-event' }], ancestorLimitRefs: inherited.map(ref), extension: { sourceRef: ref(inherited[0]), $ref: 9 } };
  const input = { task: { id: taskId, lifecycleId, status: 'running', currentRequestId: events[0].id }, source: { descriptor: { name: 'synthetic two-level ancestors', gaps: [] } }, events, instructionContext: { binding, inheritedEvents: inherited, gaps: [] }, coverage: { complete: true, gaps: [], synthetic: true } };
  const recovery = structuredClone(input);
  recovery.task.activeAlerts = [{ id: 'synthetic-old-request-drift', checkId: 'request_drift', reason: '原疑点：阶段一行动超出只改 CSV 的范围；需要相关新结果确认恢复。', evidenceIds: [oldAction.id], sourceRef: ref(inherited[0]) }];
  recovery.events.push(own('phase-two-repair', 'output', '阶段二：修复 CSV 转义并恢复无关模块，随后定点复测。', 70_000, { artifactChanged: true }), own('phase-two-target-start', 'tool-start', '定点执行 CSV quote。', 60_000, { callId: 'target', validationScope: 'targeted', sourceVersion: 'version-three', command: 'node --test test/csv.test.mjs' }), own('phase-two-target-result', 'tool-result', 'CSV quote passed；无关存储模块哈希恢复原值。', 50_000, { callId: 'target', validationScope: 'targeted', sourceVersion: 'version-three', exitCode: 0 }), own('phase-two-push-start', 'tool-start', '开始合成推送记录。', 40_000, { callId: 'push', sourceVersion: 'version-three', command: 'git push origin dev' }));
  return { input, recovery };
}

async function inference(t, { holdJudge = false, recoveryChoice = 'unknown' } = {}) {
  const captures = [];
  const server = createServer(async (req, res) => {
    try {
      let raw = ''; for await (const chunk of req) raw += chunk;
      const body = JSON.parse(raw); captures.push({ path: req.url, rawBytes: Buffer.byteLength(raw), body });
      res.setHeader('content-type', 'application/json');
      if (req.url === '/v1/chat/completions') {
        const evidence = JSON.parse(body.messages[1].content).evidence;
        const selectedOptionIds = evidence.flatMap(group => {
          const kind = group.contextRole === 'ancestor-limit' ? 'authorization' : group.origin === 'tool-result' ? 'result' : ['tool-start', 'output'].includes(group.origin) ? 'action' : 'context';
          const chosen = group.options.find(option => option.kind === kind);
          return chosen ? [chosen.optionId] : [];
        });
        res.end(JSON.stringify({ model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify({ selectedOptionIds, gaps: [] }) } }] }));
      } else if (!holdJudge) res.end(JSON.stringify({ model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: id === 'request_drift' ? 'clear' : id === 'recover_request_drift' ? recoveryChoice : 'unknown' }])) }));
    } catch (error) { res.writeHead(500); res.end(JSON.stringify({ error: error.message })); }
  });
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); assert.equal(server.listening, false); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  return { captures, analyzer(extra = {}) { return createAnalyzer({ llm: { baseUrl, model: 'fake-extractor' }, jev: { baseUrl, model: 'fake-judge' }, now: () => now, ...extra }); } };
}

test('JEV renders one lossless provenance dictionary without rewriting evidence or changing recovery questions', async t => {
  const http = await inference(t), multi = multilevel();
  const samples = [{ name: 'existing-q13', input: qualityCases(now).find(item => item.id === 'q13').input }, { name: 'multilevel-stage-one', input: multi.input }, { name: 'multilevel-stage-two', input: multi.recovery }];
  const records = [];
  for (const sample of samples) {
    const before = structuredClone(sample.input), count = http.captures.length;
    const result = await http.analyzer().analyze(sample.input);
    assert.deepEqual(sample.input, before, 'observations, source references and active alerts remain immutable');
    const calls = http.captures.slice(count);
    assert.deepEqual(calls.map(item => item.path), ['/v1/chat/completions', '/v1/systemone'], 'one batch serves the semantic and recovery questions');
    assert.deepEqual(result.checks.map(check => check.id), CHECK_IDS);
    const capture = calls[1];
    if (process.env.SENTINEL_JEV_RENDER_RECEIPTS) await writeFile(join(process.env.SENTINEL_JEV_RENDER_RECEIPTS, sample.name + '-wire.json'), JSON.stringify(capture.body, null, 2));
    records.push({ ...sample, capture, result });
  }
  const metrics = [];
  for (const { name, input, capture, result } of records) {
    const { body } = capture;
    assert.ok(Array.isArray(body.state.referenceTable), 'actual JEV body must intern complete source identities');
    assert.match(body.state.referenceRenderingInstructions, /\$eventRef/);
    assert.match(body.state.referenceRenderingInstructions, /\$ref/);
    assert.match(body.state.referenceRenderingInstructions, /Apply sharedQuestionInstructions before each semantic question/);
    const baseline = JSON.parse(await readFile(new URL('./fixtures/jev-rendering/' + name + '.json', import.meta.url), 'utf8'));
    assert.deepEqual(decodeJevRequest(body), baseline, 'entire original logical request survives the public HTTP rendering seam');
    assert.deepEqual(Object.keys(body.questions), Object.keys(baseline.questions));
    const semantic = Object.entries(body.questions).filter(([id]) => !id.startsWith('recover_'));
    assert.ok(semantic.every(([, question]) => !question.instructions.includes('state.requestId 是核心确认')));
    assert.equal(JSON.stringify(body).split('state.requestId 是核心确认').length - 1, 1, 'fixed shared provenance paragraph appears exactly once');
    assert.ok(capture.rawBytes < bytes(baseline), 'dictionary removes measured wire repetition');
    metrics.push({ name, originalBytes: bytes(baseline), wireBytes: capture.rawBytes, reduction: 1 - capture.rawBytes / bytes(baseline), entries: body.state.referenceTable.length, questionIds: Object.keys(body.questions) });
    if (!input.instructionContext) continue;
    const originals = input.instructionContext.inheritedEvents;
    for (const event of originals) assert.ok(body.state.referenceTable.some(entry => JSON.stringify(entry) === JSON.stringify(ref(event))), 'foreign duplicate local ids remain separate full identities');
    assert.ok(body.state.referenceTable.some(entry => entry.lifecycleId === 'root-earlier-life'), 'same task and local id in another lifecycle stays distinct');
    assert.ok(!body.state.referenceTable.some(entry => ['untrusted-owner', 'extension-owner', ''].includes(entry.taskId)), 'unknown evidence and incomplete refs are not traversed or interned');
    const action = body.state.events.find(event => event.text === input.events[3].text);
    assert.deepEqual(action.data, input.events[3].data);
    assert.deepEqual(action.extension, input.events[3].extension);
    assert.equal(action.text, input.events[3].text);
    assert.deepEqual(body.state.events.find(event => event.sourceRef?.note).sourceRef, originals[1].sourceRef);
    assert.deepEqual(body.state.instruction.extension, input.instructionContext.binding.extension);
    for (const check of result.checks) if (check.evidenceRefs) assert.ok(check.evidenceRefs.every(value => Object.hasOwn(value, 'taskId')), 'result audit references remain original');
  }
  const recovery = records.at(-1);
  assert.ok(Object.hasOwn(recovery.capture.body.questions, 'recover_request_drift'));
  assert.ok(!recovery.result.checks.find(check => check.id === 'request_drift').recoveryFor, 'clear semantic answer cannot override unknown recovery');
  if (process.env.SENTINEL_JEV_RENDER_RECEIPTS) await writeFile(join(process.env.SENTINEL_JEV_RENDER_RECEIPTS, 'metrics.json'), JSON.stringify(metrics, null, 2));
});

test('literal markers at declared source slots or on an event preserve the complete original request', async t => {
  const http = await inference(t), baseline = JSON.parse(await readFile(new URL('./fixtures/jev-rendering/multilevel-stage-two.json', import.meta.url), 'utf8'));
  for (const marker of ['source', 'event']) {
    const { recovery } = multilevel(), expected = structuredClone(baseline);
    if (marker === 'source') {
      recovery.instructionContext.binding.originRefs.push({ $ref: 0 });
      expected.state.instruction.originRefs.push({ $ref: 0 });
    } else {
      recovery.events[3].$eventRef = 123;
      expected.state.events[3].$eventRef = 123;
    }
    const before = structuredClone(recovery), result = await http.analyzer().analyze(recovery);
    const body = http.captures.filter(item => item.path === '/v1/systemone').at(-1).body;
    assert.ok(!Object.hasOwn(body.state, 'referenceTable'), 'colliding source markers keep original identities for this request');
    assert.match(body.state.referenceRenderingInstructions, /Apply sharedQuestionInstructions before each semantic question/);
    assert.deepEqual(decodeJevRequest(body), expected);
    assert.deepEqual(recovery, before);
    assert.deepEqual(result.checks.map(check => check.id), CHECK_IDS);
    assert.deepEqual(Object.keys(body.questions), Object.keys(baseline.questions));
  }
});

test('dictionary rendering preserves the separate relevant-result recovery decision', async t => {
  const http = await inference(t, { recoveryChoice: 'clear' }), { recovery } = multilevel();
  const result = await http.analyzer().analyze(recovery);
  const check = result.checks.find(check => check.id === 'request_drift');
  assert.equal(check.recoveryFor, 'synthetic-old-request-drift');
  assert.ok(check.recoveryEvidenceIds.includes('synthetic-density-grandchild-phase-two-target-result'));
  recovery.events = recovery.events.filter(event => !event.id.includes('phase-two'));
  recovery.task.activeAlerts[0].evidenceIds = recovery.events.filter(event => event.kind === 'tool-result').map(event => event.id);
  const noNewResult = await http.analyzer().analyze(recovery);
  const body = http.captures.filter(item => item.path === '/v1/systemone').at(-1).body;
  assert.ok(!Object.hasOwn(body.questions, 'recover_request_drift'));
  assert.ok(!noNewResult.checks.find(item => item.id === 'request_drift').recoveryFor);
});

test('a rendered JEV batch retains the caller deadline and fails as a JEV deadline', async t => {
  const http = await inference(t, { holdJudge: true });
  await assert.rejects(http.analyzer({ jevTimeoutMs: 25 }).analyze(multilevel().input), error => error.code === 'deadline' && error.stage === 'jev');
  assert.deepEqual(http.captures.map(item => item.path), ['/v1/chat/completions', '/v1/systemone']);
});

test('the unchanged input budget admits the final rendered body and rejects one character less', async t => {
  const http = await inference(t), { recovery } = multilevel();
  await http.analyzer().analyze(recovery);
  const limit = JSON.stringify(http.captures.at(-1).body).length;
  const baseline = JSON.parse(await readFile(new URL('./fixtures/jev-rendering/multilevel-stage-two.json', import.meta.url), 'utf8'));
  assert.ok(JSON.stringify(baseline).length > limit, 'the old repeated body exceeds this valid rendered budget');
  assert.ok(JSON.stringify(http.captures[0].body).length < limit, 'the extractor fits the same unchanged budget');
  let before = http.captures.length;
  const accepted = await http.analyzer({ inputBudgetChars: limit }).analyze(recovery);
  assert.deepEqual(http.captures.slice(before).map(item => item.path), ['/v1/chat/completions', '/v1/systemone']);
  assert.equal(accepted.coverage.complete, true);
  before = http.captures.length;
  const rejected = await http.analyzer({ inputBudgetChars: limit - 1 }).analyze(recovery);
  assert.deepEqual(http.captures.slice(before).map(item => item.path), ['/v1/chat/completions']);
  assert.ok(rejected.coverage.gaps.includes('analysis_input_budget'));
});

test('repeated original IDs in a public recovery batch use the existing full-identity table', async t => {
  const http = await inference(t), { recovery } = multilevel(), before = structuredClone(recovery);
  const result = await http.analyzer().analyze(recovery);
  const capture = http.captures.find(item => item.path === '/v1/systemone'), { body } = capture;
  if (process.env.SENTINEL_JEV_RENDER_RECEIPTS) await writeFile(join(process.env.SENTINEL_JEV_RENDER_RECEIPTS, 'id-alias-recovery-wire.json'), JSON.stringify(body, null, 2));
  const baseline = JSON.parse(await readFile(new URL('./fixtures/jev-rendering/multilevel-stage-two.json', import.meta.url), 'utf8'));
  const originalIds = [baseline.state.requestId, ...baseline.state.facts.map(fact => fact.eventId), ...Object.values(baseline.state.candidates).flatMap(candidate => candidate.evidenceIds), ...baseline.state.previousAlerts.flatMap(alert => alert.evidenceIds)];
  assert.ok(originalIds.length > new Set(originalIds).size, 'all four declared positions repeat original IDs');
  assert.ok(Number.isInteger(body.state.requestId?.$id), 'the actual JEV request must alias a consumed original request ID');
  assert.ok(body.state.facts.every(fact => Number.isInteger(fact.eventId?.$id)));
  assert.ok(Object.values(body.state.candidates).every(candidate => candidate.evidenceIds.every(id => Number.isInteger(id?.$id))));
  assert.ok(body.state.previousAlerts.every(alert => alert.evidenceIds.every(id => Number.isInteger(id?.$id))));
  assert.match(body.state.idReferenceRenderingInstructions, /equals only referenceTable\[n\]\.eventId/);
  assert.match(body.state.idReferenceRenderingInstructions, /no owner\/lifecycle meaning/);
  assert.deepEqual(decodeJevRequest(body), baseline);
  assert.deepEqual(recovery, before);
  assert.deepEqual(result.checks.map(check => check.id), CHECK_IDS);
  for (const check of result.checks) assert.ok(check.evidenceIds.every(id => typeof id === 'string'), 'result and audit IDs stay original strings');
  assert.ok(Object.hasOwn(body.questions, 'recover_request_drift'));
});

function duplicateIdRequest() {
  const own = eventId => ({ taskId: 'own', lifecycleId: 'life', eventId });
  const parent = { taskId: 'parent', lifecycleId: 'parent-life', eventId: 'duplicate', note: 'retain extended provenance' };
  return { model: 'synthetic-judge', state: { requestId: 'same', instruction: { inputRef: own('same'), origin: 'delegated' }, timeBasis: { originRef: own('same') },
    events: [{ id: 'same', taskId: 'own', taskLifecycleId: 'life', kind: 'input', text: 'literal {"$id":0}', data: { $id: 0, evidenceIds: [{ $id: 1 }] } }, { id: 'duplicate', taskId: 'own', taskLifecycleId: 'life', kind: 'output' }, { id: 'duplicate', taskId: 'parent', taskLifecycleId: 'parent-life', kind: 'input', sourceRef: parent }, { id: 'same', taskId: 'foreign', taskLifecycleId: 'foreign-life', kind: 'input' }, { id: 'unbound', kind: 'output', data: { sourceRef: own('unbound') } }],
    facts: [{ kind: 'action', eventId: 'duplicate', sourceRef: own('duplicate') }, { kind: 'context', eventId: 'duplicate', sourceRef: parent }, { kind: 'context', eventId: 'duplicate' }, { kind: 'context', eventId: 'unbound' }, { kind: 'context', eventId: 'missing', sourceRef: own('missing') }],
    candidates: { request_drift: { evidenceIds: ['same', 'duplicate', 'unbound', 'missing'] } }, previousAlerts: [{ id: 'old', evidenceIds: ['same', 'duplicate', 'unbound', 'missing'], sourceRef: own('same') }], extension: { $id: 0, evidenceIds: [{ $id: 1 }] } },
    questions: { request_drift: { type: 'choice', instructions: 'Compare original evidence.', criteria: {} }, recover_request_drift: { type: 'choice', instructions: 'Only a related new result proves recovery.', criteria: {} } } };
}

function originalLogicalRequest(request) {
  const expected = structuredClone(request);
  expected.questions.request_drift.instructions = JEV_PROVENANCE_INSTRUCTIONS + expected.questions.request_drift.instructions;
  return expected;
}

test('ID aliases require visible original identities and retain foreign ownership and extended references', () => {
  const input = duplicateIdRequest(), before = structuredClone(input), wire = renderJevRequest(input), { state } = wire;
  assert.deepEqual(state.referenceTable[state.requestId.$id], { taskId: 'own', lifecycleId: 'life', eventId: 'same' });
  assert.deepEqual(state.referenceTable[state.facts[0].eventId.$id], { taskId: 'own', lifecycleId: 'life', eventId: 'duplicate' });
  assert.deepEqual(state.referenceTable[state.facts[1].eventId.$id], { taskId: 'parent', lifecycleId: 'parent-life', eventId: 'duplicate' });
  assert.deepEqual(state.facts[1].sourceRef, input.state.facts[1].sourceRef);
  assert.deepEqual(state.facts.slice(2).map(fact => fact.eventId), ['duplicate', 'unbound', 'missing'], 'ambiguous, unowned and nonvisible identities stay literal');
  assert.equal(state.candidates.request_drift.evidenceIds[1].$id, state.facts[0].eventId.$id, 'candidate IDs resolve only within the confirmed request owner');
  assert.deepEqual(state.candidates.request_drift.evidenceIds.slice(2), ['unbound', 'missing']);
  assert.deepEqual(state.previousAlerts, input.state.previousAlerts, 'one alert sourceRef cannot grant every evidenceId an owner');
  assert.deepEqual(state.events[0].data, input.state.events[0].data);
  assert.equal(state.events[0].text, input.state.events[0].text);
  assert.deepEqual(state.extension, input.state.extension);
  assert.deepEqual(decodeJevRequest(wire), originalLogicalRequest(input));
  assert.deepEqual(input, before);
  const untranslated = structuredClone(wire);
  delete untranslated.state.idReferenceRenderingInstructions;
  assert.deepEqual(decodeJevRequest(untranslated).state.requestId, state.requestId, 'without ID syntax instructions the decoder does not interpret a literal $id');
  for (const anchor of ['instruction', 'timeBasis']) {
    const unconfirmed = duplicateIdRequest();
    if (anchor === 'timeBasis') delete unconfirmed.state.instruction;
    unconfirmed.state[anchor][anchor === 'instruction' ? 'inputRef' : 'originRef'] = { taskId: 'own', lifecycleId: 'life', eventId: 'nonvisible-input' };
    const rendered = renderJevRequest(unconfirmed);
    assert.equal(rendered.state.requestId, 'same');
    assert.deepEqual(rendered.state.candidates.request_drift.evidenceIds, unconfirmed.state.candidates.request_drift.evidenceIds);
    assert.deepEqual(decodeJevRequest(rendered), originalLogicalRequest(unconfirmed));
  }
});

test('literal $id markers in each declared ID position skip ID aliases while retaining the triples rendering', () => {
  for (const position of ['request', 'fact', 'candidate', 'alert']) {
    const input = duplicateIdRequest();
    if (position === 'request') input.state.requestId = { $id: 0 };
    if (position === 'fact') input.state.facts[0].eventId = { $id: 0 };
    if (position === 'candidate') input.state.candidates.request_drift.evidenceIds[0] = { $id: 0 };
    if (position === 'alert') input.state.previousAlerts[0].evidenceIds[0] = { $id: 0 };
    const before = structuredClone(input), wire = renderJevRequest(input);
    assert.ok(Array.isArray(wire.state.referenceTable));
    assert.ok(Number.isInteger(wire.state.events[0].$eventRef), 'v14 triples are still rendered');
    assert.ok(!Object.hasOwn(wire.state, 'idReferenceRenderingInstructions'));
    assert.deepEqual(wire.state.requestId, input.state.requestId);
    assert.deepEqual(wire.state.facts.map(fact => fact.eventId), input.state.facts.map(fact => fact.eventId));
    assert.deepEqual(wire.state.candidates.request_drift.evidenceIds, input.state.candidates.request_drift.evidenceIds);
    assert.deepEqual(wire.state.previousAlerts, input.state.previousAlerts);
    assert.deepEqual(decodeJevRequest(wire), originalLogicalRequest(input));
    assert.deepEqual(input, before);
  }
});
